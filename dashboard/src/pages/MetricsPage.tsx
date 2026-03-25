import { useEffect, useMemo, useRef, useState } from "react";
import { Line, LineChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api } from "../api/client";
import { Session, MetricOhlc } from "../api/types";
import { WsChannel } from "../api/ws";

type Props = { session: Session | null };

type TimeWindow = "5m" | "15m" | "1h" | "6h" | "24h";
type MetricPreset = "system" | "throughput" | "latency" | "runtime" | "custom";

type ChartPoint = {
  ts: string;
  label: string;
  [key: string]: string | number;
};

const windowMinutes: Record<TimeWindow, number> = {
  "5m": 5,
  "15m": 15,
  "1h": 60,
  "6h": 360,
  "24h": 1440
};

const wildcardGroups: Record<string, string[]> = {
  "*cpu": ["cpu_percent", "cpu_cores_used"],
  "*mem": ["memory_alloc_mb", "memory_inuse_mb", "memory_sys_mb", "memory_heap_usage_percent"],
  "*io": ["io_read_ops_per_sec", "io_write_ops_per_sec", "io_write_mb_per_sec"],
  "*lat": ["get_avg_ms", "set_avg_ms", "append_avg_ms", "timeline_avg_ms"]
};

const metricCatalog = [
  ...wildcardGroups["*cpu"],
  ...wildcardGroups["*mem"],
  ...wildcardGroups["*io"],
  ...wildcardGroups["*lat"],
  "heap_alloc_bytes",
  "gc_pause_total_ms",
  "goroutines",
  "pending_tasks",
  "append_count"
];

function expandMetricTokens(tokens: string[]) {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const raw of tokens) {
    const token = raw.trim();
    if (!token) continue;
    const normalized = token.toLowerCase();

    if (wildcardGroups[normalized]) {
      for (const metric of wildcardGroups[normalized]) {
        if (!seen.has(metric)) {
          seen.add(metric);
          out.push(metric);
        }
      }
      continue;
    }

    if (normalized.startsWith("*")) {
      const prefix = normalized.slice(1);
      for (const metric of metricCatalog) {
        if (metric.toLowerCase().startsWith(prefix) && !seen.has(metric)) {
          seen.add(metric);
          out.push(metric);
        }
      }
      continue;
    }

    if (!seen.has(token)) {
      seen.add(token);
      out.push(token);
    }
  }
  return out;
}

const metricPresets: Record<Exclude<MetricPreset, "custom">, { label: string; metrics: string[] }> = {
  system: {
    label: "System",
    metrics: ["*cpu", "*mem", "heap_alloc_bytes", "gc_pause_total_ms"]
  },
  throughput: {
    label: "Throughput",
    metrics: ["*io", "append_count"]
  },
  latency: {
    label: "Latency",
    metrics: ["*lat"]
  },
  runtime: {
    label: "Runtime",
    metrics: ["goroutines", "pending_tasks", "scheduler_run_avg_ms", "key_lock_wait_avg_ms"]
  }
};

const defaultMetricsInput = metricPresets.system.metrics.join(",");
const colors = ["#7eb6ff", "#f0c36b", "#73d39f", "#d98cff", "#ff8a8a", "#80e2ff"];

function computeRange(windowKey: TimeWindow) {
  const to = new Date();
  const from = new Date(to.getTime() - windowMinutes[windowKey] * 60 * 1000);
  return { from: from.toISOString(), to: to.toISOString() };
}

function formatLabel(ts: string) {
  const d = new Date(ts);
  return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}:${String(d.getSeconds()).padStart(2, "0")}`;
}

export function MetricsPage({ session }: Props) {
  const [metricsInput, setMetricsInput] = useState(defaultMetricsInput);
  const [activePreset, setActivePreset] = useState<MetricPreset>("system");
  const [windowKey, setWindowKey] = useState<TimeWindow>("15m");
  const [bucket, setBucket] = useState("10s");
  const [status, setStatus] = useState("Idle");
  const [wsConnected, setWsConnected] = useState(false);
  const [historyData, setHistoryData] = useState<Record<string, MetricOhlc[]>>({});
  const [liveData, setLiveData] = useState<Record<string, number>>({});
  const wsRef = useRef<WsChannel | null>(null);
  const selectedMetrics = useMemo(() => expandMetricTokens(metricsInput.split(",")), [metricsInput]);

  const refreshHistory = async (metricsToLoad: string[] = selectedMetrics) => {
    if (!session) return setStatus("Please connect first.");
    const { from, to } = computeRange(windowKey);
    const resp = await api.getMetricsHistory(session, metricsToLoad, from, to, bucket);
    if (!resp.ok || !resp.data) return setStatus(resp.error?.message ?? "History failed");
    setHistoryData(resp.data.metrics);
    setStatus(`History loaded: ${from} -> ${to}`);
  };

  const onPresetClick = async (preset: Exclude<MetricPreset, "custom">) => {
    const metrics = metricPresets[preset].metrics.join(",");
    setMetricsInput(metrics);
    setActivePreset(preset);
    await refreshHistory(expandMetricTokens(metrics.split(",")));
  };

  const onMetricsInputChange = (value: string) => {
    setMetricsInput(value);
    setActivePreset("custom");
  };

  useEffect(() => {
    if (!session) return;
    if (!wsRef.current) wsRef.current = new WsChannel();
    const ws = wsRef.current;

    const unsub = ws.onEvent((event) => {
      if (event.type === "status") {
        setWsConnected(event.connected);
        if (!event.connected) setStatus(`WS reconnecting: ${event.reason ?? "unknown"}`);
        return;
      }

      const type = String(event.data.type ?? "");
      if (type === "metrics") {
        const data = event.data.data as Record<string, number>;
        setLiveData(data ?? {});
      }
      if (type === "error") {
        setStatus(`WS error: ${String(event.data.error ?? "unknown")}`);
      }
    });

    ws.connect(session);
    ws.subscribe("metrics", 10);
    return () => {
      unsub();
      ws.disconnect();
    };
  }, [session]);

  const chartData = useMemo(() => {
    const pointMap = new Map<string, ChartPoint>();

    for (const metric of selectedMetrics) {
      const series = historyData[metric] ?? [];
      for (const item of series) {
        const ts = item.start;
        const current = pointMap.get(ts) ?? { ts, label: formatLabel(ts) };
        current[metric] = item.close;
        pointMap.set(ts, current);
      }
    }

    if (Object.keys(liveData).length > 0) {
      const nowTs = new Date().toISOString();
      const current = pointMap.get(nowTs) ?? { ts: nowTs, label: formatLabel(nowTs) };
      for (const metric of selectedMetrics) {
        if (typeof liveData[metric] === "number") {
          current[metric] = liveData[metric];
        }
      }
      pointMap.set(nowTs, current);
    }

    return Array.from(pointMap.values()).sort((a, b) => a.ts.localeCompare(b.ts));
  }, [historyData, liveData, selectedMetrics]);

  const historyRows = useMemo(() => {
    return Object.entries(historyData)
      .flatMap(([metric, items]) =>
        items.map((it) => ({
          metric,
          start: it.start,
          open: it.open,
          high: it.high,
          low: it.low,
          close: it.close,
          count: it.count
        }))
      )
      .slice(-120);
  }, [historyData]);

  return (
    <section className="card">
      <h2>Metrics</h2>
      <p className="muted">Grafana-style trend chart with live websocket updates + /metrics/history baseline.</p>

      <div className="row wrap">
        <div className="metrics-presets" role="group" aria-label="Quick metric presets">
          {Object.entries(metricPresets).map(([key, preset]) => (
            <button
              key={key}
              type="button"
              className={`preset-btn ${activePreset === key ? "active" : ""}`}
              onClick={() => void onPresetClick(key as Exclude<MetricPreset, "custom">)}
            >
              {preset.label}
            </button>
          ))}
          <span className={`preset-custom ${activePreset === "custom" ? "active" : ""}`}>Custom</span>
        </div>
        <label>
          Metrics (comma-separated, supports `*io,*cpu,*mem,*lat`)
          <input
            value={metricsInput}
            onChange={(e) => onMetricsInputChange(e.target.value)}
          />
        </label>
        <label>
          Window
          <select value={windowKey} onChange={(e) => setWindowKey(e.target.value as TimeWindow)}>
            <option value="5m">5m</option>
            <option value="15m">15m</option>
            <option value="1h">1h</option>
            <option value="6h">6h</option>
            <option value="24h">24h</option>
          </select>
        </label>
        <label>
          Bucket
          <input value={bucket} onChange={(e) => setBucket(e.target.value)} />
        </label>
      </div>

      <div className="row">
        <button type="button" className="primary" onClick={() => void refreshHistory()}>
          Load History
        </button>
      </div>

      <div className="status">WS: {wsConnected ? "connected" : "disconnected"}</div>
      <div className="status">Status: {status}</div>

      <div className="metrics-mini-grid">
        {selectedMetrics.map((metric, idx) => {
          const series = chartData
            .filter((point) => typeof point[metric] === "number")
            .map((point) => ({ label: point.label, value: Number(point[metric]) }));
          const latest = series.length > 0 ? series[series.length - 1].value : undefined;
          return (
            <div className="metric-mini-card" key={metric}>
              <div className="metric-mini-head">
                <span className="metric-mini-name">{metric}</span>
                <span className="metric-mini-value">{latest === undefined ? "-" : latest.toFixed(3)}</span>
              </div>
              <ResponsiveContainer width="100%" height={140}>
                <LineChart data={series}>
                  <CartesianGrid strokeDasharray="2 2" stroke="#e0e7f2" />
                  <XAxis dataKey="label" hide />
                  <YAxis hide domain={["auto", "auto"]} />
                  <Tooltip />
                  <Line
                    type="monotone"
                    dataKey="value"
                    stroke={colors[idx % colors.length]}
                    strokeWidth={2}
                    dot={false}
                    connectNulls
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          );
        })}
      </div>

      <h3>OHLC History</h3>
      <table>
        <thead>
          <tr>
            <th>metric</th>
            <th>start</th>
            <th>open</th>
            <th>high</th>
            <th>low</th>
            <th>close</th>
            <th>count</th>
          </tr>
        </thead>
        <tbody>
          {historyRows.map((row, idx) => (
            <tr key={`${row.metric}-${row.start}-${idx}`}>
              <td>{row.metric}</td>
              <td>{row.start}</td>
              <td>{row.open}</td>
              <td>{row.high}</td>
              <td>{row.low}</td>
              <td>{row.close}</td>
              <td>{row.count}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}
