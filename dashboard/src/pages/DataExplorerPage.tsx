import { useState } from "react";
import { api } from "../api/client";
import { Session, TimelineEntry } from "../api/types";

type Props = { session: Session | null };

export function DataExplorerPage({ session }: Props) {
  const [key, setKey] = useState("user:1");
  const [jsonText, setJsonText] = useState('{"name":"tom"}');
  const [recordInfo, setRecordInfo] = useState("No data loaded.");
  const [timeline, setTimeline] = useState<TimelineEntry[]>([]);
  const [status, setStatus] = useState("Idle");

  const getValue = async () => {
    if (!session) return setStatus("Please connect first.");
    const resp = await api.getKey(session, key);
    if (!resp.ok || !resp.data) return setStatus(resp.error?.message ?? "Get failed");

    setJsonText(JSON.stringify(resp.data.value, null, 2));
    setRecordInfo(`version=${resp.data.version}, updated_at=${resp.data.updated_at}`);
    setStatus("Loaded key value");
  };

  const saveValue = async () => {
    if (!session) return setStatus("Please connect first.");
    try {
      const value = JSON.parse(jsonText) as unknown;
      const resp = await api.putKey(session, key, value);
      if (!resp.ok) return setStatus(resp.error?.message ?? "Save failed");
      setStatus("Saved via REST PUT /kv/{key}");
      await getTimeline();
    } catch {
      setStatus("Invalid JSON");
    }
  };

  const deleteValue = async () => {
    if (!session) return setStatus("Please connect first.");
    const resp = await api.deleteKey(session, key);
    setStatus(resp.ok ? "Deleted key" : resp.error?.message ?? "Delete failed");
  };

  const getTimeline = async () => {
    if (!session) return setStatus("Please connect first.");
    const resp = await api.getTimeline(session, key, true, 20);
    if (!resp.ok || !resp.data) return setStatus(resp.error?.message ?? "Timeline failed");
    setTimeline(resp.data.timeline as TimelineEntry[]);
    setStatus("Timeline loaded");
  };

  const rollback = async (version: string) => {
    if (!session) return setStatus("Please connect first.");
    const ok = window.confirm(`Rollback ${key} to VERSION:${version}?`);
    if (!ok) return;
    const resp = await api.rollbackKey(session, key, version);
    if (!resp.ok) return setStatus(resp.error?.message ?? "Rollback failed");
    setStatus(`Rollback done · requestId=${resp.requestId}`);
    await getValue();
    await getTimeline();
  };

  const formatValue = (value: unknown) => {
    if (value === undefined || value === null) return "-";
    if (typeof value === "string") return value;
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  };

  return (
    <section className="card">
      <h2>Data Explorer</h2>
      <p className="muted">Direct key query, schema-less JSON editing, save, and rollback with audit hints.</p>

      <label>
        Key
        <input value={key} onChange={(e) => setKey(e.target.value)} placeholder="user:1" />
      </label>

      <div className="row">
        <button type="button" onClick={getValue}>Load</button>
        <button type="button" className="primary" onClick={saveValue}>Save JSON</button>
        <button type="button" onClick={deleteValue}>Delete</button>
        <button type="button" onClick={getTimeline}>Timeline</button>
      </div>

      <textarea rows={10} value={jsonText} onChange={(e) => setJsonText(e.target.value)} />
      <div className="status">Record: {recordInfo}</div>
      <div className="status">Status: {status}</div>

      <h3>Timeline / Rollback</h3>
      <table>
        <thead>
          <tr>
            <th>event_id</th>
            <th>operation</th>
            <th>event_name</th>
            <th>old_value</th>
            <th>updated_at</th>
            <th>action</th>
          </tr>
        </thead>
        <tbody>
          {timeline.map((item) => (
            <tr key={`${item.event_id}-${item.operation}`}>
              <td>{item.event_id}</td>
              <td>{item.operation}</td>
              <td>{item.event_name ?? "-"}</td>
              <td title={formatValue(item.old_value)}>{formatValue(item.old_value)}</td>
              <td>{item.updated_at ?? "-"}</td>
              <td><button type="button" onClick={() => rollback(String(item.event_id))}>Rollback</button></td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}
