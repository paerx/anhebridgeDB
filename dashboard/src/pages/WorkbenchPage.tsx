import { useMemo, useState } from "react";
import { api } from "../api/client";
import { Session } from "../api/types";

type Props = { session: Session | null };

type ViewMode = "table" | "json";

const templates = [
  "GET user:1;",
  "AGET user:1 user:2;",
  "SEARCH EVENTS KEY:user:1 LIMIT:20 PAGE:1;",
  "ROLLBACK user:1 VERSION:LAST;"
];

export function WorkbenchPage({ session }: Props) {
  const [query, setQuery] = useState("GET user:1;");
  const [results, setResults] = useState<unknown[]>([]);
  const [status, setStatus] = useState("Idle");
  const [mode, setMode] = useState<ViewMode>("table");
  const [history, setHistory] = useState<string[]>([]);
  const [favorites, setFavorites] = useState<string[]>([]);

  const canRun = Boolean(session);

  const run = async () => {
    if (!session) {
      setStatus("Please connect first.");
      return;
    }
    setStatus("Executing...");
    const resp = await api.executeDsl(session, query);
    if (!resp.ok || !resp.data) {
      setStatus(`${resp.error?.code}: ${resp.error?.message}`);
      return;
    }
    setResults(resp.data.results);
    setHistory((prev) => [query, ...prev].slice(0, 30));
    setStatus(`Done · requestId=${resp.requestId}`);
  };

  const tableRows = useMemo(() => {
    return results.map((item, index) => ({
      index: index + 1,
      value: typeof item === "object" ? JSON.stringify(item) : String(item)
    }));
  }, [results]);

  return (
    <section className="card">
      <h2>Workbench</h2>
      <p className="muted">Database-tool style DSL execution with history, favorites, and templates.</p>

      <div className="row wrap">
        {templates.map((tpl) => (
          <button type="button" key={tpl} onClick={() => setQuery(tpl)}>{tpl}</button>
        ))}
      </div>

      <textarea rows={8} value={query} onChange={(e) => setQuery(e.target.value)} />

      <div className="row">
        <button type="button" className="primary" disabled={!canRun} onClick={run}>Run DSL</button>
        <button type="button" onClick={() => setMode("table")}>Table View</button>
        <button type="button" onClick={() => setMode("json")}>JSON View</button>
        <button type="button" onClick={() => setFavorites((prev) => [query, ...prev].slice(0, 20))}>Favorite</button>
      </div>

      <div className="status">{status}</div>

      {mode === "table" ? (
        <table>
          <thead>
            <tr><th>#</th><th>Result</th></tr>
          </thead>
          <tbody>
            {tableRows.map((row) => (
              <tr key={row.index}><td>{row.index}</td><td>{row.value}</td></tr>
            ))}
          </tbody>
        </table>
      ) : (
        <pre>{JSON.stringify(results, null, 2)}</pre>
      )}

      <div className="row split">
        <div>
          <h3>History</h3>
          <ul>{history.map((h, idx) => <li key={`${h}-${idx}`}>{h}</li>)}</ul>
        </div>
        <div>
          <h3>Favorites</h3>
          <ul>{favorites.map((h, idx) => <li key={`${h}-${idx}`}>{h}</li>)}</ul>
        </div>
      </div>
    </section>
  );
}
