import { useMemo, useState } from "react";
import { Session } from "./api/types";
import { loadSession, saveSession } from "./store/session";
import { ConnectionPage } from "./pages/ConnectionPage";
import { WorkbenchPage } from "./pages/WorkbenchPage";
import { DataExplorerPage } from "./pages/DataExplorerPage";
import { MetricsPage } from "./pages/MetricsPage";
import { ImportExportPage } from "./pages/ImportExportPage";

type Page = "connection" | "workbench" | "explorer" | "metrics" | "transfer";

const navItems: { key: Page; label: string; desc: string }[] = [
  { key: "connection", label: "Connection", desc: "Address and auth" },
  { key: "workbench", label: "Workbench", desc: "Run DSL queries" },
  { key: "explorer", label: "Data Explorer", desc: "Edit key JSON" },
  { key: "metrics", label: "Metrics", desc: "Realtime + history" },
  { key: "transfer", label: "Import / Export", desc: "Upload and download backups" }
];

export function App() {
  const [session, setSession] = useState<Session | null>(() => loadSession());
  const [activePage, setActivePage] = useState<Page>("connection");

  const onSessionChange = (next: Session | null) => {
    setSession(next);
    saveSession(next);
    if (next) {
      setActivePage("workbench");
    }
  };

  const authHint = useMemo(() => {
    if (!session) return "Not connected";
    return `Connected as ${session.username}`;
  }, [session]);

  const pageTitle = navItems.find((item) => item.key === activePage)?.label ?? "Dashboard";

  return (
    <div className="docker-shell">
      <header className="docker-topbar">
        <div className="docker-brand">anhe.dashboard</div>
        <div className="docker-search">
          <input placeholder="Search pages, keys, metrics..." />
        </div>
        <div className="docker-actions">
          <span className="session-chip">{authHint}</span>
        </div>
      </header>

      <div className="docker-layout">
        <aside className="docker-sidebar">
          <div className="sidebar-group">
            {navItems.map((item) => (
              <button
                key={item.key}
                className={item.key === activePage ? "side-item active" : "side-item"}
                onClick={() => setActivePage(item.key)}
                type="button"
              >
                <span className="side-title">{item.label}</span>
                <span className="side-desc">{item.desc}</span>
              </button>
            ))}
          </div>
        </aside>

        <main className="docker-main">
          <section className="page-head">
            <h1>{pageTitle}</h1>
            <p>Manage connection, data operations, and metrics in one control plane.</p>
          </section>

          <section className="page-body">
            {activePage === "connection" && <ConnectionPage session={session} onSessionChange={onSessionChange} />}
            {activePage === "workbench" && <WorkbenchPage session={session} />}
            {activePage === "explorer" && <DataExplorerPage session={session} />}
            {activePage === "metrics" && <MetricsPage session={session} />}
            {activePage === "transfer" && <ImportExportPage session={session} />}
          </section>
        </main>
      </div>
    </div>
  );
}
