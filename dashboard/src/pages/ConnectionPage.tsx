import { useMemo, useState } from "react";
import { api } from "../api/client";
import { Session } from "../api/types";

type Props = {
  session: Session | null;
  onSessionChange: (next: Session | null) => void;
};

function normalizeBaseUrl(raw: string) {
  return raw.replace(/\/$/, "");
}

function toWsUrl(baseUrl: string) {
  if (baseUrl.startsWith("https://")) return `${baseUrl.replace("https://", "wss://")}/ws`;
  if (baseUrl.startsWith("http://")) return `${baseUrl.replace("http://", "ws://")}/ws`;
  return `${baseUrl}/ws`;
}

export function ConnectionPage({ session, onSessionChange }: Props) {
  const [baseUrl, setBaseUrl] = useState(session?.baseUrl ?? import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8080");
  const [username, setUsername] = useState(session?.username ?? "admin");
  const [password, setPassword] = useState("");
  const [status, setStatus] = useState("Idle");

  const currentSession = useMemo(() => {
    if (!session) return "No active session";
    return `${session.username} @ ${session.baseUrl}`;
  }, [session]);

  const testConnection = async () => {
    const normalized = normalizeBaseUrl(baseUrl);
    setStatus("Testing...");
    const resp = await api.testConnection(normalized);
    setStatus(resp.ok ? "Connection OK" : `Connection failed: ${resp.error?.message}`);
  };

  const login = async () => {
    const normalized = normalizeBaseUrl(baseUrl);
    setStatus("Logging in...");
    const resp = await api.login(normalized, username, password);
    if (!resp.ok || !resp.data) {
      setStatus(`Login failed: ${resp.error?.message}`);
      return;
    }

    onSessionChange({
      baseUrl: normalized,
      wsUrl: toWsUrl(normalized),
      username: resp.data.username,
      token: resp.data.token,
      issuedAt: resp.data.issued_at,
      expiresAt: resp.data.expires_at
    });
    setStatus("Login success");
  };

  return (
    <section className="card">
      <h2>Connection</h2>
      <p className="muted">Address, authentication, connection test, and current session.</p>

      <label>
        API Base URL
        <input value={baseUrl} onChange={(e) => setBaseUrl(e.target.value)} placeholder="http://127.0.0.1:8080" />
      </label>
      <div className="row">
        <label>
          Username
          <input value={username} onChange={(e) => setUsername(e.target.value)} />
        </label>
        <label>
          Password
          <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
        </label>
      </div>
      <div className="row">
        <button type="button" onClick={testConnection}>Test Connection</button>
        <button type="button" className="primary" onClick={login}>Connect & Login</button>
        <button type="button" onClick={() => onSessionChange(null)}>Clear Session</button>
      </div>

      <div className="status">Status: {status}</div>
      <div className="status">Current Session: {currentSession}</div>
      {session?.expiresAt ? <div className="status">Token Expiry: {session.expiresAt}</div> : null}
    </section>
  );
}
