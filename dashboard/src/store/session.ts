import { Session } from "../api/types";

const SESSION_KEY = "anhe_dashboard_session";

export function loadSession(): Session | null {
  const raw = sessionStorage.getItem(SESSION_KEY);
  if (!raw) return null;
  try {
    const session = JSON.parse(raw) as Session;
    if (session.expiresAt) {
      const expiresAtMs = Date.parse(session.expiresAt);
      if (!Number.isNaN(expiresAtMs) && expiresAtMs <= Date.now()) {
        sessionStorage.removeItem(SESSION_KEY);
        return null;
      }
    }
    return session;
  } catch {
    return null;
  }
}

export function saveSession(session: Session | null) {
  if (!session) {
    sessionStorage.removeItem(SESSION_KEY);
    return;
  }
  sessionStorage.setItem(SESSION_KEY, JSON.stringify(session));
}
