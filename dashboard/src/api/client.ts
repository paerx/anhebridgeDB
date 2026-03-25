import { RequestEnvelope, ResponseEnvelope, Session, MetricsHistoryResponse } from "./types";

function safeMessage(status: number) {
  switch (status) {
    case 400:
      return "Request is invalid.";
    case 401:
      return "Authentication required or expired.";
    case 403:
      return "Permission denied.";
    case 404:
      return "Requested resource not found.";
    case 408:
      return "Request timeout.";
    case 429:
      return "Too many requests. Please retry later.";
    default:
      if (status >= 500) return "Server is temporarily unavailable.";
      return "Request failed.";
  }
}

function toApiError(status: number, _text: string) {
  return {
    code: `HTTP_${status}`,
    message: safeMessage(status),
    retriable: status >= 500 || status === 429
  };
}

function buildHeaders(token?: string, extra?: HeadersInit): HeadersInit {
  return {
    "Content-Type": "application/json",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...extra
  };
}

function makeEnvelope<T>(payload: T, timeoutMs = 10000): RequestEnvelope<T> {
  return {
    requestId: crypto.randomUUID(),
    timeoutMs,
    payload
  };
}

async function request<TReq, TResp>(params: {
  session?: Session;
  method: "GET" | "POST" | "PUT" | "DELETE";
  path: string;
  payload?: TReq;
  timeoutMs?: number;
}): Promise<ResponseEnvelope<TResp>> {
  const { session, method, path, payload, timeoutMs = 10000 } = params;
  const requestEnv = makeEnvelope(payload as TReq, timeoutMs);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), requestEnv.timeoutMs);

  try {
    const url = `${session?.baseUrl ?? ""}${path}`;
    const resp = await fetch(url, {
      method,
      headers: buildHeaders(session?.token),
      body: method === "GET" ? undefined : JSON.stringify(payload ?? {}),
      signal: controller.signal
    });

    const requestId = requestEnv.requestId;
    if (!resp.ok) {
      return {
        requestId,
        ok: false,
        error: toApiError(resp.status, await resp.text())
      };
    }

    if (resp.status === 204) {
      return { requestId, ok: true, data: undefined as TResp };
    }

    const data = (await resp.json()) as TResp;
    return { requestId, ok: true, data };
  } catch (error) {
    const isTimeout = error instanceof DOMException && error.name === "AbortError";
    return {
      requestId: requestEnv.requestId,
      ok: false,
      error: {
        code: isTimeout ? "TIMEOUT" : "NETWORK_ERROR",
        message: isTimeout ? "Request timeout" : "Network request failed",
        retriable: true
      }
    };
  } finally {
    clearTimeout(timer);
  }
}

export const api = {
  testConnection(baseUrl: string) {
    return request<undefined, { status: string }>({
      method: "GET",
      path: `${baseUrl}/healthz`
    });
  },

  login(baseUrl: string, username: string, password: string) {
    return request<{ username: string; password: string }, { token: string; username: string; issued_at?: string; expires_at?: string }>({
      method: "POST",
      path: `${baseUrl}/auth/login`,
      payload: { username, password }
    });
  },

  executeDsl(session: Session, query: string, timeoutMs = 15000) {
    return request<{ query: string }, { results: unknown[] }>({
      session,
      method: "POST",
      path: "/dsl",
      payload: { query },
      timeoutMs
    });
  },

  getKey(session: Session, key: string) {
    return request<undefined, { key: string; value: unknown; version: number; updated_at: string }>({
      session,
      method: "GET",
      path: `/kv/${encodeURIComponent(key)}`
    });
  },

  putKey(session: Session, key: string, value: unknown) {
    return request<unknown, unknown>({
      session,
      method: "PUT",
      path: `/kv/${encodeURIComponent(key)}`,
      payload: value
    });
  },

  deleteKey(session: Session, key: string) {
    return request<undefined, unknown>({
      session,
      method: "DELETE",
      path: `/kv/${encodeURIComponent(key)}`
    });
  },

  getTimeline(session: Session, key: string, withDiff = true, limit = 20) {
    return request<undefined, { key: string; timeline: unknown[] }>({
      session,
      method: "GET",
      path: `/kv/${encodeURIComponent(key)}/timeline?diff=${withDiff ? "true" : "false"}&limit=${limit}`
    });
  },

  rollbackKey(session: Session, key: string, version: string) {
    return api.executeDsl(session, `ROLLBACK ${key} VERSION:${version}; GET ${key};`);
  },

  getMetrics(session: Session) {
    return request<undefined, Record<string, number>>({
      session,
      method: "GET",
      path: "/metrics"
    });
  },

  getMetricsHistory(session: Session, metrics: string[], from: string, to: string, bucket: string) {
    const q = new URLSearchParams({
      metrics: metrics.join(","),
      from,
      to,
      bucket
    });
    return request<undefined, MetricsHistoryResponse>({
      session,
      method: "GET",
      path: `/metrics/history?${q.toString()}`
    });
  },

  async exportBackup(session: Session, scope: "full" | "segments", segments?: string[]) {
    const q = new URLSearchParams({ scope });
    if (scope === "segments" && segments && segments.length > 0) {
      q.set("segments", segments.join(","));
    }
    const resp = await fetch(`${session.baseUrl}/admin/export?${q.toString()}`, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${session.token}`
      }
    });
    if (!resp.ok) {
      throw new Error(safeMessage(resp.status));
    }
    const blob = await resp.blob();
    const filename = resp.headers.get("Content-Disposition")?.match(/filename="?([^"]+)"?/)?.[1] ?? `anhe-export-${scope}.tar.gz`;
    return { blob, filename };
  },

  async importBackup(session: Session, scope: "full" | "segments", file: File) {
    const q = new URLSearchParams({ scope });
    const resp = await fetch(`${session.baseUrl}/admin/import?${q.toString()}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${session.token}`,
        "Content-Type": "application/gzip"
      },
      body: file
    });
    if (!resp.ok) {
      throw new Error(safeMessage(resp.status));
    }
    return resp.json();
  }
};
