import { Session } from "./types";

type WsEvent =
  | { type: "status"; connected: boolean; reason?: string }
  | { type: "message"; data: Record<string, unknown> };

export type WsListener = (event: WsEvent) => void;

export class WsChannel {
  private ws: WebSocket | null = null;
  private retry = 0;
  private heartbeatTimer: number | null = null;
  private reconnectTimer: number | null = null;
  private readonly listeners = new Set<WsListener>();
  private readonly topics = new Set<string>();
  private session: Session | null = null;
  private authed = false;
  private authFailed = false;

  connect(session: Session) {
    this.session = session;
    this.open();
  }

  disconnect() {
    this.clearTimers();
    this.retry = 0;
    this.authed = false;
    this.authFailed = false;
    this.ws?.close();
    this.ws = null;
  }

  subscribe(topic: "metrics" | "stats", intervalSeconds = 10) {
    this.topics.add(topic);
    if (this.authed) {
      this.send({ type: "subscribe", topic, interval_seconds: intervalSeconds });
    }
  }

  onEvent(listener: WsListener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private emit(event: WsEvent) {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  private open() {
    if (!this.session) return;
    const socket = new WebSocket(this.session.wsUrl);
    this.ws = socket;
    this.authed = false;

    socket.onopen = () => {
      this.retry = 0;
      this.emit({ type: "status", connected: true });
      this.startHeartbeat();
      this.send({ type: "auth", token: this.session?.token ?? "" });
    };

    socket.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data) as Record<string, unknown>;
        const type = String(data.type ?? "");
        if (type === "authed") {
          this.authed = true;
          this.authFailed = false;
          for (const topic of this.topics) {
            this.send({ type: "subscribe", topic, interval_seconds: 10 });
          }
        }
        if (type === "error" && String(data.code ?? "").toLowerCase() === "unauthorized") {
          this.authFailed = true;
          this.emit({ type: "status", connected: false, reason: "unauthorized" });
          this.ws?.close();
          return;
        }
        this.emit({ type: "message", data });
      } catch {
        this.emit({ type: "status", connected: true, reason: "invalid ws payload" });
      }
    };

    socket.onclose = () => {
      this.emit({ type: "status", connected: false, reason: "socket closed" });
      this.scheduleReconnect();
    };

    socket.onerror = () => {
      this.emit({ type: "status", connected: false, reason: "socket error" });
    };
  }

  private send(payload: Record<string, unknown>) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(payload));
    }
  }

  private startHeartbeat() {
    if (this.heartbeatTimer) {
      window.clearInterval(this.heartbeatTimer);
    }
    this.heartbeatTimer = window.setInterval(() => {
      this.send({ type: "ping" });
    }, 20000);
  }

  private scheduleReconnect() {
    this.clearTimers();
    if (!this.session) return;
    if (this.authFailed) {
      return
    }
    const base = Math.min(30000, 1000 * 2 ** this.retry);
    const jitter = Math.floor(Math.random() * 500);
    this.retry += 1;
    this.reconnectTimer = window.setTimeout(() => this.open(), base + jitter);
  }

  private clearTimers() {
    if (this.heartbeatTimer) {
      window.clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.reconnectTimer) {
      window.clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }
}
