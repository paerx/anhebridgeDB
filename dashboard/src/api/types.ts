export type ApiError = {
  code: string;
  message: string;
  retriable?: boolean;
  details?: unknown;
};

export type RequestEnvelope<T> = {
  requestId: string;
  timeoutMs: number;
  payload: T;
};

export type ResponseEnvelope<T> = {
  requestId: string;
  ok: boolean;
  data?: T;
  error?: ApiError;
};

export type Session = {
  baseUrl: string;
  wsUrl: string;
  username: string;
  token: string;
  issuedAt?: string;
  expiresAt?: string;
};

export type DslResult = Record<string, unknown> | string | number | boolean | null;

export type MetricOhlc = {
  start: string;
  end: string;
  open: number;
  high: number;
  low: number;
  close: number;
  count: number;
};

export type MetricsHistoryResponse = {
  from: string;
  to: string;
  bucket: string;
  metrics: Record<string, MetricOhlc[]>;
};

export type TimelineEntry = {
  event_id: number;
  operation: string;
  event_name?: string;
  updated_at?: string;
  new_value?: unknown;
  old_value?: unknown;
  diff?: unknown;
};
