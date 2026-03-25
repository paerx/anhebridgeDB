# Dashboard (frontend-only)

This directory contains the separated dashboard frontend (`React + Vite + TypeScript`).

## Run

```bash
cd dashboard
npm install
npm run dev
```

Default frontend URL: `http://localhost:5173`

## Backend binding

The dashboard talks to the existing Go backend through HTTP/WS only (no backend code mixed here):

- REST: `http://127.0.0.1:8080`
- WS: `ws://127.0.0.1:8080/ws`

You can change base URL in the Connection page, then login.

## Implemented pages

- Connection: address, auth, connection test, current session
- Workbench: DSL editor, Table/JSON result view, history/favorites/templates
- Data Explorer: key query, schema-less JSON edit/save, rollback and timeline/audit hints
- Metrics: realtime WS + `/metrics/history` loading, 5m/15m/1h/6h/24h windows, OHLC table fallback

## Notes

- This is a clean starter implementation focused on front/back separation and protocol wiring.
- You can switch to Vue later if needed, while keeping the same API/WS contract model.
