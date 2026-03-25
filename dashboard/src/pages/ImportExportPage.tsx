import { useState } from "react";
import { api } from "../api/client";
import { Session } from "../api/types";

type Props = { session: Session | null };
const MAX_IMPORT_SIZE = 200 * 1024 * 1024;

function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

export function ImportExportPage({ session }: Props) {
  const [scope, setScope] = useState<"full" | "segments">("full");
  const [segmentsRaw, setSegmentsRaw] = useState("");
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [status, setStatus] = useState("Idle");
  const [importing, setImporting] = useState(false);

  const validateTarGzFile = (file: File) => {
    const lower = file.name.toLowerCase();
    if (!lower.endsWith(".tar.gz")) {
      return "Only .tar.gz files are allowed.";
    }
    if (file.size > MAX_IMPORT_SIZE) {
      return "File exceeds 200MB limit.";
    }
    return null;
  };

  const exportNow = async () => {
    if (!session) {
      setStatus("Please connect first.");
      return;
    }
    try {
      setStatus("Exporting...");
      const segments = segmentsRaw
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      const { blob, filename } = await api.exportBackup(session, scope, segments);
      downloadBlob(blob, filename);
      setStatus(`Export success: ${filename}`);
    } catch (error) {
      setStatus("Export failed.");
    }
  };

  const importNow = async () => {
    if (!session) {
      setStatus("Please connect first.");
      return;
    }
    if (!uploadFile) {
      setStatus("Please choose a .tar.gz file first.");
      return;
    }
    const validationErr = validateTarGzFile(uploadFile);
    if (validationErr) {
      setStatus(validationErr);
      return;
    }
    const sizeMB = (uploadFile.size / (1024 * 1024)).toFixed(2);
    const confirmed = window.confirm(
      `Import backup?\nScope: ${scope}\nFile: ${uploadFile.name}\nSize: ${sizeMB} MB`
    );
    if (!confirmed) {
      setStatus("Import cancelled.");
      return;
    }
    try {
      setImporting(true);
      setStatus("Importing...");
      const result = await api.importBackup(session, scope, uploadFile);
      setStatus(`Import success: ${JSON.stringify(result)}`);
    } catch {
      setStatus("Import failed.");
    } finally {
      setImporting(false);
    }
  };

  return (
    <section className="card">
      <h2>Import / Export</h2>
      <p className="muted">Upload and export backup packages similar to Docker image upload/export workflows.</p>

      <div className="row wrap">
        <label>
          Scope
          <select value={scope} onChange={(e) => setScope(e.target.value as "full" | "segments")}>
            <option value="full">full</option>
            <option value="segments">segments</option>
          </select>
        </label>
        <label>
          Segments (comma-separated, for segments scope)
          <input
            placeholder="segment_000001.anhe,segment_000002.anhe"
            value={segmentsRaw}
            onChange={(e) => setSegmentsRaw(e.target.value)}
          />
        </label>
      </div>

      <div className="row wrap">
        <button type="button" className="primary" onClick={exportNow}>Export Backup</button>
      </div>

      <label>
        Upload .tar.gz backup
        <input
          type="file"
          accept=".tar.gz,application/gzip"
          onChange={(e) => {
            const file = e.target.files?.[0] ?? null;
            if (!file) {
              setUploadFile(null);
              return;
            }
            const err = validateTarGzFile(file);
            if (err) {
              setUploadFile(null);
              setStatus(err);
              return;
            }
            setUploadFile(file);
            setStatus(`Selected file: ${file.name}`);
          }}
        />
      </label>
      <div className="row wrap">
        <button type="button" onClick={importNow} disabled={importing}>{importing ? "Importing..." : "Import Backup"}</button>
      </div>

      <div className="status">Status: {status}</div>
    </section>
  );
}
