export function formatSpeed(speedBps) {
  const value = Number(speedBps || 0);
  if (!Number.isFinite(value) || value <= 0) return '0 B/s';
  if (value < 1024) return `${value.toFixed(0)} B/s`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB/s`;
  return `${(value / (1024 * 1024)).toFixed(2)} MB/s`;
}

export function formatEta(seconds) {
  const value = Number(seconds || 0);
  if (!Number.isFinite(value) || value <= 0) return '—';
  const total = Math.round(value);
  const mins = Math.floor(total / 60);
  const secs = total % 60;
  if (mins <= 0) return `${secs}s`;
  return `${mins}m ${secs}s`;
}

export function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

export function byId(id) {
  return document.getElementById(id);
}
