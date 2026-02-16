import { byId, escapeHtml, formatEta, formatSpeed } from './utils.js';

export function updateRunButtons(status) {
  const downloadBtn = byId('download-toggle-btn');
  const uploadBtn = byId('upload-toggle-btn');
  if (!downloadBtn || !uploadBtn) return;

  const running = !!status.running;
  const mode = String(status.mode || 'idle');

  const resetButtonStyle = (button, secondary) => {
    button.classList.remove('danger');
    button.classList.toggle('secondary', !!secondary);
  };

  if (running && mode === 'download') {
    downloadBtn.textContent = 'Прервать скачивание';
    downloadBtn.classList.remove('secondary');
    downloadBtn.classList.add('danger');
    uploadBtn.textContent = 'Начать загрузку';
    resetButtonStyle(uploadBtn, true);
    uploadBtn.disabled = true;
    return;
  }

  if (running && mode === 'upload') {
    uploadBtn.textContent = 'Прервать загрузку';
    uploadBtn.classList.remove('secondary');
    uploadBtn.classList.add('danger');
    downloadBtn.textContent = 'Начать скачивание';
    resetButtonStyle(downloadBtn, false);
    downloadBtn.disabled = true;
    return;
  }

  downloadBtn.textContent = 'Начать скачивание';
  uploadBtn.textContent = 'Начать загрузку';
  resetButtonStyle(downloadBtn, false);
  resetButtonStyle(uploadBtn, true);
  downloadBtn.disabled = false;
  uploadBtn.disabled = false;
}

function setText(id, text) {
  const element = byId(id);
  if (element) element.textContent = text;
}

function setHtml(id, html) {
  const element = byId(id);
  if (element) element.innerHTML = html;
}

function renderFileProgresses(itemsMap) {
  const body = byId('file-progress-body');
  if (!body) return;

  const entries = Object.entries(itemsMap || {});
  if (!entries.length) {
    body.innerHTML = '<tr><td colspan="3">Нет активных загрузок.</td></tr>';
    return;
  }

  entries.sort((a, b) => String(b[0]).localeCompare(String(a[0])));
  const viewEntries = entries.slice(0, 3);
  body.innerHTML = '';

  viewEntries.forEach(([messageId, item]) => {
    const percent = Number(item.percent || 0);
    const received = Number(item.received || 0);
    const total = Number(item.total || 0);
    const state = String(item.state || '');
    const errorText = escapeHtml(item.error || '');
    const speedText = formatSpeed(item.speed_bps || 0);
    const etaText = formatEta(item.eta_sec || 0);

    const row = document.createElement('tr');
    row.innerHTML = `
      <td class="mono">${messageId}</td>
      <td class="progress-cell">
        <progress max="100" value="${percent}"></progress>
        <div class="progress-meta">${percent}% (${received}/${total})</div>
        <div class="progress-meta">${speedText} | ETA: ${etaText}</div>
      </td>
      <td>${state}${errorText ? `<div class="progress-meta">${errorText}</div>` : ''}</td>
    `;
    body.appendChild(row);
  });

  if (entries.length > viewEntries.length) {
    const extra = entries.length - viewEntries.length;
    const row = document.createElement('tr');
    row.innerHTML = `<td colspan="3">Ещё ${extra} файла(ов) в очереди...</td>`;
    body.appendChild(row);
  }
}

function renderUploadProgresses(itemsMap) {
  const body = byId('upload-progress-body');
  if (!body) return;

  const entries = Object.entries(itemsMap || {});
  if (!entries.length) {
    body.innerHTML = '<tr><td colspan="3">Нет активных upload задач.</td></tr>';
    return;
  }

  entries.sort((a, b) => String(b[0]).localeCompare(String(a[0])));
  const viewEntries = entries.slice(0, 3);
  body.innerHTML = '';

  viewEntries.forEach(([uploadId, item]) => {
    const percent = Number(item.percent || 0);
    const received = Number(item.received || 0);
    const total = Number(item.total || 0);
    const state = String(item.state || '');
    const errorText = escapeHtml(item.error || '');
    const speedText = formatSpeed(item.speed_bps || 0);
    const etaText = formatEta(item.eta_sec || 0);

    const row = document.createElement('tr');
    row.innerHTML = `
      <td class="mono">${uploadId}</td>
      <td class="progress-cell">
        <progress max="100" value="${percent}"></progress>
        <div class="progress-meta">${percent}% (${received}/${total})</div>
        <div class="progress-meta">${speedText} | ETA: ${etaText}</div>
      </td>
      <td>${state}${errorText ? `<div class="progress-meta">${errorText}</div>` : ''}</td>
    `;
    body.appendChild(row);
  });

  if (entries.length > viewEntries.length) {
    const extra = entries.length - viewEntries.length;
    const row = document.createElement('tr');
    row.innerHTML = `<td colspan="3">Ещё ${extra} upload задач(и)...</td>`;
    body.appendChild(row);
  }
}

export function applyStatusPayload(status) {
  const runState = byId('run-state');
  if (runState) {
    runState.textContent = status.running ? 'Запущен' : 'Остановлен';
    runState.className = status.running ? 'run' : 'stop';
  }

  setText('status-message', `Сообщение: ${status.message || ''}`);

  const authText = status.auth_authorized ? 'успешно' : 'не выполнена';
  const authClass = status.auth_authorized ? 'run' : 'stop';
  setHtml('status-auth', `Авторизация: <span class="${authClass}">${authText}</span> (${status.auth_message || ''})`);

  let proxyText = 'не используется';
  let proxyClass = 'stop';
  if (status.proxy_enabled) {
    proxyText = status.proxy_available ? 'подключение установлено' : 'ошибка подключения';
    proxyClass = status.proxy_available ? 'run' : 'stop';
  }
  setHtml('status-proxy', `MTProxy: <span class="${proxyClass}">${proxyText}</span>`);

  let sftpText = 'не используется';
  let sftpClass = 'stop';
  if (status.sftp_enabled) {
    sftpText = status.sftp_available ? 'подключение установлено' : 'ошибка подключения';
    sftpClass = status.sftp_available ? 'run' : 'stop';
  }
  setHtml('status-sftp', `SFTP: <span class="${sftpClass}">${sftpText}</span>`);

  let ftpsText = 'не используется';
  let ftpsClass = 'stop';
  if (status.ftps_enabled) {
    ftpsText = status.ftps_available ? 'подключение установлено' : 'ошибка подключения';
    ftpsClass = status.ftps_available ? 'run' : 'stop';
  }
  setHtml('status-ftps', `FTPS: <span class="${ftpsClass}">${ftpsText}</span> (${escapeHtml(status.ftps_message || '')})`);
  setText('status-ftps-message', `FTPS сообщение: ${status.ftps_message || ''}`);

  const ftpsCheckRunning = !!status.ftps_check_running;
  setHtml('status-ftps-check-state', `FTPS проверка: <span class="${ftpsCheckRunning ? 'run' : 'stop'}">${ftpsCheckRunning ? 'выполняется' : 'не запущена'}</span>`);

  setText(
    'status-ftps-check-counters',
    `FTPS check: ${status.ftps_check_checked || 0} / ${status.ftps_check_total || 0} | подтверждено ${status.ftps_check_verified || 0} | missing ${status.ftps_check_missing || 0} | ошибок ${status.ftps_check_failed || 0} | удалено ${status.ftps_check_cleaned || 0}`,
  );
  setText('status-ftps-check-file', `FTPS текущий файл: ${status.ftps_check_current_file || ''}`);
  setText('status-ftps-check-last', `FTPS last: ${status.ftps_check_last_info || ''}`);

  const missingExamples = Array.isArray(status.ftps_check_missing_examples)
    ? status.ftps_check_missing_examples
    : [];
  setText('status-ftps-check-missing', `FTPS missing paths: ${missingExamples.join(' | ')}`);

  setText('status-counters', `Скачано: ${status.downloaded || 0} | Ошибок: ${status.failed || 0} | Пропущено: ${status.skipped || 0}`);
  setText('status-sftp-counters', `Upload: загружено ${status.sftp_uploaded || 0} | пропущено ${status.sftp_skipped || 0} | ошибок ${status.sftp_failed || 0}`);
  setText('status-concurrency', `Параллельность: ${status.current_concurrency || 1} / ${status.configured_concurrency || 1}`);
  setText('status-progress', `Прогресс файла: ${status.progress_percent || 0}% (${status.progress_received || 0} / ${status.progress_total || 0} байт)`);
  setText(
    'status-upload-progress',
    `Upload: ${status.upload_progress_percent || 0}% (${status.upload_progress_received || 0} / ${status.upload_progress_total || 0} байт) | ${formatSpeed(status.upload_progress_speed_bps || 0)} | ETA: ${formatEta(status.upload_progress_eta_sec || 0)}`,
  );
  setText('status-current', `Текущий message_id: ${status.current_message_id || ''}`);
  setText('status-file', `Последний файл: ${status.last_file || ''}`);
  setText('status-updated', `Обновлено: ${status.updated_at || ''}`);

  renderFileProgresses(status.file_progresses || {});
  renderUploadProgresses(status.upload_file_progresses || {});
  updateRunButtons(status);
}
