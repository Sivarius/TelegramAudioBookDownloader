import { byId } from './utils.js';
import { applyStatusPayload, updateRunButtons } from './status.js';

async function runFormAction(action) {
  const form = document.querySelector('form');
  if (!form) return;

  const data = new FormData(form);
  try {
    const response = await fetch(action, {
      method: 'POST',
      body: data,
      cache: 'no-store',
      headers: { 'X-Requested-With': 'fetch' },
    });

    const contentType = String(response.headers.get('content-type') || '').toLowerCase();
    const text = await response.text();

    if (!contentType.includes('text/html')) {
      const target = byId('result-message');
      if (target) target.textContent = text || '';
      return;
    }

    const doc = new DOMParser().parseFromString(text, 'text/html');
    const map = [
      ['#result-message', 'result-message'],
      ['#need-code-hint', 'need-code-hint'],
      ['#need-password-hint', 'need-password-hint'],
      ['#preview-table-body', 'preview-table-body'],
      ['#remote-preview-table-body', 'remote-preview-table-body'],
      ['#remote-preview-meta', 'remote-preview-meta'],
    ];

    map.forEach(([srcSelector, targetId]) => {
      const src = doc.querySelector(srcSelector);
      const target = byId(targetId);
      if (src && target) target.innerHTML = src.innerHTML;
    });

    await refreshDebugLogs();
  } catch (error) {
    const target = byId('result-message');
    if (target) target.textContent = `Результат: ошибка запроса (${error})`;
  }
}

function submitControlAction(action) {
  const form = document.querySelector('form');
  if (!form) return;
  form.setAttribute('action', action);
  runFormAction(action);
}

function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  const button = byId('theme-toggle');
  if (!button) return;
  button.textContent = theme === 'dark' ? 'Светлая тема' : 'Тёмная тема';
}

function activateTab(tabId) {
  document.querySelectorAll('.tab-panel').forEach((element) => element.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach((element) => element.classList.remove('active'));

  const panel = byId(tabId);
  const button = document.querySelector(`.tab-btn[data-tab-target="${tabId}"]`);
  if (panel) panel.classList.add('active');
  if (button) button.classList.add('active');
}

function bindRevealToggle(toggleId, selector) {
  const toggle = byId(toggleId);
  if (!toggle) return;

  const apply = () => {
    document.querySelectorAll(selector).forEach((input) => {
      input.type = toggle.checked ? 'text' : 'password';
    });
  };

  toggle.addEventListener('change', apply);
  apply();
}

function toggleMtproxyField() {
  const useProxy = byId('use_mtproxy')?.checked;
  const group = byId('mtproxy-group');
  if (!group) return;
  group.classList.toggle('hidden', !useProxy);
}

async function loadChannelMenu(refresh = false) {
  const form = document.querySelector('form');
  const status = byId('channel-menu-status');
  const list = byId('channel-menu-list');
  if (!form || !status || !list) return;

  const data = new FormData(form);
  data.set('refresh', refresh ? '1' : '0');
  status.textContent = refresh ? 'Проверка каналов...' : 'Чтение кэша каналов...';
  list.innerHTML = '';

  try {
    const response = await fetch('/channels_status', { method: 'POST', body: data });
    const payload = await response.json();
    status.textContent = payload.message || '';

    if (!payload.items || payload.items.length === 0) {
      list.innerHTML = '<div class="sub">Нет сохранённых каналов.</div>';
      return;
    }

    payload.items.forEach((item) => {
      const row = document.createElement('div');
      row.className = 'channel-item';

      const left = document.createElement('div');
      left.className = 'channel-meta';
      left.innerHTML = `<div><strong>${item.channel_title || item.channel_ref}</strong></div><div class="mono">${item.channel_ref}</div><div class="sub">${item.status || ''}</div><div class="sub">Последняя проверка: ${item.last_checked_at || 'никогда'}</div>`;

      const rightWrap = document.createElement('div');
      rightWrap.className = 'channel-actions';

      const badge = document.createElement('span');
      badge.className = `badge ${item.has_new_audio ? 'ok' : 'warn'}`;
      badge.textContent = item.has_new_audio ? 'Новые есть' : 'Новых нет';

      const selectBtn = document.createElement('button');
      selectBtn.type = 'button';
      selectBtn.className = 'btn secondary xs';
      selectBtn.textContent = 'Выбрать';
      selectBtn.addEventListener('click', () => {
        const channelId = byId('channel_id');
        if (channelId) channelId.value = item.channel_ref || '';
        const cleanupSftp = byId('cleanup_local_after_sftp');
        const cleanupFtps = byId('cleanup_local_after_ftps');
        if (cleanupSftp) cleanupSftp.checked = !!item.cleanup_local;
        if (cleanupFtps) cleanupFtps.checked = !!item.cleanup_local;
        const statusFile = byId('status-file');
        const statusCurrent = byId('status-current');
        if (statusFile) statusFile.textContent = `Последний файл: ${item.last_file_path || ''}`;
        if (statusCurrent) statusCurrent.textContent = `Текущий message_id: ${item.last_message_id || ''}`;
      });

      const prefWrap = document.createElement('div');
      prefWrap.className = 'channel-prefs';
      prefWrap.innerHTML = `
        <label class="check-row"><input type="checkbox" data-pref="check_new">Проверять новые главы</label>
        <label class="check-row"><input type="checkbox" data-pref="auto_download">Включить автозагрузку</label>
        <label class="check-row"><input type="checkbox" data-pref="auto_sftp">Включить автозагрузку на SFTP</label>
        <label class="check-row"><input type="checkbox" data-pref="auto_ftps">Включить автозагрузку на FTPS</label>
        <label class="check-row"><input type="checkbox" data-pref="cleanup_local">Очистка локального после выгрузки</label>
      `;

      prefWrap.querySelector('input[data-pref="check_new"]').checked = !!item.check_new;
      prefWrap.querySelector('input[data-pref="auto_download"]').checked = !!item.auto_download;
      prefWrap.querySelector('input[data-pref="auto_sftp"]').checked = !!item.auto_sftp;
      prefWrap.querySelector('input[data-pref="auto_ftps"]').checked = !!item.auto_ftps;
      prefWrap.querySelector('input[data-pref="cleanup_local"]').checked = !!item.cleanup_local;

      const saveBtn = document.createElement('button');
      saveBtn.type = 'button';
      saveBtn.className = 'btn secondary xs';
      saveBtn.textContent = 'Сохранить';
      saveBtn.addEventListener('click', async () => {
        const saveData = new FormData(document.querySelector('form'));
        saveData.set('channel_ref', item.channel_ref || '');
        saveData.set('channel_id', String(item.channel_id || 0));
        saveData.set('channel_title', item.channel_title || '');
        saveData.set('check_new', prefWrap.querySelector('input[data-pref="check_new"]').checked ? '1' : '0');
        saveData.set('auto_download', prefWrap.querySelector('input[data-pref="auto_download"]').checked ? '1' : '0');
        saveData.set('auto_sftp', prefWrap.querySelector('input[data-pref="auto_sftp"]').checked ? '1' : '0');
        saveData.set('auto_ftps', prefWrap.querySelector('input[data-pref="auto_ftps"]').checked ? '1' : '0');
        saveData.set('cleanup_local', prefWrap.querySelector('input[data-pref="cleanup_local"]').checked ? '1' : '0');

        const response = await fetch('/channels_preferences_update', { method: 'POST', body: saveData });
        const result = await response.json();
        status.textContent = result.message || '';
      });

      const delBtn = document.createElement('button');
      delBtn.type = 'button';
      delBtn.className = 'btn secondary xs';
      delBtn.textContent = 'Удалить';
      delBtn.addEventListener('click', async () => {
        const deleteData = new FormData();
        deleteData.set('channel_ref', item.channel_ref || '');
        const response = await fetch('/channels_delete', { method: 'POST', body: deleteData });
        const result = await response.json();
        status.textContent = result.message || '';
        await loadChannelMenu(false);
      });

      rightWrap.appendChild(badge);
      rightWrap.appendChild(selectBtn);
      rightWrap.appendChild(saveBtn);
      rightWrap.appendChild(delBtn);

      row.appendChild(left);
      row.appendChild(prefWrap);
      row.appendChild(rightWrap);
      list.appendChild(row);
    });
  } catch (error) {
    status.textContent = `Ошибка загрузки списка: ${error}`;
  }
}

async function refreshDebugLogs() {
  const toggle = byId('debug-toggle');
  const box = byId('debug-box');
  const pre = byId('debug-log');
  if (!toggle || !box || !pre) return;

  if (!toggle.checked) {
    box.classList.remove('show');
    return;
  }

  box.classList.add('show');
  try {
    const response = await fetch('/debug_logs', { cache: 'no-store' });
    if (!response.ok) return;

    const data = await response.json();
    pre.textContent = (data.lines || []).join('\n') || 'No logs yet.';
  } catch {
    // ignore debug polling errors
  }
}

function applyDebugColumnsVisibility(enabled) {
  document.body.classList.toggle('debug-mode', !!enabled);
}

async function applyDownloadNewRange(enabled) {
  const fromInput = byId('from_index');
  const toInput = byId('to_index');
  const form = document.querySelector('form');
  const status = byId('channel-menu-status');
  const result = byId('result-message');
  if (!fromInput || !toInput || !form) return;

  if (!enabled) {
    fromInput.disabled = false;
    toInput.disabled = false;
    return;
  }

  const data = new FormData(form);
  fromInput.disabled = false;
  toInput.disabled = false;
  try {
    const response = await fetch('/suggest_new_range', { method: 'POST', body: data });
    const payload = await response.json().catch(() => ({}));
    if (!payload || !payload.ok) {
      if (status && payload?.message) status.textContent = payload.message;
      if (result && payload?.message) result.textContent = `Результат: ${payload.message}`;
      return;
    }

    fromInput.value = payload.from_index || '';
    toInput.value = payload.to_index || '';
    if (status && payload.message) status.textContent = payload.message;
    if (result && payload.message) result.textContent = `Результат: ${payload.message}`;
    fromInput.disabled = true;
    toInput.disabled = true;
  } catch {
    if (result) result.textContent = 'Результат: не удалось получить диапазон новых глав.';
  }
}

function connectStatusStream() {
  if (!window.EventSource) return;

  const eventSource = new EventSource('/status_stream');
  eventSource.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data || '{}');
      applyStatusPayload(payload);
    } catch {
      // ignore malformed payload
    }
  };
}

function bindPrimaryActions() {
  const downloadBtn = byId('download-toggle-btn');
  const uploadBtn = byId('upload-toggle-btn');

  if (downloadBtn) {
    downloadBtn.addEventListener('click', async () => {
      try {
        const response = await fetch('/status', { cache: 'no-store' });
        const state = await response.json();
        const running = !!state.running;
        const mode = String(state.mode || 'idle');

        if (running && mode === 'download') {
          submitControlAction('/stop_download');
          return;
        }
        if (!running) submitControlAction('/start');
      } catch {
        submitControlAction('/start');
      }
    });
  }

  if (uploadBtn) {
    uploadBtn.addEventListener('click', async () => {
      try {
        const response = await fetch('/status', { cache: 'no-store' });
        const state = await response.json();
        const running = !!state.running;
        const mode = String(state.mode || 'idle');

        if (running && mode === 'upload') {
          submitControlAction('/stop_download');
          return;
        }
        if (!running) submitControlAction('/start_upload');
      } catch {
        submitControlAction('/start_upload');
      }
    });
  }
}

function bindFormSubmitRouting() {
  const form = document.querySelector('form');
  if (!form) return;

  let lastClickedFormAction = null;

  const actionButtons = form.querySelectorAll('button[type="submit"][formaction]');
  actionButtons.forEach((button) => {
    button.addEventListener('click', async (event) => {
      event.preventDefault();
      const action = button.getAttribute('formaction') || form.getAttribute('action') || '/authorize';
      lastClickedFormAction = action;
      await runFormAction(action);
    });
  });

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const submitter = event.submitter;
    const action =
      (submitter && submitter.getAttribute('formaction')) ||
      lastClickedFormAction ||
      form.getAttribute('action') ||
      '/authorize';
    lastClickedFormAction = null;
    await runFormAction(action);
  });
}

function init() {
  const themeToggle = byId('theme-toggle');
  if (themeToggle) {
    themeToggle.addEventListener('click', () => {
      const current = localStorage.getItem('theme') || 'dark';
      const next = current === 'dark' ? 'light' : 'dark';
      localStorage.setItem('theme', next);
      applyTheme(next);
    });
  }

  applyTheme(localStorage.getItem('theme') || 'dark');
  updateRunButtons({ running: false, mode: 'idle' });

  document.querySelectorAll('.tab-btn').forEach((button) => {
    button.addEventListener('click', () => activateTab(button.getAttribute('data-tab-target')));
  });
  activateTab('tab-control');

  bindRevealToggle('show-telegram-settings', '.tg-secret');
  bindRevealToggle('show-sftp-settings', '.sftp-secret');
  bindRevealToggle('show-ftps-settings', '.ftps-secret');

  byId('use_mtproxy')?.addEventListener('change', toggleMtproxyField);
  toggleMtproxyField();

  const useSftp = byId('use_sftp');
  const useFtps = byId('use_ftps');
  useSftp?.addEventListener('change', (event) => {
    if (event.target.checked && useFtps) useFtps.checked = false;
  });
  useFtps?.addEventListener('change', (event) => {
    if (event.target.checked && useSftp) useSftp.checked = false;
  });

  byId('channel-menu-toggle')?.addEventListener('click', async () => {
    const box = byId('channel-menu-box');
    if (!box) return;
    box.classList.toggle('hidden');
    if (!box.classList.contains('hidden')) await loadChannelMenu(false);
  });

  byId('channel-menu-refresh')?.addEventListener('click', async () => {
    await loadChannelMenu(true);
  });

  byId('debug-toggle')?.addEventListener('change', async (event) => {
    const enabled = !!event.target.checked;
    applyDebugColumnsVisibility(enabled);
    try {
      const data = new FormData();
      data.set('enabled', enabled ? '1' : '0');
      await fetch('/debug_mode', { method: 'POST', body: data });
    } catch {
      // ignore debug mode errors
    }
    refreshDebugLogs();
  });

  byId('download_new')?.addEventListener('change', (event) => {
    applyDownloadNewRange(!!event.target.checked);
  });

  applyDownloadNewRange(!!byId('download_new')?.checked);

  bindPrimaryActions();
  bindFormSubmitRouting();

  setInterval(() => {
    if (byId('debug-toggle')?.checked) refreshDebugLogs();
  }, 2000);

  applyDebugColumnsVisibility(!!byId('debug-toggle')?.checked);
  connectStatusStream();
  refreshDebugLogs();
}

init();
