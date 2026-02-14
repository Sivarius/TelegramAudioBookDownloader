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
        if (contentType.includes('text/html')) {
          const doc = new DOMParser().parseFromString(text, 'text/html');
          const result = doc.querySelector('#result-message');
          const needCode = doc.querySelector('#need-code-hint');
          const needPassword = doc.querySelector('#need-password-hint');
          const previewBody = doc.querySelector('#preview-table-body');
          if (result) {
            const target = document.getElementById('result-message');
            if (target) target.innerHTML = result.innerHTML;
          }
          if (needCode) {
            const target = document.getElementById('need-code-hint');
            if (target) target.innerHTML = needCode.innerHTML;
          }
          if (needPassword) {
            const target = document.getElementById('need-password-hint');
            if (target) target.innerHTML = needPassword.innerHTML;
          }
          if (previewBody) {
            const target = document.getElementById('preview-table-body');
            if (target) target.innerHTML = previewBody.innerHTML;
          }
          await refreshDebugLogs();
          return;
        }
        const target = document.getElementById('result-message');
        if (target) target.textContent = text || '';
      } catch (e) {
        const target = document.getElementById('result-message');
        if (target) target.textContent = `Результат: ошибка запроса (${e})`;
      }
    }

    function submitControlAction(action) {
      const form = document.querySelector('form');
      if (!form) return;
      form.setAttribute('action', action);
      runFormAction(action);
    }

    function updateRunButtons(s) {
      const downloadBtn = document.getElementById('download-toggle-btn');
      const uploadBtn = document.getElementById('upload-toggle-btn');
      if (!downloadBtn || !uploadBtn) return;

      const running = !!s.running;
      const mode = String(s.mode || 'idle');
      const resetButtonStyle = (btn, isSecondary) => {
        btn.classList.remove('danger');
        btn.classList.toggle('secondary', !!isSecondary);
      };

      if (running && mode === 'download') {
        downloadBtn.textContent = 'Прервать скачивание';
        downloadBtn.classList.remove('secondary');
        downloadBtn.classList.add('danger');
        uploadBtn.textContent = 'Начать загрузку';
        resetButtonStyle(uploadBtn, true);
        uploadBtn.disabled = true;
      } else if (running && mode === 'upload') {
        uploadBtn.textContent = 'Прервать загрузку';
        uploadBtn.classList.remove('secondary');
        uploadBtn.classList.add('danger');
        downloadBtn.textContent = 'Начать скачивание';
        resetButtonStyle(downloadBtn, false);
        downloadBtn.disabled = true;
      } else {
        downloadBtn.textContent = 'Начать скачивание';
        uploadBtn.textContent = 'Начать загрузку';
        resetButtonStyle(downloadBtn, false);
        resetButtonStyle(uploadBtn, true);
        downloadBtn.disabled = false;
        uploadBtn.disabled = false;
      }
    }

    function applyStatusPayload(s) {
      const runState = document.getElementById('run-state');
      runState.textContent = s.running ? 'Запущен' : 'Остановлен';
      runState.className = s.running ? 'run' : 'stop';

      document.getElementById('status-message').textContent = `Сообщение: ${s.message || ''}`;
      const authText = s.auth_authorized ? 'успешно' : 'не выполнена';
      const authClass = s.auth_authorized ? 'run' : 'stop';
      document.getElementById('status-auth').innerHTML = `Авторизация: <span class="${authClass}">${authText}</span> (${s.auth_message || ''})`;
      let proxyText = 'не используется';
      let proxyClass = 'stop';
      if (s.proxy_enabled) {
        proxyText = s.proxy_available ? 'подключение установлено' : 'ошибка подключения';
        proxyClass = s.proxy_available ? 'run' : 'stop';
      }
      document.getElementById('status-proxy').innerHTML = `MTProxy: <span class="${proxyClass}">${proxyText}</span>`;
      let sftpText = 'не используется';
      let sftpClass = 'stop';
      if (s.sftp_enabled) {
        sftpText = s.sftp_available ? 'подключение установлено' : 'ошибка подключения';
        sftpClass = s.sftp_available ? 'run' : 'stop';
      }
      document.getElementById('status-sftp').innerHTML = `SFTP: <span class="${sftpClass}">${sftpText}</span>`;
      let ftpsText = 'не используется';
      let ftpsClass = 'stop';
      if (s.ftps_enabled) {
        ftpsText = s.ftps_available ? 'подключение установлено' : 'ошибка подключения';
        ftpsClass = s.ftps_available ? 'run' : 'stop';
      }
      document.getElementById('status-ftps').innerHTML = `FTPS: <span class="${ftpsClass}">${ftpsText}</span>`;
      const ftpsCheckRunning = !!s.ftps_check_running;
      document.getElementById('status-ftps-check-state').innerHTML = `FTPS проверка: <span class="${ftpsCheckRunning ? 'run' : 'stop'}">${ftpsCheckRunning ? 'выполняется' : 'не запущена'}</span>`;
      document.getElementById('status-ftps-check-counters').textContent = `FTPS check: ${s.ftps_check_checked || 0} / ${s.ftps_check_total || 0} | подтверждено ${s.ftps_check_verified || 0} | missing ${s.ftps_check_missing || 0} | ошибок ${s.ftps_check_failed || 0} | удалено ${s.ftps_check_cleaned || 0}`;
      document.getElementById('status-ftps-check-file').textContent = `FTPS текущий файл: ${s.ftps_check_current_file || ''}`;
      document.getElementById('status-counters').textContent = `Скачано: ${s.downloaded || 0} | Ошибок: ${s.failed || 0} | Пропущено: ${s.skipped || 0}`;
      document.getElementById('status-sftp-counters').textContent = `Upload: загружено ${s.sftp_uploaded || 0} | пропущено ${s.sftp_skipped || 0} | ошибок ${s.sftp_failed || 0}`;
      document.getElementById('status-concurrency').textContent = `Параллельность: ${s.current_concurrency || 1} / ${s.configured_concurrency || 1}`;
      document.getElementById('status-progress').textContent = `Прогресс файла: ${s.progress_percent || 0}% (${s.progress_received || 0} / ${s.progress_total || 0} байт)`;
      document.getElementById('status-upload-progress').textContent = `Upload: ${s.upload_progress_percent || 0}% (${s.upload_progress_received || 0} / ${s.upload_progress_total || 0} байт) | ${formatSpeed(s.upload_progress_speed_bps || 0)} | ETA: ${formatEta(s.upload_progress_eta_sec || 0)}`;
      document.getElementById('status-current').textContent = `Текущий message_id: ${s.current_message_id || ''}`;
      document.getElementById('status-file').textContent = `Последний файл: ${s.last_file || ''}`;
      document.getElementById('status-updated').textContent = `Обновлено: ${s.updated_at || ''}`;
      renderFileProgresses(s.file_progresses || {});
      renderUploadProgresses(s.upload_file_progresses || {});
      updateRunButtons(s);
    }

    function renderFileProgresses(itemsMap) {
      const body = document.getElementById('file-progress-body');
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
        const err = escapeHtml(item.error || '');
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
          <td>${state}${err ? `<div class="progress-meta">${err}</div>` : ''}</td>
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
      const body = document.getElementById('upload-progress-body');
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
        const err = escapeHtml(item.error || '');
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
          <td>${state}${err ? `<div class="progress-meta">${err}</div>` : ''}</td>
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

    function formatSpeed(speedBps) {
      const v = Number(speedBps || 0);
      if (!Number.isFinite(v) || v <= 0) return '0 B/s';
      if (v < 1024) return `${v.toFixed(0)} B/s`;
      if (v < 1024 * 1024) return `${(v / 1024).toFixed(1)} KB/s`;
      return `${(v / (1024 * 1024)).toFixed(2)} MB/s`;
    }

    function formatEta(seconds) {
      const s = Number(seconds || 0);
      if (!Number.isFinite(s) || s <= 0) return '—';
      const total = Math.round(s);
      const m = Math.floor(total / 60);
      const sec = total % 60;
      if (m <= 0) return `${sec}s`;
      return `${m}m ${sec}s`;
    }

    function escapeHtml(value) {
      return String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }

    function connectStatusStream() {
      if (!window.EventSource) {
        return;
      }
      const es = new EventSource('/status_stream');
      es.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data || '{}');
          applyStatusPayload(payload);
        } catch (_) {
        }
      };
      es.onerror = () => {
        // browser auto-reconnects SSE connection
      };
    }

    function applyTheme(theme) {
      document.documentElement.setAttribute('data-theme', theme);
      const button = document.getElementById('theme-toggle');
      button.textContent = theme === 'dark' ? 'Светлая тема' : 'Тёмная тема';
    }

    document.getElementById('theme-toggle').addEventListener('click', () => {
      const current = localStorage.getItem('theme') || 'light';
      const next = current === 'dark' ? 'light' : 'dark';
      localStorage.setItem('theme', next);
      applyTheme(next);
    });

    applyTheme(localStorage.getItem('theme') || 'light');
    updateRunButtons({ running: false, mode: 'idle' });

    function activateTab(tabId) {
      document.querySelectorAll('.tab-panel').forEach((el) => el.classList.remove('active'));
      document.querySelectorAll('.tab-btn').forEach((el) => el.classList.remove('active'));
      const panel = document.getElementById(tabId);
      const btn = document.querySelector(`.tab-btn[data-tab-target="${tabId}"]`);
      if (panel) panel.classList.add('active');
      if (btn) btn.classList.add('active');
    }

    document.querySelectorAll('.tab-btn').forEach((btn) => {
      btn.addEventListener('click', () => activateTab(btn.getAttribute('data-tab-target')));
    });
    activateTab('tab-control');

    function bindRevealToggle(toggleId, selector) {
      const toggle = document.getElementById(toggleId);
      const apply = () => {
        document.querySelectorAll(selector).forEach((input) => {
          input.type = toggle.checked ? 'text' : 'password';
        });
      };
      toggle.addEventListener('change', apply);
      apply();
    }

    bindRevealToggle('show-telegram-settings', '.tg-secret');
    bindRevealToggle('show-sftp-settings', '.sftp-secret');
    bindRevealToggle('show-ftps-settings', '.ftps-secret');

    function toggleMtproxyField() {
      const useProxy = document.getElementById('use_mtproxy').checked;
      const group = document.getElementById('mtproxy-group');
      if (useProxy) {
        group.classList.remove('hidden');
      } else {
        group.classList.add('hidden');
      }
    }

    document.getElementById('use_mtproxy').addEventListener('change', toggleMtproxyField);
    toggleMtproxyField();

    // Allow only one remote upload protocol at a time.
    document.getElementById('use_sftp').addEventListener('change', (e) => {
      if (e.target.checked) {
        document.getElementById('use_ftps').checked = false;
      }
    });
    document.getElementById('use_ftps').addEventListener('change', (e) => {
      if (e.target.checked) {
        document.getElementById('use_sftp').checked = false;
      }
    });
    if (document.getElementById('use_sftp').checked && document.getElementById('use_ftps').checked) {
      document.getElementById('use_ftps').checked = false;
    }

    async function loadChannelMenu(refresh = false) {
      const form = document.querySelector('form');
      const data = new FormData(form);
      data.set('refresh', refresh ? '1' : '0');
      const status = document.getElementById('channel-menu-status');
      const list = document.getElementById('channel-menu-list');
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
          const btn = document.createElement('button');
          btn.type = 'button';
          btn.className = 'btn secondary xs';
          btn.textContent = 'Выбрать';
          btn.addEventListener('click', () => {
            document.getElementById('channel_id').value = item.channel_ref || '';
            document.getElementById('cleanup_local_after_sftp').checked = !!item.cleanup_local;
            document.getElementById('cleanup_local_after_ftps').checked = !!item.cleanup_local;
            document.getElementById('status-file').textContent = `Последний файл: ${item.last_file_path || ''}`;
            document.getElementById('status-current').textContent = `Текущий message_id: ${item.last_message_id || ''}`;
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
            const data = new FormData(document.querySelector('form'));
            data.set('channel_ref', item.channel_ref || '');
            data.set('channel_id', String(item.channel_id || 0));
            data.set('channel_title', item.channel_title || '');
            data.set('check_new', prefWrap.querySelector('input[data-pref="check_new"]').checked ? '1' : '0');
            data.set('auto_download', prefWrap.querySelector('input[data-pref="auto_download"]').checked ? '1' : '0');
            data.set('auto_sftp', prefWrap.querySelector('input[data-pref="auto_sftp"]').checked ? '1' : '0');
            data.set('auto_ftps', prefWrap.querySelector('input[data-pref="auto_ftps"]').checked ? '1' : '0');
            data.set('cleanup_local', prefWrap.querySelector('input[data-pref="cleanup_local"]').checked ? '1' : '0');
            const response = await fetch('/channels_preferences_update', { method: 'POST', body: data });
            const result = await response.json();
            status.textContent = result.message || '';
          });
          const delBtn = document.createElement('button');
          delBtn.type = 'button';
          delBtn.className = 'btn secondary xs';
          delBtn.textContent = 'Удалить';
          delBtn.addEventListener('click', async () => {
            const data = new FormData();
            data.set('channel_ref', item.channel_ref || '');
            const response = await fetch('/channels_delete', { method: 'POST', body: data });
            const result = await response.json();
            status.textContent = result.message || '';
            await loadChannelMenu(false);
          });
          rightWrap.appendChild(badge);
          rightWrap.appendChild(document.createElement('br'));
          rightWrap.appendChild(btn);
          rightWrap.appendChild(document.createElement('br'));
          rightWrap.appendChild(saveBtn);
          rightWrap.appendChild(document.createElement('br'));
          rightWrap.appendChild(delBtn);
          row.appendChild(left);
          row.appendChild(prefWrap);
          row.appendChild(rightWrap);
          list.appendChild(row);
        });
      } catch (e) {
        status.textContent = `Ошибка загрузки списка: ${e}`;
      }
    }

    document.getElementById('channel-menu-toggle').addEventListener('click', async () => {
      const box = document.getElementById('channel-menu-box');
      box.classList.toggle('hidden');
      if (!box.classList.contains('hidden')) {
        await loadChannelMenu(false);
      }
    });
    document.getElementById('channel-menu-refresh').addEventListener('click', async () => {
      await loadChannelMenu(true);
    });

    async function refreshDebugLogs() {
      const toggle = document.getElementById('debug-toggle');
      const box = document.getElementById('debug-box');
      const pre = document.getElementById('debug-log');
      if (!toggle.checked) {
        box.classList.remove('show');
        return;
      }
      box.classList.add('show');
      try {
        const response = await fetch('/debug_logs', { cache: 'no-store' });
        if (!response.ok) {
          return;
        }
        const data = await response.json();
        pre.textContent = (data.lines || []).join('\n') || 'No logs yet.';
      } catch (_) {
      }
    }

    document.getElementById('debug-toggle').addEventListener('change', async (e) => {
      const enabled = !!e.target.checked;
      try {
        const data = new FormData();
        data.set('enabled', enabled ? '1' : '0');
        await fetch('/debug_mode', { method: 'POST', body: data });
      } catch (_) {
      }
      refreshDebugLogs();
    });

    async function applyDownloadNewRange(enabled) {
      const fromInput = document.getElementById('from_index');
      const toInput = document.getElementById('to_index');
      fromInput.disabled = enabled;
      toInput.disabled = enabled;
      if (!enabled) {
        return;
      }
      const data = new FormData(document.querySelector('form'));
      try {
        const response = await fetch('/suggest_new_range', { method: 'POST', body: data });
        const payload = await response.json();
        if (payload && payload.ok) {
          fromInput.value = payload.from_index || '';
          toInput.value = payload.to_index || '';
          const status = document.getElementById('channel-menu-status');
          if (status && payload.message) {
            status.textContent = payload.message;
          }
        }
      } catch (_) {
      }
    }

    document.getElementById('download_new').addEventListener('change', (e) => {
      applyDownloadNewRange(!!e.target.checked);
    });
    applyDownloadNewRange(document.getElementById('download_new').checked);

    document.getElementById('download-toggle-btn').addEventListener('click', async () => {
      try {
        const response = await fetch('/status', { cache: 'no-store' });
        const state = await response.json();
        const running = !!state.running;
        const mode = String(state.mode || 'idle');
        if (running && mode === 'download') {
          submitControlAction('/stop_download');
          return;
        }
        if (!running) {
          submitControlAction('/start');
        }
      } catch (_) {
        submitControlAction('/start');
      }
    });

    document.getElementById('upload-toggle-btn').addEventListener('click', async () => {
      try {
        const response = await fetch('/status', { cache: 'no-store' });
        const state = await response.json();
        const running = !!state.running;
        const mode = String(state.mode || 'idle');
        if (running && mode === 'upload') {
          submitControlAction('/stop_download');
          return;
        }
        if (!running) {
          submitControlAction('/start_upload');
        }
      } catch (_) {
        submitControlAction('/start_upload');
      }
    });

    setInterval(() => {
      if (document.getElementById('debug-toggle').checked) {
        refreshDebugLogs();
      }
    }, 2000);
    const form = document.querySelector('form');
    if (form) {
      form.addEventListener('submit', async (event) => {
        event.preventDefault();
        const submitter = event.submitter;
        const action =
          (submitter && submitter.getAttribute('formaction')) ||
          form.getAttribute('action') ||
          '/authorize';
        await runFormAction(action);
      });
    }
    connectStatusStream();
    refreshDebugLogs();


