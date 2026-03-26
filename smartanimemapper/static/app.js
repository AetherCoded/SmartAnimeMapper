(function () {
  const modal = () => document.getElementById('file-browser-modal');
  let currentInputId = null;

  async function getJson(url, options = {}) {
    const response = await fetch(url, options);
    if (!response.ok) {
      const text = await response.text();
      throw new Error(text || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async function postJson(url, payload = {}) {
    return getJson(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
  }

  async function refreshStatus() {
    const data = await getJson('/api/status');
    const status = data.status;
    const runtime = data.runtime;
    const setText = (id, value) => {
      const el = document.getElementById(id);
      if (el) el.textContent = value;
    };
    setText('sonarr-backup-date', status.sonarr_backup_display);
    setText('radarr-backup-date', status.radarr_backup_display);
    setText('anidb-fetch-date', status.anidb_fetch_display);
    setText('kometa-fetch-date', status.kometa_fetch_display);
    setText('compile-date', status.compiled_patch_display);
    const fetchButton = document.getElementById('fetch-now-button');
    if (fetchButton) fetchButton.disabled = !status.fetch_allowed_dashboard;

    const setProgress = (prefix, task) => {
      const label = document.getElementById(`${prefix}-progress-label`);
      const bar = document.getElementById(`${prefix}-progress-bar`);
      if (label) label.textContent = task.message || 'idle';
      if (bar) bar.style.width = `${task.progress || 0}%`;
    };
    if (runtime && runtime.tasks) {
      if (runtime.tasks.compile) setProgress('compile', runtime.tasks.compile);
      if (runtime.tasks.patch) setProgress('patch', runtime.tasks.patch);
      const summary = document.getElementById('result-summary');
      if (summary) {
        const result = runtime.results.patch || runtime.results.compile || runtime.results.fetch || runtime.results.backup;
        summary.textContent = result ? JSON.stringify(result, null, 2) : 'No task results yet.';
      }
    }
  }

  async function refreshLogs() {
    const viewer = document.getElementById('log-viewer');
    if (!viewer) return;
    const payload = await getJson('/api/logs');
    viewer.value = payload.text || '';
  }

  async function backupNow() {
    try {
      await postJson('/api/backup');
      await refreshStatus();
      alert('Backup complete.');
    } catch (error) {
      alert(`Backup failed: ${error.message}`);
    }
  }

  async function fetchNow(force) {
    try {
      const payload = await postJson('/api/fetch', { force: !!force });
      if (!payload.ok) {
        alert(payload.message);
      }
    } catch (error) {
      alert(`Fetch failed to start: ${error.message}`);
    }
  }

  async function compileNow() {
    try {
      const payload = await postJson('/api/compile');
      if (!payload.ok) alert(payload.message);
    } catch (error) {
      alert(`Compile failed to start: ${error.message}`);
    }
  }

  async function patchNow() {
    const confirmed = window.confirm('Database containers should be offline for patch. Confirm your sonarr/radarr instances are stopped before proceeding.');
    if (!confirmed) return;
    try {
      const payload = await postJson('/api/patch');
      if (!payload.ok) alert(payload.message);
    } catch (error) {
      alert(`Patch failed to start: ${error.message}`);
    }
  }

  async function clearLogs() {
    if (!window.confirm('Clear the error log?')) return;
    try {
      await postJson('/api/logs/clear');
      await refreshLogs();
    } catch (error) {
      alert(`Could not clear logs: ${error.message}`);
    }
  }

  async function openBrowser(inputId) {
    currentInputId = inputId;
    const input = document.getElementById(inputId);
    const startPath = input && input.value ? input.value : '';
    modal().classList.remove('hidden');
    await loadBrowser(startPath);
  }

  function closeBrowser() {
    modal().classList.add('hidden');
    currentInputId = null;
  }

  async function loadBrowser(path) {
    const list = document.getElementById('browser-list');
    const crumb = document.getElementById('browser-breadcrumb');
    list.innerHTML = '<div class="muted">Loading…</div>';
    const query = path ? `?path=${encodeURIComponent(path)}` : '';
    try {
      const payload = await getJson(`/api/fs/list${query}`);
      list.innerHTML = '';
      if (payload.roots) {
        crumb.textContent = 'Available roots';
        payload.roots.forEach((root) => {
          const btn = document.createElement('button');
          btn.type = 'button';
          btn.className = 'browser-entry';
          btn.innerHTML = `<span>${root.label}</span><small>${root.path}</small>`;
          btn.onclick = () => loadBrowser(root.path);
          list.appendChild(btn);
        });
        return;
      }
      crumb.textContent = payload.current;
      if (payload.parent) {
        const up = document.createElement('button');
        up.type = 'button';
        up.className = 'browser-entry';
        up.innerHTML = '<span>..</span><small>Parent directory</small>';
        up.onclick = () => loadBrowser(payload.parent);
        list.appendChild(up);
      }
      payload.entries.forEach((entry) => {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'browser-entry';
        btn.innerHTML = `<span>${entry.is_dir ? '📁' : '🗄️'} ${entry.name}</span><small>${entry.path}</small>`;
        btn.onclick = () => {
          if (entry.is_dir) {
            loadBrowser(entry.path);
          } else if (currentInputId) {
            const input = document.getElementById(currentInputId);
            if (input) input.value = entry.path;
            closeBrowser();
          }
        };
        list.appendChild(btn);
      });
    } catch (error) {
      list.innerHTML = `<div class="error-callout">${error.message}</div>`;
    }
  }

  function boot() {
    const page = document.body.dataset.page;
    if (page === 'dashboard' || page === 'settings') {
      refreshStatus().catch(() => {});
      setInterval(() => refreshStatus().catch(() => {}), 2500);
    }
    if (page === 'settings') {
      refreshLogs().catch(() => {});
    }
  }

  window.SAM = {
    refreshStatus,
    refreshLogs,
    backupNow,
    fetchNow,
    compileNow,
    patchNow,
    clearLogs,
    openBrowser,
    closeBrowser,
  };

  document.addEventListener('DOMContentLoaded', boot);
})();
