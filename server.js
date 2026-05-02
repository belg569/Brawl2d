/**
 * BRAWL 2D — WebSocket Game Server
 * 
 * Запуск:
 *   npm install ws
 *   node server.js
 * 
 * Или с автоперезапуском:
 *   npm install ws nodemon
 *   npx nodemon server.js
 * 
 * Для деплоя на Railway / Render / VPS:
 *   Установить переменную PORT (по умолчанию 3000)
 *   Команда запуска: node server.js
 */

const { WebSocketServer, WebSocket } = require('ws');
const { randomUUID } = require('crypto');

const PORT = process.env.PORT || 3000;
const wss  = new WebSocketServer({ port: PORT });

// ── STATE ─────────────────────────────────────────────────────────────────────
const queue   = new Map();   // socketId → ws   (игроки в поиске)
const rooms   = new Map();   // roomId   → Room
const clients = new Map();   // socketId → { ws, roomId }

class Room {
  constructor(id, p1ws, p1id, p2ws, p2id) {
    this.id   = id;
    this.tick = 0;
    this.started = false;

    this.players = {
      [p1id]: { ws: p1ws, id: p1id, num: 1, x: 0.2, y: 0.5, hp: 100, aimAngle: 0,    dead: false },
      [p2id]: { ws: p2ws, id: p2id, num: 2, x: 0.8, y: 0.5, hp: 100, aimAngle: Math.PI, dead: false },
    };
    this.bullets = [];   // { id, x, y, vx, vy, owner }
    this.tickInterval = null;
  }

  // Отправить всем игрокам в комнате
  broadcast(msg) {
    const raw = JSON.stringify(msg);
    for (const p of Object.values(this.players)) {
      if (p.ws.readyState === WebSocket.OPEN) p.ws.send(raw);
    }
  }

  // Отправить конкретному игроку
  send(playerId, msg) {
    const p = this.players[playerId];
    if (p && p.ws.readyState === WebSocket.OPEN) p.ws.send(JSON.stringify(msg));
  }

  start() {
    this.started = true;
    // Сообщить обоим их номер и стартовое состояние
    for (const p of Object.values(this.players)) {
      p.ws.send(JSON.stringify({
        type: 'game_start',
        yourNum:  p.num,
        roomId:   this.id,
        state:    this.getState(),
      }));
    }

    // Серверный тик 20 раз в секунду (50мс)
    this.tickInterval = setInterval(() => this.serverTick(), 50);
  }

  serverTick() {
    const dt = 0.05;
    const SPEED  = 220;
    const BSPEED = 520;
    const BCDMG  = 18;
    const W = 1, H = 1; // нормализованные координаты (0..1)

    // Пули
    for (let i = this.bullets.length - 1; i >= 0; i--) {
      const b = this.bullets[i];
      b.x += b.vx * dt;
      b.y += b.vy * dt;

      if (b.x < 0 || b.x > 1 || b.y < 0 || b.y > 1) {
        this.bullets.splice(i, 1); continue;
      }

      // Попадание по противнику
      for (const p of Object.values(this.players)) {
        if (p.num === b.owner || p.dead) continue;
        const dx = b.x - p.x, dy = b.y - p.y;
        // playerRadius ≈ 22px из 500px арены ≈ 0.044 нормализованных единиц
        if (Math.hypot(dx, dy) < 0.055) {
          p.hp = Math.max(0, p.hp - BCDMG);
          this.bullets.splice(i, 1);
          if (p.hp <= 0) {
            p.dead = true;
            this.endGame();
            return;
          }
          break;
        }
      }
    }

    this.tick++;
    // Каждые 4 тика (200мс) рассылаем состояние
    if (this.tick % 4 === 0) {
      this.broadcast({ type: 'state', state: this.getState() });
    }
  }

  getState() {
    const ps = {};
    for (const [id, p] of Object.entries(this.players)) {
      ps[id] = { num: p.num, x: p.x, y: p.y, hp: p.hp, aimAngle: p.aimAngle, dead: p.dead };
    }
    return { players: ps, bullets: this.bullets, tick: this.tick };
  }

  handleInput(playerId, data) {
    const p = this.players[playerId];
    if (!p || p.dead) return;

    const dt = 0.05;
    const SPEED = 220;

    // Позиция: клиент шлёт свою нормализованную позицию (авторитет у клиента для движения)
    if (data.x !== undefined) p.x = Math.max(0, Math.min(1, data.x));
    if (data.y !== undefined) p.y = Math.max(0, Math.min(1, data.y));
    if (data.aimAngle !== undefined) p.aimAngle = data.aimAngle;

    // Выстрел
    if (data.shoot) {
      const ax = Math.cos(p.aimAngle), ay = Math.sin(p.aimAngle);
      const BSPEED_NORM = 520 / 500; // скорость в нормализованных единицах/сек
      this.bullets.push({
        id:    randomUUID().slice(0, 8),
        x:     p.x + ax * 0.056,
        y:     p.y + ay * 0.056,
        vx:    ax * BSPEED_NORM,
        vy:    ay * BSPEED_NORM,
        owner: p.num,
      });
      // Немедленно broadcast выстрела
      this.broadcast({ type: 'bullet_fired', bullet: this.bullets[this.bullets.length - 1] });
    }
  }

  endGame() {
    clearInterval(this.tickInterval);
    const winner = Object.values(this.players).find(p => !p.dead);
    this.broadcast({
      type:     'game_over',
      winnerNum: winner ? winner.num : 0,
    });
  }

  destroy() {
    clearInterval(this.tickInterval);
  }
}

// ── CONNECTION HANDLER ────────────────────────────────────────────────────────
wss.on('connection', (ws) => {
  const socketId = randomUUID();
  clients.set(socketId, { ws, roomId: null });

  ws.send(JSON.stringify({ type: 'connected', socketId }));
  console.log(`[+] Connected: ${socketId} | Total: ${wss.clients.size}`);

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    const client = clients.get(socketId);

    switch (msg.type) {

      // Игрок встаёт в очередь
      case 'find_match': {
        if (queue.has(socketId)) break;  // уже в очереди

        // Есть ли кто-то в очереди?
        if (queue.size > 0) {
          // Берём первого из очереди
          const [opponentId, opponentWs] = queue.entries().next().value;
          queue.delete(opponentId);

          const roomId = randomUUID().slice(0, 8);
          const room   = new Room(roomId, ws, socketId, opponentWs, opponentId);
          rooms.set(roomId, room);

          clients.get(socketId).roomId    = roomId;
          clients.get(opponentId).roomId  = roomId;

          console.log(`[room] Created ${roomId}: ${socketId} vs ${opponentId}`);
          room.start();
        } else {
          // Встаём в очередь
          queue.set(socketId, ws);
          ws.send(JSON.stringify({ type: 'queued', position: queue.size }));
          console.log(`[queue] ${socketId} waiting | Queue size: ${queue.size}`);
        }
        break;
      }

      // Игрок отменяет поиск
      case 'cancel_match': {
        queue.delete(socketId);
        ws.send(JSON.stringify({ type: 'queue_cancelled' }));
        break;
      }

      // Игровой ввод (позиция + выстрел)
      case 'input': {
        const roomId = client?.roomId;
        if (!roomId) break;
        const room = rooms.get(roomId);
        if (!room) break;
        room.handleInput(socketId, msg);
        break;
      }

      // Пинг / поддержание соединения
      case 'ping': {
        ws.send(JSON.stringify({ type: 'pong', ts: msg.ts }));
        break;
      }
    }
  });

  ws.on('close', () => {
    console.log(`[-] Disconnected: ${socketId}`);
    queue.delete(socketId);

    const client = clients.get(socketId);
    if (client?.roomId) {
      const room = rooms.get(client.roomId);
      if (room) {
        // Сообщить противнику о дисконнекте
        room.broadcast({ type: 'opponent_left' });
        room.destroy();
        rooms.delete(client.roomId);
      }
    }
    clients.delete(socketId);
  });

  ws.on('error', (err) => {
    console.error(`[!] Error ${socketId}:`, err.message);
  });
});

console.log(`🎮 BRAWL 2D Server running on ws://localhost:${PORT}`);
console.log(`   Деплой: Railway / Render / любой VPS с Node.js`);
