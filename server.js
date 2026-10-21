const path = require('path');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 5e6, cors: { origin: true } });
const PORT = process.env.PORT || 3000;
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '3693';
const TOPIC_STATE = 'samsung_plan_ws_2026_minji_v5/state';
const TOPIC_ACTION = 'samsung_plan_ws_2026_minji_v5/action';
const TOPIC_DRAW = 'samsung_plan_ws_2026_minji_v5/draw';

app.use(express.static(path.join(__dirname)));
app.get('/health', (_req, res) => res.json({ ok: true, service: 'flyingpan-workshop-game' }));

const scores = Object.fromEntries(Array.from({ length: 13 }, (_, i) => [`Table ${i + 1}`, 0]));
let state = {
  game: 'lobby', roundId: 0, scores, players: [], drawer: null, drawingWord: '칼퇴근', secretNumber: 45,
  quizIndex: 0, oxIndex: 0,
  quizzes: [
    { q: '전략기획팀의 주력 업무가 아닌 것은?', choices: ['전략 수립', '기획실 운영', '직접 민원 응대', '경영 지원'], answer: 2 },
    { q: '더플라잉팬레드에서 가장 먼저 할 일은?', choices: ['메뉴판 정독', '게임 승리하기', '업무 보고하기', '멍 때리기'], answer: 1 }
  ],
  oxQuizzes: [
    { q: '사당동에 위치한 더플라잉팬레드는 회식 장소로 완벽하다?', answer: 'O' },
    { q: '오늘 워크샵에서 우승한 테이블은 칼퇴 특권이 주어진다?', answer: 'O' }
  ],
  choseongList: [ { target: 'ㄱ ㅈ', answer: '기획조정' }, { target: 'ㅅ ㅅ ㅈ ㄱ', answer: '삼성증권' } ]
};
let latestDrawing = 'CLEAR';
let speedWinner = null;
const scoredCorrect = new Set();
const speedFirstClick = new Set();

function clone(value) { return JSON.parse(JSON.stringify(value)); }
function validTable(table) { return /^Table (?:[1-9]|1[0-3])$/.test(String(table || '')); }
function cleanName(name) { return String(name || '').trim().slice(0, 32); }
function emitTopic(topic, payload, except) {
  const packet = { topic, payload: typeof payload === 'string' ? payload : JSON.stringify(payload) };
  if (except) except.broadcast.emit('mqtt:message', packet);
  else io.emit('mqtt:message', packet);
}
function stateFor(socket) {
  if (socket.data.isAdmin) return clone(state);
  const safe = clone(state);
  safe.secretNumber = null;
  safe.quizzes = safe.quizzes.map(({ answer, ...q }) => q);
  safe.oxQuizzes = safe.oxQuizzes.map(({ answer, ...q }) => q);
  safe.choseongList = safe.choseongList.map(({ answer, ...q }) => q);
  safe.players = safe.players.map(({ socketId, ...player }) => player);
  return safe;
}
function publishState() {
  for (const socket of io.sockets.sockets.values()) {
    socket.emit('mqtt:message', { topic: TOPIC_STATE, payload: JSON.stringify(stateFor(socket)) });
  }
}
function publishAction(action) { emitTopic(TOPIC_ACTION, action); }
function feedback(socket, kind, message, extra = {}) { socket.emit('game:feedback', { kind, message, ...extra }); }
function scoreTable(table, amount) {
  if (!validTable(table)) return;
  state.scores[table] = Number(state.scores[table] || 0) + amount;
  publishState();
}
function markCorrectOnce(key) {
  if (scoredCorrect.has(key)) return false;
  scoredCorrect.add(key);
  return true;
}
function actionLog(type, player, score = 0, detail = '') {
  const action = { type, table: player.table, name: player.name, score, detail };
  publishAction(action);
}
function sameDrawer(player) {
  return !!state.drawer && state.drawer.table === player.table && state.drawer.name === player.name;
}

io.on('connection', (socket) => {
  socket.data.isAdmin = false;
  socket.data.player = null;
  socket.emit('mqtt:message', { topic: TOPIC_STATE, payload: JSON.stringify(stateFor(socket)) });
  socket.emit('mqtt:message', { topic: TOPIC_DRAW, payload: latestDrawing });

  socket.on('admin:auth', (password, callback) => {
    const ok = typeof password === 'string' && password === ADMIN_PASSWORD;
    if (ok) {
      socket.data.isAdmin = true;
      if (typeof callback === 'function') callback({ ok: true, state: clone(state) });
      socket.emit('mqtt:message', { topic: TOPIC_STATE, payload: JSON.stringify(stateFor(socket)) });
    } else if (typeof callback === 'function') callback({ ok: false });
  });

  socket.on('mqtt:publish', (packet = {}) => {
    const topic = String(packet.topic || '');
    const raw = String(packet.payload ?? '');

    if (topic === TOPIC_STATE) {
      if (!socket.data.isAdmin) return socket.emit('server:error', '관리자만 게임 상태를 변경할 수 있습니다.');
      let incoming;
      try { incoming = JSON.parse(raw); } catch { return socket.emit('server:error', '게임 상태 데이터 형식이 올바르지 않습니다.'); }
      if (!incoming || typeof incoming !== 'object' || !Array.isArray(incoming.quizzes) || !Array.isArray(incoming.oxQuizzes)) return;
      const previous = state;
      const previousGame = state.game;
      // Keep the server's live roster; configuration and scores can be changed by the authenticated admin.
      const livePlayers = state.players;
      const safeScores = { ...state.scores, ...(incoming.scores || {}) };
      for (let i = 1; i <= 13; i++) safeScores[`Table ${i}`] = Number(safeScores[`Table ${i}`] || 0);
      state = {
        ...state, ...incoming,
        players: livePlayers,
        scores: safeScores,
        game: ['lobby', 'quiz', 'ox', 'drawing', 'choseong', 'balance', 'speed'].includes(incoming.game) ? incoming.game : state.game,
        secretNumber: Math.max(1, Math.min(100, Number(incoming.secretNumber) || 45)),
        quizzes: incoming.quizzes.slice(0, 200),
        oxQuizzes: incoming.oxQuizzes.slice(0, 200),
        choseongList: Array.isArray(incoming.choseongList) ? incoming.choseongList.slice(0, 200) : state.choseongList
      };
      const newRound = Number(state.roundId || 0) !== Number(previous.roundId || 0);
      if (newRound || state.game !== previousGame) {
        scoredCorrect.clear();
        speedWinner = null;
        if (state.game === 'drawing' && previousGame !== 'drawing') { latestDrawing = 'CLEAR'; emitTopic(TOPIC_DRAW, latestDrawing); }
      } else {
        if (state.quizIndex !== previous.quizIndex) for (const key of [...scoredCorrect]) if (key.startsWith('quiz:')) scoredCorrect.delete(key);
        if (state.oxIndex !== previous.oxIndex) for (const key of [...scoredCorrect]) if (key.startsWith('ox:')) scoredCorrect.delete(key);
        if (state.drawingWord !== previous.drawingWord || JSON.stringify(state.drawer) !== JSON.stringify(previous.drawer)) for (const key of [...scoredCorrect]) if (key.startsWith('drawing:')) scoredCorrect.delete(key);
        if (state.secretNumber !== previous.secretNumber) for (const key of [...scoredCorrect]) if (key.startsWith('updown:')) scoredCorrect.delete(key);
        if (JSON.stringify(state.choseongList) !== JSON.stringify(previous.choseongList)) for (const key of [...scoredCorrect]) if (key.startsWith('choseong:')) scoredCorrect.delete(key);
      }
      publishState();
      return;
    }

    if (topic === TOPIC_DRAW) {
      if (!socket.data.isAdmin && !socket.data.player) return;
      const player = socket.data.player;
      if (!socket.data.isAdmin && !sameDrawer(player)) return;
      if (raw !== 'CLEAR' && (!raw.startsWith('data:image/png;base64,') && !raw.startsWith('data:image/jpeg;base64,'))) return;
      latestDrawing = raw;
      emitTopic(TOPIC_DRAW, raw);
      return;
    }

    if (topic !== TOPIC_ACTION) return;
    let action;
    try { action = JSON.parse(raw); } catch { return; }
    if (!action || typeof action.type !== 'string') return;

    if (action.type === 'JOIN') {
      const input = action.player || {};
      const table = String(input.table || '');
      const name = cleanName(input.name);
      if (!validTable(table) || !name) return socket.emit('server:error', '테이블 번호와 닉네임을 확인하세요.');
      socket.data.player = { table, name, isHost: false };
      const existing = state.players.findIndex(p => p.socketId === socket.id);
      if (existing >= 0) state.players.splice(existing, 1);
      state.players.push({ table, name, socketId: socket.id });
      publishState();
      publishAction({ type: 'JOIN', player: { table, name } });
      return;
    }
    if (action.type === 'SHOW_RESULT') {
      if (socket.data.isAdmin) publishAction(action);
      return;
    }
    if (action.type === 'ADMIN_SCORE') {
      if (!socket.data.isAdmin || !validTable(action.table)) return;
      const delta = Math.max(-10000, Math.min(10000, Number(action.delta) || 0));
      scoreTable(action.table, delta);
      return;
    }

    const player = socket.data.player;
    if (!player || !validTable(player.table)) return socket.emit('server:error', '먼저 테이블에 입장하세요.');
    const table = player.table;
    const name = player.name;

    if (action.type === 'QUIZ_ANSWER') {
      const qIdx = Number(action.qIdx);
      const q = state.quizzes[state.quizIndex];
      if (state.game !== 'quiz' || qIdx !== state.quizIndex || !q || !Array.isArray(q.choices)) return;
      const key = `quiz:${qIdx}:${table}`;
      if (scoredCorrect.has(key)) return feedback(socket, 'info', '이 테이블은 이미 해당 문제의 정답 점수를 받았습니다.');
      const correct = Number(action.choice) === Number(q.answer);
      if (correct) {
        if (!markCorrectOnce(key)) return;
        scoreTable(table, 10); actionLog('CORRECT', player, 10); feedback(socket, 'correct', '정답! 테이블 점수 +10점 🎉');
      } else { actionLog('WRONG', player, 0, q.choices[Number(action.choice)] || '선택 답안'); feedback(socket, 'wrong', '오답입니다. 다시 도전하세요.'); }
      return;
    }

    if (action.type === 'OX_ANSWER') {
      const qIdx = Number(action.qIdx); const q = state.oxQuizzes[state.oxIndex];
      if (state.game !== 'ox' || qIdx !== state.oxIndex || !q) return;
      const key = `ox:${qIdx}:${table}`;
      if (scoredCorrect.has(key)) return feedback(socket, 'info', '이 테이블은 이미 해당 문제의 정답 점수를 받았습니다.');
      const choice = String(action.choice || '').toUpperCase();
      if (choice === String(q.answer).toUpperCase()) {
        if (!markCorrectOnce(key)) return;
        scoreTable(table, 10); actionLog('CORRECT', player, 10); feedback(socket, 'correct', '정답! 테이블 점수 +10점 🎉');
      } else { actionLog('WRONG', player, 0, choice); feedback(socket, 'wrong', '오답입니다.'); }
      return;
    }

    if (action.type === 'CHOSEONG_ANSWER') {
      if (state.game !== 'choseong') return;
      const item = state.choseongList[0]; if (!item) return;
      const key = `choseong:0:${table}`;
      if (scoredCorrect.has(key)) return feedback(socket, 'info', '이 테이블은 이미 정답 점수를 받았습니다.');
      if (String(action.value || '').trim().replace(/\s/g, '') === String(item.answer || '').trim().replace(/\s/g, '')) {
        if (!markCorrectOnce(key)) return;
        scoreTable(table, 10); actionLog('CORRECT', player, 10); feedback(socket, 'correct', '정답 인정! +10점!');
      } else { actionLog('WRONG', player, 0, String(action.value || '').slice(0, 80)); feedback(socket, 'wrong', '아닙니다. 다시 입력해 보세요.'); }
      return;
    }

    if (action.type === 'DRAW_GUESS') {
      if (state.game !== 'drawing' || sameDrawer(player)) return;
      const key = `drawing:${table}`;
      if (scoredCorrect.has(key)) return feedback(socket, 'info', '이 테이블은 이미 정답 점수를 받았습니다.');
      if (String(action.value || '').trim().toLowerCase() === String(state.drawingWord || '').trim().toLowerCase()) {
        if (!markCorrectOnce(key)) return;
        scoreTable(table, 20); actionLog('CORRECT', player, 20); feedback(socket, 'correct', '🎉 정답입니다! 테이블 점수 +20점!');
      } else { actionLog('WRONG', player, 0, String(action.value || '').slice(0, 80)); feedback(socket, 'wrong', '아닙니다. 다시 맞혀 보세요.'); }
      return;
    }

    if (action.type === 'UPDOWN_GUESS') {
      if (state.game !== 'balance') return;
      const value = Number(action.value);
      if (!Number.isInteger(value) || value < 1 || value > 100) return;
      if (value === Number(state.secretNumber)) {
        const key = `updown:${table}`;
        if (!markCorrectOnce(key)) return feedback(socket, 'info', '이 테이블은 이미 정답 점수를 받았습니다.');
        scoreTable(table, 30); actionLog('CORRECT', player, 30); feedback(socket, 'correct', '🎉 정답! 테이블 점수 +30점!', { status: '정답입니다!' });
      } else {
        const status = value < Number(state.secretNumber) ? `UP: ${value}보다 큰 숫자입니다.` : `DOWN: ${value}보다 작은 숫자입니다.`;
        actionLog('WRONG', player, 0, status); feedback(socket, 'wrong', status, { status });
      }
      return;
    }

    if (action.type === 'SPEED_CLICK') {
      if (state.game !== 'speed') return;
      if (speedWinner) return feedback(socket, 'info', '이번 라운드 1등이 이미 결정되었습니다.');
      speedWinner = socket.id;
      scoreTable(table, 20);
      publishAction({ type: 'SPEED_CLICK', table, name, score: 20 });
      feedback(socket, 'correct', '광클 1등! 테이블 점수 +20점 ⚡');
    }
  });

  socket.on('disconnect', () => {
    if (socket.data.player) {
      state.players = state.players.filter(p => p.socketId !== socket.id);
      publishState();
    }
  });
});

server.listen(PORT, '0.0.0.0', () => console.log(`Workshop game listening on ${PORT}`));
