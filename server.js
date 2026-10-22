const path=require('path'),express=require('express'),http=require('http');
const {Server}=require('socket.io');
const app=express(),server=http.createServer(app),io=new Server(server,{maxHttpBufferSize:5e6,cors:{origin:true}});
const PORT=process.env.PORT||3000,ADMIN_PASSWORD=process.env.ADMIN_PASSWORD||'3693';
const TOPIC_STATE='samsung_plan_ws_2026_minji_v5/state',TOPIC_ACTION='samsung_plan_ws_2026_minji_v5/action',TOPIC_DRAW='samsung_plan_ws_2026_minji_v5/draw';
app.use(express.static(__dirname));app.get('/',(_,res)=>res.sendFile(path.join(__dirname,'index.html')));app.get('/health',(_,res)=>res.json({ok:true,service:'earth-arcade'}));
const scores=Object.fromEntries(Array.from({length:13},(_,i)=>['Table '+(i+1),0]));
let state={game:'lobby',roundId:0,scores,players:[],drawer:null,drawingWord:'칼퇴근',drawingIndex:0,drawingQuestions:[{word:'칼퇴근',drawer:null}],secretNumber:45,quizIndex:0,oxIndex:0,choseongIndex:0,balanceIndex:0,speedIndex:0,quizzes:[{q:'전략기획팀의 주력 업무가 아닌 것은?',choices:['전략 수립','기획실 운영','직접 민원 응대','경영 지원'],answer:2},{q:'더플라잉팬레드에서 가장 먼저 할 일은?',choices:['메뉴판 정독','게임 승리하기','업무 보고하기','멍 때리기'],answer:1}],oxQuizzes:[{q:'사당동에 위치한 더플라잉팬레드는 회식 장소로 완벽하다?',answer:'O'},{q:'오늘 워크샵에서 우승한 테이블은 칼퇴 특권이 주어진다?',answer:'O'}],choseongList:[{target:'ㄱ ㅈ',answer:'기획조정'},{target:'ㅅ ㅅ ㅈ ㄱ',answer:'삼성증권'}]};
let latestDrawing='CLEAR';const scoredCorrect=new Set();
let speedGoTimer=null;
state.speedPhase='waiting';state.speedWinners=[];
const durations={quiz:10000,drawing:300000,choseong:30000,ox:10000,balance:300000};
state.startedAt=null;state.deadline=null;state.revealed=false;state.revealData=null;state.midScoreVisible=false;
const roundWinners=[];
function roundResult(){const i=state.game==='quiz'?state.quizIndex:state.game==='ox'?state.oxIndex:state.game==='choseong'?state.choseongIndex:0;
 let answer=state.game==='quiz'?state.quizzes[i]?.choices?.[state.quizzes[i]?.answer]:state.game==='ox'?state.oxQuizzes[i]?.answer:state.game==='choseong'?state.choseongList[i]?.answer:state.game==='drawing'?state.drawingWord:state.game==='balance'?state.secretNumber:null;
 const explanation=state.game==='ox'?(state.oxQuizzes[i]?.explanation||''):'';
 return {game:state.game,answer:state.game==='speed'?'선착순 3개 테이블':answer,explanation,rankings:roundWinners.map(x=>({...x}))};}
function startRound(){if(speedGoTimer){clearTimeout(speedGoTimer);speedGoTimer=null;}state.speedPhase='waiting';state.speedWinners=[];state.startedAt=Date.now();state.deadline=durations[state.game]?state.startedAt+durations[state.game]:null;state.revealed=false;state.revealData=null;roundWinners.length=0;}
function accepting(socket){if(!durations[state.game])return true;if(state.revealed||!state.deadline||Date.now()>=state.deadline){feedback(socket,'info','⏰ 정답 접수가 마감되었습니다.');return false;}return true;}

const validTable=t=>/^Table (?:[1-9]|1[0-3])$/.test(String(t||''));
const emit=(topic,payload)=>io.emit('mqtt:message',{topic,payload:typeof payload==='string'?payload:JSON.stringify(payload)});
function stateFor(socket){const s=JSON.parse(JSON.stringify(state));if(!socket.data.isAdmin){if(!state.revealed){s.revealData=null;}if(!state.revealed)s.speedWinners=[];if(!sameDrawer(socket.data.player))s.drawingWord=null;s.secretNumber=null;s.quizzes=s.quizzes.map(({answer,...q})=>q);s.oxQuizzes=s.oxQuizzes.map(({answer,...q})=>q);s.choseongList=s.choseongList.map(({answer,...q})=>q);s.drawingQuestions=s.drawingQuestions.map(q=>({word:null,drawer:q.drawer}));s.players=s.players.map(({socketId,...p})=>p);}return s;}
function publishState(){for(const socket of io.sockets.sockets.values())socket.emit('mqtt:message',{topic:TOPIC_STATE,payload:JSON.stringify(stateFor(socket))});}
function feedback(socket,kind,message,extra={}){socket.emit('game:feedback',{kind,message,...extra});}
function log(type,player,score=0,detail=''){emit(TOPIC_ACTION,{type,table:player.table,name:player.name,score,detail});}
function award(socket,player,key,points,message){if(scoredCorrect.has(key))return feedback(socket,'info','이 테이블은 이미 이번 문제의 점수를 받았습니다.');scoredCorrect.add(key);roundWinners.push({table:player.table,name:player.name,points});state.scores[player.table]=(state.scores[player.table]||0)+points;publishState();log('CORRECT',player,points);feedback(socket,'correct',message);}
function sameDrawer(p){return !!p&&!!state.drawer&&p.table===state.drawer.table&&p.name===state.drawer.name;}
io.on('connection',socket=>{
 socket.data.isAdmin=false;socket.data.player=null;
 socket.emit('mqtt:message',{topic:TOPIC_STATE,payload:JSON.stringify(stateFor(socket))});socket.emit('mqtt:message',{topic:TOPIC_DRAW,payload:latestDrawing});
 socket.on('admin:auth',(password,cb)=>{const ok=password===ADMIN_PASSWORD;if(ok){socket.data.isAdmin=true;socket.emit('mqtt:message',{topic:TOPIC_STATE,payload:JSON.stringify(stateFor(socket))});}if(typeof cb==='function')cb(ok?{ok:true,state:JSON.parse(JSON.stringify(state))}:{ok:false});});
 socket.on('mqtt:publish',packet=>{
  const topic=String(packet?.topic||''),raw=String(packet?.payload??'');
  if(topic===TOPIC_STATE){if(!socket.data.isAdmin)return socket.emit('server:error','관리자 권한이 필요합니다.');let incoming;try{incoming=JSON.parse(raw);}catch{return;}
   if(!incoming||!Array.isArray(incoming.quizzes)||!Array.isArray(incoming.oxQuizzes))return;
   const old=state,players=state.players;
   const safeScores={...old.scores};for(let i=1;i<=13;i++){const t='Table '+i;const n=Number(incoming.scores?.[t]);if(Number.isFinite(n))safeScores[t]=n;}
   const drawingQuestions=Array.isArray(incoming.drawingQuestions)&&incoming.drawingQuestions.length?incoming.drawingQuestions.slice(0,200).map(q=>({word:String(q.word||'').slice(0,100),drawer:q.drawer||null})).filter(q=>q.word):old.drawingQuestions;
   state={...old,...incoming,players,scores:safeScores,drawingQuestions:drawingQuestions.length?drawingQuestions:old.drawingQuestions,quizzes:incoming.quizzes.slice(0,200),oxQuizzes:incoming.oxQuizzes.slice(0,200),choseongList:Array.isArray(incoming.choseongList)?incoming.choseongList.slice(0,200):old.choseongList,secretNumber:Math.min(100,Math.max(1,Number(incoming.secretNumber)||old.secretNumber)),game:['lobby','quiz','drawing','choseong','ox','balance','speed'].includes(incoming.game)?incoming.game:old.game};
   const roundChanged=state.roundId!==old.roundId||state.game!==old.game||state.quizIndex!==old.quizIndex||state.oxIndex!==old.oxIndex||state.choseongIndex!==old.choseongIndex||state.drawingIndex!==old.drawingIndex||state.balanceIndex!==old.balanceIndex||state.speedIndex!==old.speedIndex;
   if(roundChanged){startRound();scoredCorrect.clear();if(state.game==='drawing'){latestDrawing='CLEAR';emit(TOPIC_DRAW,'CLEAR');}}
   if(state.game==='drawing'&&(state.drawingWord!==old.drawingWord||JSON.stringify(state.drawer)!==JSON.stringify(old.drawer))){scoredCorrect.clear();latestDrawing='CLEAR';emit(TOPIC_DRAW,'CLEAR');}
   if(state.secretNumber!==old.secretNumber)for(const k of [...scoredCorrect])if(k.startsWith('updown:'))scoredCorrect.delete(k);
   state.midScoreVisible=old.midScoreVisible;
   if(!roundChanged){state.speedPhase=old.speedPhase;state.speedWinners=old.speedWinners;state.startedAt=old.startedAt;state.deadline=old.deadline;state.revealed=old.revealed;state.revealData=old.revealData;}
   publishState();return;
  }
  if(topic===TOPIC_DRAW){if(!socket.data.isAdmin&&!sameDrawer(socket.data.player))return;if(state.game==='drawing'&&state.deadline&&Date.now()>=state.deadline)return;if(raw!=='CLEAR'&&!/^data:image\/(png|jpeg);base64,/.test(raw))return;latestDrawing=raw;emit(TOPIC_DRAW,raw);return;}
  if(topic!==TOPIC_ACTION)return;let action;try{action=JSON.parse(raw);}catch{return;}if(!action||typeof action.type!=='string')return;
  if(action.type==='JOIN'){const input=action.player||{},table=String(input.table||''),name=String(input.name||'').trim().slice(0,32);if(!validTable(table)||!name)return socket.emit('server:error','테이블과 이름을 입력하세요.');socket.data.player={table,name};state.players=state.players.filter(p=>p.socketId!==socket.id);state.players.push({table,name,socketId:socket.id});publishState();emit(TOPIC_ACTION,{type:'JOIN',player:{table,name}});return;}
  if(action.type==='SHOW_MID_SCORE'){if(!socket.data.isAdmin)return;state.midScoreVisible=true;publishState();return;}
  if(action.type==='CLOSE_MID_SCORE'){if(!socket.data.isAdmin)return;state.midScoreVisible=false;publishState();return;}
  if(action.type==='SPEED_START'){if(!socket.data.isAdmin||state.game!=='speed'||state.speedPhase!=='waiting')return;state.speedPhase='ready';publishState();const round=state.roundId;speedGoTimer=setTimeout(()=>{speedGoTimer=null;if(state.game==='speed'&&state.roundId===round&&state.speedPhase==='ready'){state.speedPhase='go';publishState();}},1500+Math.floor(Math.random()*2000));return;}
  if(action.type==='REVEAL_ANSWER'){if(!socket.data.isAdmin||(!durations[state.game]&&state.game!=='speed'))return;state.revealed=true;state.revealData=roundResult();publishState();return;}
  if(action.type==='SHOW_RESULT'){if(socket.data.isAdmin)emit(TOPIC_ACTION,action);return;}
  if(action.type==='ADMIN_SCORE'){if(!socket.data.isAdmin||!validTable(action.table))return;state.scores[action.table]+=(Number(action.delta)||0);publishState();return;}
  const player=socket.data.player;if(!player)return socket.emit('server:error','먼저 참가자 등록을 해주세요.');const table=player.table;
  if(action.type==='QUIZ_ANSWER'){const q=state.quizzes[state.quizIndex];if(state.game!=='quiz'||Number(action.qIdx)!==state.quizIndex||!q||!accepting(socket))return;const key=`quiz:${state.quizIndex}:${table}`;if(Number(action.choice)===Number(q.answer)){const rank=roundWinners.length;const points=[20,15,10][rank]||5;award(socket,player,key,points,`정답! 테이블 점수 +${points}점 🎉`);}else{log('WRONG',player,0,'객관식');feedback(socket,'wrong','오답입니다.');}return;}
  if(action.type==='OX_ANSWER'){const q=state.oxQuizzes[state.oxIndex];if(state.game!=='ox'||Number(action.qIdx)!==state.oxIndex||!q||!accepting(socket))return;if(String(action.choice).toUpperCase()===String(q.answer).toUpperCase())award(socket,player,`ox:${state.oxIndex}:${table}`,10,'정답! +10점 🎉');else{log('WRONG',player,0,'OX');feedback(socket,'wrong','오답입니다.');}return;}
  if(action.type==='CHOSEONG_ANSWER'){const q=state.choseongList[state.choseongIndex||0];if(state.game!=='choseong'||!q||!accepting(socket))return;if(String(action.value||'').replace(/\s/g,'')===String(q.answer||'').replace(/\s/g,''))award(socket,player,`choseong:${state.choseongIndex||0}:${table}`,10,'정답 인정! +10점!');else{log('WRONG',player,0,String(action.value||'').slice(0,80));feedback(socket,'wrong','아닙니다.');}return;}
  if(action.type==='DRAW_GUESS'){if(state.game!=='drawing'||sameDrawer(player)||!accepting(socket))return;if(String(action.value||'').trim().toLowerCase()===String(state.drawingWord||'').trim().toLowerCase())award(socket,player,`drawing:${state.drawingIndex||0}:${table}`,20,'🎉 정답입니다! +20점!');else{log('WRONG',player,0,String(action.value||'').slice(0,80));feedback(socket,'wrong','아닙니다.');}return;}
  if(action.type==='UPDOWN_GUESS'){if(state.game!=='balance'||!accepting(socket))return;const n=Number(action.value);if(!Number.isInteger(n)||n<1||n>100)return;if(n===state.secretNumber)award(socket,player,`updown:${state.balanceIndex||0}:${table}`,30,'🎉 정답! +30점!');else{const status=n<state.secretNumber?`UP: ${n}보다 큰 숫자입니다.`:`DOWN: ${n}보다 작은 숫자입니다.`;log('WRONG',player,0,status);feedback(socket,'wrong',status,{status});}return;}
  if(action.type==='SPEED_CLICK'){if(state.game!=='speed')return;if(state.speedPhase!=='go')return feedback(socket,'info','아직 시작 전이거나 선착순 접수가 끝났습니다.');if(state.speedWinners.some(x=>x.table===table))return feedback(socket,'info','이미 이 테이블이 선착순에 선정됐습니다.');const rank=state.speedWinners.length+1;const points=[30,20,10][rank-1];if(!points)return;const winner={table,name:player.name,points,rank};state.speedWinners.push(winner);roundWinners.push(winner);state.scores[table]+=points;if(state.speedWinners.length===3)state.speedPhase='closed';publishState();emit(TOPIC_ACTION,{type:'SPEED_CLICK',table,name:player.name,score:points,rank});feedback(socket,'correct',`접수 완료! 결과 발표를 기다려주세요.`);return;}
 });
 socket.on('disconnect',()=>{if(socket.data.player){state.players=state.players.filter(p=>p.socketId!==socket.id);publishState();}});
});
server.listen(PORT,'0.0.0.0',()=>console.log('Earth arcade listening on '+PORT));
