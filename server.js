const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');
const path = require('path');
const { Redis } = require('@upstash/redis');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: true, credentials: true }, pingInterval: 10000, pingTimeout: 30000, connectTimeout: 20000, maxHttpBufferSize: 1e6 });
app.use(express.static(path.join(__dirname)));

const PORT = process.env.PORT || 3000;
const START_CHIPS = 20000;
const START_SB = 100;
const START_BB = 200;
const BLIND_INTERVAL = 10 * 60 * 1000;
const RECONNECT_GRACE = 10 * 60 * 1000;
const MAX_PLAYERS = 9;
const ALL_IN_SHOWDOWN_DELAY = 12000;
const RESULT_SHOW_DELAY = 12000;
const FOLD_RESULT_DELAY = 5500;
const ACTION_TIMEOUT = 30 * 1000;
const rooms = new Map();
const REDIS_ROOMS_KEY = 'poker:rooms:v3';
let redis = null;
let persistChain = Promise.resolve();
if(process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN){
  redis = Redis.fromEnv();
  console.log('[REDIS] Upstash persistence enabled');
}else{
  console.log('[REDIS] Environment variables not configured; using in-memory rooms only');
}

function serializeRoom(r){
  const cleanPlayer=p=>({
    token:p.token,nickname:p.nickname,seat:p.seat,chips:p.chips,ready:p.ready,isHost:p.isHost,
    offline:p.offline,status:p.status,inHand:p.inHand,folded:p.folded,allIn:p.allIn,holeCards:p.holeCards,
    totalBet:p.totalBet,roundBet:p.roundBet,avatar:p.avatar,reconnectUntil:p.reconnectUntil||0
  });
  return {
    code:r.code,hostId:r.hostId,phase:r.phase,smallBlind:r.smallBlind,bigBlind:r.bigBlind,nextBlindAt:r.nextBlindAt,
    showdownStarted:!!r.showdownStarted,revealAllShowdown:!!r.revealAllShowdown,winnerIds:r.winnerIds||[],
    deck:r.deck||[],community:r.community||[],dealerSeat:r.dealerSeat,hasStartedHand:!!r.hasStartedHand,sbSeat:r.sbSeat,bbSeat:r.bbSeat,
    currentPlayerId:r.currentPlayerId,street:r.street,pot:r.pot,minRaiseTo:r.minRaiseTo,actionDeadline:r.actionDeadline||null,
    resultDeadline:r.resultDeadline||null,nextHandAt:r.nextHandAt||null,streetAdvanceAt:r.streetAdvanceAt||null,
    revealShowdown:!!r.revealShowdown,players:r.players.map(cleanPlayer),
    acted:[...(r.acted||[])],pending:[...(r.pending||[])],
    actedTokens:[...(r.acted||[])].map(id=>r.players.find(p=>p.id===id)?.token).filter(Boolean),
    pendingTokens:[...(r.pending||[])].map(id=>r.players.find(p=>p.id===id)?.token).filter(Boolean),
    currentPlayerToken:r.currentPlayerId?r.players.find(p=>p.id===r.currentPlayerId)?.token||null:null,
    pendingResults:r.pendingResults||null,
    pendingWinnings:r.pendingWinnings?[...r.pendingWinnings.entries()]:null,pendingPot:r.pendingPot||0,lastResults:r.lastResults||null
  };
}
function snapshotRooms(){
  const out={};
  for(const [code,r] of rooms)out[code]=serializeRoom(r);
  return out;
}
function schedulePersist(){
  if(!redis)return;
  const snapshot=snapshotRooms();
  persistChain=persistChain.then(()=>redis.set(REDIS_ROOMS_KEY,snapshot)).catch(e=>console.error('[REDIS] save failed:',e.message));
}
function scheduleDeleteRoom(code){
  if(!redis)return;
  persistChain=persistChain.then(async()=>{
    const snapshot=snapshotRooms();
    await redis.set(REDIS_ROOMS_KEY,snapshot);
  }).catch(e=>console.error('[REDIS] delete/save failed:',e.message));
}
function restoreRoom(raw){
  if(!raw||!raw.code||!Array.isArray(raw.players))return null;
  const r={...raw,blindTimer:null,showdownTimer:null,resultTimer:null,actionTimer:null,acted:new Set(),pending:new Set(),pendingWinnings:raw.pendingWinnings?new Map(raw.pendingWinnings):null};
  r.players=r.players.map(p=>({...p,id:null,socket:null,offline:true,status:'离线',reconnectUntil:Date.now()+RECONNECT_GRACE,_showdownVisible:false}));
  r._actedTokens=new Set(raw.actedTokens||[]);
  r._pendingTokens=new Set(raw.pendingTokens||[]);
  r._currentPlayerToken=raw.currentPlayerToken||null;
  // A restart invalidates old socket ids. Players recover through their durable token.
  r.hostId=null;
  const host=r.players.find(p=>p.isHost);
  if(host)host.reconnectUntil=Date.now()+RECONNECT_GRACE;
  // Keep the persisted host marker on the player; hostId is rebound when that player resumes.
  if(r.phase==='lobby'){
    r.currentPlayerId=null;
    r.pending=new Set();
    r.acted=new Set();
    r._currentPlayerToken=null;
    r._pendingTokens=new Set();
    r._actedTokens=new Set();
  }
  return r;
}
async function loadPersistedRooms(){
  if(!redis)return;
  try{
    const saved=await redis.get(REDIS_ROOMS_KEY);
    if(saved&&typeof saved==='object'){
      for(const [code,raw] of Object.entries(saved)){
        const r=restoreRoom(raw);if(r)rooms.set(code,r);
      }
    }
    console.log(`[REDIS] restored ${rooms.size} room(s)`);
  }catch(e){console.error('[REDIS] restore failed:',e.message)}
}
function armRestoredTimers(r){
  if(!rooms.has(r.code))return;
  const now=Date.now();
  // Blind timer always follows the persisted nextBlindAt.
  clearTimeout(r.blindTimer);
  const blindDelay=Math.max(0,(r.nextBlindAt||now+BLIND_INTERVAL)-now);
  r.blindTimer=setTimeout(()=>blindUp(r),blindDelay);

  if(r.phase==='playing'){
    const actor=r.players.find(p=>p.id===r.currentPlayerId && p.socket && p.inHand&&!p.folded&&!p.allIn);
    if(actor){
      const delay=Math.max(0,(r.actionDeadline||now+ACTION_TIMEOUT)-now);
      clearActionTimer(r);
      r.actionDeadline=now+delay;
      r.actionTimer=setTimeout(()=>{
        if(!rooms.has(r.code)||r.phase!=='playing'||r.currentPlayerId!==actor.id)return;
        notice(r,`${actor.nickname} 操作超时，自动弃牌`);actor.folded=true;actor.status='超时弃牌';r.pending.delete(actor.id);r.acted.add(actor.id);advanceAfterAction(r,actor);
      },delay);
    }else if(r.currentPlayerId===null && !r._currentPlayerToken){
      // Server restarted during an automatic street transition / all-in runout.
      setTimeout(()=>{if(rooms.has(r.code)&&r.phase==='playing'&&r.currentPlayerId===null)advanceStreet(r)},1000);
    }
  }else if(r.phase==='showdown'){
    const deadline=r.resultDeadline||now+RESULT_SHOW_DELAY;
    const delay=Math.max(0,deadline-now);
    clearTimeout(r.resultTimer);
    r.resultTimer=setTimeout(()=>{
      if(r.pendingWinnings instanceof Map)finalizeShowdown(r);else finalizeNormalResult(r,r.winnerIds||[]);
    },delay);
  }else if(r.phase==='handEnd'){
    const delay=Math.max(0,(r.nextHandAt||now+3500)-now);
    setTimeout(()=>nextHand(r),delay);
  }
}
async function initPersistence(){
  await loadPersistedRooms();
  for(const r of rooms.values())armRestoredTimers(r);
}


const SUITS = ['♠','♥','♦','♣'];
const RANKS = ['2','3','4','5','6','7','8','9','10','J','Q','K','A'];
const RANK_VALUE = Object.fromEntries(RANKS.map((r,i)=>[r,i+2]));

function newDeck(){
  const d=[]; for(const s of SUITS) for(const r of RANKS)d.push({suit:s,rank:r});
  return d;
}
function shuffle(deck){
  for(let i=deck.length-1;i>0;i--){const j=crypto.randomInt(i+1);[deck[i],deck[j]]=[deck[j],deck[i]]}
  return deck;
}
function roomCode(){
  let c; do c=String(crypto.randomInt(1000,10000)); while(rooms.has(c)); return c;
}
function avatar(){return ['♠','♥','♦','♣','🐶','🐱','🦊','🐼','🐸'][crypto.randomInt(9)]}
function currentHand(cards){
  if(!cards || cards.length<2)return null;
  if(cards.length>=5)return bestHand(cards);
  const vals=cards.map(c=>RANK_VALUE[c.rank]);
  const counts={}; for(const v of vals) counts[v]=(counts[v]||0)+1;
  const groups=Object.values(counts).sort((a,b)=>b-a);
  if(groups[0]===4)return {name:'四条'};
  if(groups[0]===3)return {name:'三条'};
  if(groups[0]===2 && groups[1]===2)return {name:'两对'};
  if(groups[0]===2)return {name:'一对'};
  return {name:'高牌'};
}
function publicPlayer(p,viewerId,r){
  const revealed=!!p._showdownVisible;
  const isMe=p.id===viewerId;
  const handInfo=(revealed||isMe)?currentHand([...p.holeCards,...r.community]):null;
  return {id:p.id,seat:p.seat,nickname:p.nickname,chips:p.chips,ready:p.ready,isHost:p.isHost,offline:p.offline,status:p.status,inHand:p.inHand,avatar:p.avatar,blind:p.seat===p._sbSeat?'SB':(p.seat===p._bbSeat?'BB':''),roundBet:p.roundBet,totalBet:p.totalBet,showdownCards:revealed?p.holeCards:undefined,handName:handInfo?.name||''};
}
function alivePlayers(r){return r.players.filter(p=>p.inHand && !p.folded)}
function eligiblePlayers(r){return r.players.filter(p=>p.inHand && !p.folded && p.chips>=0)}
function activeBetters(r){return r.players.filter(p=>p.inHand && !p.folded && !p.allIn)}
function nextSeat(r,from, predicate=p=>p.inHand){
  for(let step=1;step<=MAX_PLAYERS;step++){const s=(from+step)%MAX_PLAYERS;const p=r.players.find(x=>x.seat===s);if(p&&predicate(p))return p}
  return null;
}
function playerBySeat(r,s){return r.players.find(p=>p.seat===s)}
function findNextActor(r,from){
  return nextSeat(r,from,p=>p.inHand&&!p.folded&&!p.allIn);
}
function nextPendingActor(r,from){
  return nextSeat(r,from,p=>p.inHand&&!p.folded&&!p.allIn&&r.pending.has(p.id));
}
function resetRoundBets(r, preserveExisting=false){
  for(const p of r.players){
    if(!preserveExisting) p.roundBet=0;
    p.status=p.inHand?(p.allIn?'全押':'等待'):'等待';
  }
}
function totalCommitted(r){return r.players.reduce((a,p)=>a+p.totalBet,0)}
function currentHighest(r){return Math.max(0,...alivePlayers(r).map(p=>p.roundBet))}
function pendingActors(r){return alivePlayers(r).filter(p=>!p.allIn && r.pending.has(p.id))}
function allBetsSettled(r){
  return pendingActors(r).length===0;
}

function assertHandInvariant(r){
  // 上线前安全检查：任何玩家的筹码、下注和底池不应出现负数；当前行动者必须是合法玩家。
  for(const p of r.players){
    if(p.chips<0 || p.roundBet<0 || p.totalBet<0) throw new Error('内部状态异常：出现负筹码或负下注');
    if(p.roundBet>p.totalBet) throw new Error('内部状态异常：本轮下注超过累计下注');
  }
  if(r.pot<0) throw new Error('内部状态异常：底池不能为负数');
  if(r.phase==='playing' && r.currentPlayerId){
    const actor=r.players.find(p=>p.id===r.currentPlayerId);
    if(!actor || !actor.inHand || actor.folded || actor.allIn) throw new Error('内部状态异常：当前行动者无效');
  }
}
function safeBroadcast(r){
  try{assertHandInvariant(r)}catch(e){console.error(`[STATE CHECK] room ${r.code}: ${e.message}`)}
  broadcastState(r);
}

function setPot(r){r.pot=totalCommitted(r)}
function toPublicState(r,socketId){
  const me=r.players.find(p=>p.id===socketId);
  const myCards=me?.holeCards || [];
  r.players.forEach(p=>{p._sbSeat=r.sbSeat;p._bbSeat=r.bbSeat;if(r.phase==='handEnd') p._showdownVisible=!!(!p.folded && r.revealAllShowdown); else if(r.phase!=='showdown') p._showdownVisible=false;});
  const players=r.players.map(p=>publicPlayer(p,socketId,r));
  return {
    roomCode:r.code,phase:r.phase,smallBlind:r.smallBlind,bigBlind:r.bigBlind,nextBlindAt:r.nextBlindAt,sbSeat:r.sbSeat,bbSeat:r.bbSeat,dealerSeat:r.dealerSeat,
    players,community:r.community,pot:r.pot,currentPlayerId:r.currentPlayerId,
    meId:socketId,me:me?{id:me.id,ready:me.ready,isHost:me.isHost,chips:me.chips,status:me.status,blind:me.seat===r.sbSeat?'SB':(me.seat===r.bbSeat?'BB':''),showdownVisible:!!me._showdownVisible,handName:currentHand([...me.holeCards,...r.community])?.name||''}:null,hostId:r.hostId,canStart:!!me && me.id===r.hostId && r.phase==='lobby' && r.players.filter(p=>p.ready&&p.chips>0).length>=2,readyCount:r.players.filter(p=>p.ready&&p.chips>0).length,meFolded:!!me?.folded,myCards,
    toCall:me?Math.max(0,currentHighest(r)-me.roundBet):0,
    highestBet:currentHighest(r),minRaiseTo:r.minRaiseTo,canCheck:me?currentHighest(r)===me.roundBet:false,
    actionDeadline:r.actionDeadline||null,
    winnerIds:r.winnerIds||[],
  };
}
function broadcastState(r){setPot(r);for(const p of r.players)if(p.socket)io.to(p.socket).emit('state',toPublicState(r,p.socket));schedulePersist()}
function notice(r,msg){io.to(r.code).emit('toast',msg)}
function clearActionTimer(r){
  clearTimeout(r.actionTimer);
  r.actionTimer=null;
  r.actionDeadline=null;
}
function startActionTimer(r,p){
  clearActionTimer(r);
  if(!p || r.phase!=='playing')return;
  r.actionDeadline=Date.now()+ACTION_TIMEOUT;
  const expectedId=p.id;
  r.actionTimer=setTimeout(()=>{
    if(!rooms.has(r.code)||r.phase!=='playing'||r.currentPlayerId!==expectedId)return;
    const actor=r.players.find(x=>x.id===expectedId);
    if(!actor)return;
    notice(r,`${actor.nickname} 操作超时，自动弃牌`);
    actor.folded=true;
    actor.status='超时弃牌';
    r.pending.delete(actor.id);
    r.acted.add(actor.id);
    advanceAfterAction(r,actor);
  },ACTION_TIMEOUT);
}

function startBlindTimer(r){
  r.nextBlindAt=Date.now()+BLIND_INTERVAL;
  clearTimeout(r.blindTimer);
  r.blindTimer=setTimeout(()=>blindUp(r),BLIND_INTERVAL);
}
function blindUp(r){
  if(!rooms.has(r.code))return;
  r.smallBlind*=2;r.bigBlind*=2;
  notice(r,`升盲：${r.smallBlind} / ${r.bigBlind}`);
  startBlindTimer(r);
  broadcastState(r);
}
function seatPlayers(r){
  const seated=r.players.filter(p=>p.inHand);
  return seated.sort((a,b)=>a.seat-b.seat);
}
function chooseDealer(r){
  const eligible=r.players.filter(p=>p.inHand&&p.chips>0&&!p.offline);
  if(!eligible.length){r.dealerSeat=null;return;}
  // 每一场游戏的第一手随机庄位；之后庄位按顺时针轮转，不再每手随机。
  // 若原庄家离桌/淘汰，则从原庄位顺时针寻找下一位仍有资格参赛的玩家。
  if(r.dealerSeat==null || !r.hasStartedHand){
    r.dealerSeat=eligible[crypto.randomInt(eligible.length)].seat;
  }else{
    const next=nextSeat(r,r.dealerSeat,p=>p.inHand&&p.chips>0&&!p.offline);
    r.dealerSeat=next?.seat ?? eligible[0].seat;
  }
  r.hasStartedHand=true;
}
function dealOne(r,p){p.holeCards.push(r.deck.pop())}
function prepareHand(r){
  const candidates=r.players.filter(p=>p.inHand && p.chips>0 && !p.offline);
  if(candidates.length<2){r.phase='lobby';for(const p of r.players)p.inHand=false;return false}
  r.players.forEach(p=>{p.inHand=candidates.includes(p);p.folded=false;p.allIn=false;p.totalBet=0;p.roundBet=0;p.holeCards=[];p.status=p.inHand?'等待':'旁观'});
  chooseDealer(r);
  r.deck=shuffle(newDeck());r.community=[];r.pot=0;r.currentPlayerId=null;r.minRaiseTo=r.bigBlind;r.acted=new Set();r.pending=new Set();
  candidates.forEach(p=>{dealOne(r,p);dealOne(r,p)});
  const dealer=playerBySeat(r,r.dealerSeat);
  let sb,bb,firstActor;
  if(candidates.length===2){
    // Heads-up：庄家同时是小盲，翻牌前庄家/小盲先行动，翻牌后庄家最后行动。
    sb=dealer;
    bb=nextSeat(r,dealer.seat,p=>p.inHand);
    firstActor=sb;
  }else{
    sb=nextSeat(r,dealer.seat,p=>p.inHand);
    bb=sb?nextSeat(r,sb.seat,p=>p.inHand):null;
    firstActor=bb?findNextActor(r,bb.seat):null;
  }
  r.sbSeat=sb?.seat;r.bbSeat=bb?.seat;
  postBlind(r,sb,r.smallBlind);postBlind(r,bb,r.bigBlind);
  // 翻牌前必须保留大小盲已经投入的筹码，不能把 roundBet 清零。
  resetRoundBets(r,true);
  r.phase='playing';
  r.street='preflop';
  r.pending=new Set(candidates.filter(p=>!p.allIn).map(p=>p.id));
  r.currentPlayerId=firstActor?.id||null;
  if(firstActor){firstActor.status='操作中';startActionTimer(r,firstActor)}
  if(!r.currentPlayerId)finishEarly(r);
  safeBroadcast(r);
  return true;
}
function postBlind(r,p,amount){
  if(!p)return;
  const pay=Math.min(p.chips,amount);p.chips-=pay;p.roundBet+=pay;p.totalBet+=pay;
  if(p.chips===0)p.allIn=true;
}
function startGame(r){
  if(r.phase!=='lobby')return;
  const ready=r.players.filter(p=>p.ready&&p.chips>0);
  if(ready.length<2)throw new Error('至少需要2名已准备且有筹码的玩家');
  r.players.forEach(p=>p.inHand=ready.includes(p));
  r.phase='playing';if(!r.blindTimer)startBlindTimer(r);prepareHand(r);broadcastState(r);
}
function validateActor(r,p){
  if(r.phase!=='playing')throw new Error('当前没有可操作的牌局');
  if(r.currentPlayerId!==p.id)throw new Error('还没轮到你');
  if(!p.inHand||p.folded||p.allIn)throw new Error('你当前不能操作');
}
function putChips(r,p,amount){
  const pay=Math.min(p.chips,amount);
  p.chips-=pay;p.roundBet+=pay;p.totalBet+=pay;
  if(p.chips===0)p.allIn=true;
  setPot(r);
  return pay;
}
function revealOwnCards(r,p){
  if(r.phase!=='showdown') throw new Error('当前不是摊牌/结果展示阶段');
  if(!p.holeCards?.length) throw new Error('当前没有可展示的底牌');
  p._showdownVisible=true;
  broadcastState(r);
  notice(r,p.folded?`${p.nickname} 展示了已弃牌的底牌（仅供查看，不影响本局胜负）`:`${p.nickname} 亮出了自己的底牌`);
}

function doAction(r,p,a){
  validateActor(r,p);
  clearActionTimer(r);
  const highest=currentHighest(r);
  let didRaise=false;
  const callNeed=Math.max(0,highest-p.roundBet);
  if(a.type==='fold'){p.folded=true;p.status='弃牌'}
  else if(a.type==='check'){
    // 德州规则：只要当前玩家的本轮投入低于桌面最高下注，就绝对不能过牌。
    // 翻牌前尤其如此：小盲/其他玩家必须至少跟到大盲；只有轮回到大盲，且没有人加注时，大盲才可以过牌。
    if(callNeed!==0)throw new Error(`当前不能过牌，还需要跟注 ${callNeed}`);
    if(r.street==='preflop' && p.id!==playerBySeat(r,r.bbSeat)?.id && p.roundBet<r.bigBlind){
      throw new Error(`翻牌前不能过牌，至少需要跟注到 ${r.bigBlind}`);
    }
    p.status='过牌';
  } else if(a.type==='call'){
    if(callNeed===0){p.status='过牌'}
    else {
      // 跟注只能补齐到当前最高的本轮投入，绝不能因为对手筹码更多而把自己的全部筹码一起投入。
      const payable=Math.min(callNeed,p.chips);
      putChips(r,p,payable);
      p.status=p.allIn?'全押':'跟注';
    }
  } else if(a.type==='raise'){
    const target=Number(a.amount);
    if(!Number.isInteger(target)||target<=highest)throw new Error('加注总投入必须高于当前最高下注');
    const minTarget=highest + r.minRaiseTo;
    const need=target-p.roundBet;
    if(need<=0)throw new Error('加注金额无效');
    const isAllInTarget=need>=p.chips;
    // 未达到最低完整加注额时，只有“把剩余筹码全部打光”的短加注才允许成立；短加注不会重新开启已行动玩家的下注权。
    if(target<minTarget && !isAllInTarget)throw new Error(`最低需要加到 ${minTarget}`);
    if(isAllInTarget){
      const before=p.roundBet;
      putChips(r,p,p.chips);p.status='全押';
      const raiseSize=p.roundBet-highest;
      if(raiseSize>=r.minRaiseTo){r.minRaiseTo=raiseSize;didRaise=true;}
    }else{
      putChips(r,p,need);r.minRaiseTo=target-highest;p.status='加注';didRaise=true;
    }
  } else if(a.type==='allin'){
    if(p.chips<=0)throw new Error('你已经全押');
    const beforeHighest=highest;
    const beforeBet=p.roundBet;
    putChips(r,p,p.chips);p.status='全押';
    const raiseSize=p.roundBet-beforeHighest;
    // All-in 只有达到完整最低加注额才会重新开启其他玩家的行动权；短加注只会让尚未行动/仍需跟注的人处理到新的最高下注。
    if(p.roundBet>beforeHighest && raiseSize>=r.minRaiseTo){
      r.minRaiseTo=raiseSize;didRaise=true;
    }
  } else throw new Error('未知操作');
  if(didRaise){
    // 完整加注：所有仍能下注的其他玩家重新获得行动权。
    r.pending=new Set(alivePlayers(r).filter(x=>x.id!==p.id && !x.allIn).map(x=>x.id));
  }else{
    r.pending.delete(p.id);
    // 短 All-in 虽然不能重新开启加注，但所有投入低于新最高下注的存活玩家仍必须面对新的跟注额。
    for(const x of alivePlayers(r)){
      if(!x.allIn && x.roundBet<currentHighest(r))r.pending.add(x.id);
    }
  }
  r.acted.add(p.id);
  advanceAfterAction(r,p);
}
function advanceAfterAction(r,p){
  if(alivePlayers(r).length===1){finishEarly(r);return}
  if(allBetsSettled(r)){advanceStreet(r);return}
  const next=nextPendingActor(r,p.seat);
  r.currentPlayerId=next?.id||null;
  if(next){next.status='操作中';startActionTimer(r,next)}
  safeBroadcast(r);
}
function burn(r){if(r.deck.length)r.deck.pop()}
function dealCommunity(r,n){for(let i=0;i<n;i++)r.community.push(r.deck.pop())}
function advanceStreet(r){
  // 单挑时一名玩家已经全押后，另一名玩家完成跟注即没有继续下注空间。
  // 不应让剩余玩家在翻牌、转牌、河牌逐街点击“过牌”，直接自动发完剩余公共牌。
  const live=alivePlayers(r);
  const headsUpAllIn=live.length===2 && live.some(p=>p.allIn);
  if(headsUpAllIn && r.street!=='river'){
    if(r.street==='preflop'){burn(r);dealCommunity(r,3);r.street='flop'}
    else if(r.street==='flop'){burn(r);dealCommunity(r,1);r.street='turn'}
    else if(r.street==='turn'){burn(r);dealCommunity(r,1);r.street='river'}
    r.pending.clear();
    r.currentPlayerId=null;
    safeBroadcast(r);
    setTimeout(()=>{
      if(rooms.has(r.code) && r.phase==='playing' && r.currentPlayerId===null) advanceStreet(r);
    },2200);
    return;
  }
  if(r.street==='preflop'){burn(r);dealCommunity(r,3);r.street='flop'}
  else if(r.street==='flop'){burn(r);dealCommunity(r,1);r.street='turn'}
  else if(r.street==='turn'){burn(r);dealCommunity(r,1);r.street='river'}
  else {
    // 河牌下注全部完成：不要直接弹出结果。先进入摊牌展示，让所有人看到完整公共牌和底牌，
    // 停留 RESULT_SHOW_DELAY 后再结算。
    beginShowdown(r);
    return;
  }
  resetRoundBets(r);r.minRaiseTo=r.bigBlind;r.acted=new Set();
  r.pending=new Set(alivePlayers(r).filter(p=>!p.allIn).map(p=>p.id));
  const first=nextSeat(r, r.dealerSeat, p=>p.inHand&&!p.folded&&!p.allIn&&r.pending.has(p.id));
  r.currentPlayerId=first?.id||null;
  // 给翻牌/转牌/河牌一个约2秒的展示时间，让玩家看清新牌后再开始下一轮倒计时。
  safeBroadcast(r);
  setTimeout(()=>{
    if(!rooms.has(r.code)||r.phase!=='playing')return;
    if(first){
      first.status='操作中';
      startActionTimer(r,first);
      safeBroadcast(r);
    } else if(r.street==='river'){
      beginShowdown(r);
    } else {
      advanceStreet(r);
    }
  },2000);
}
function evaluate5(cards){
  const vals=cards.map(c=>RANK_VALUE[c.rank]).sort((a,b)=>b-a);
  const counts={};for(const v of vals)counts[v]=(counts[v]||0)+1;
  const groups=Object.entries(counts).map(([v,c])=>({v:+v,c})).sort((a,b)=>b.c-a.c||b.v-a.v);
  const suits={};for(const c of cards)(suits[c.suit]??=[]).push(RANK_VALUE[c.rank]);
  const flush=Object.values(suits).some(a=>a.length===5);
  function straightHighOf(arr){
    const u=[...new Set(arr)].sort((a,b)=>b-a);
    if(u.includes(14))u.push(1);
    for(let i=0;i<=u.length-5;i++){const a=u.slice(i,i+5);if(a[0]-a[4]===4)return a[0]}
    return null;
  }
  const straightHigh=straightHighOf(vals);
  const flushSuit=Object.entries(suits).find(([,a])=>a.length===5);
  const flushStraightHigh=flushSuit?straightHighOf(flushSuit[1]):null;
  if(flushStraightHigh)return {cat:8,t:[flushStraightHigh],name:flushStraightHigh===14?'皇家同花顺':'同花顺'};
  if(groups[0]?.c===4)return {cat:7,t:[groups[0].v,groups.find(x=>x.c===1).v],name:'四条'};
  if(groups[0]?.c===3&&groups.some(x=>x.c===2))return {cat:6,t:[groups[0].v,groups.find(x=>x.c===2).v],name:'葫芦'};
  if(flush)return {cat:5,t:vals,name:'同花'};
  if(straightHigh)return {cat:4,t:[straightHigh],name:'顺子'};
  if(groups[0]?.c===3)return {cat:3,t:[groups[0].v,...groups.filter(x=>x.c===1).map(x=>x.v).sort((a,b)=>b-a)],name:'三条'};
  const pairs=groups.filter(x=>x.c===2).map(x=>x.v).sort((a,b)=>b-a);
  if(pairs.length>=2)return {cat:2,t:[pairs[0],pairs[1],groups.find(x=>x.c===1).v],name:'两对'};
  if(pairs.length===1)return {cat:1,t:[pairs[0],...groups.filter(x=>x.c===1).map(x=>x.v).sort((a,b)=>b-a)],name:'一对'};
  return {cat:0,t:vals,name:'高牌'};
}
function bestHand(cards){
  let best=null;
  for(let a=0;a<cards.length-4;a++)for(let b=a+1;b<cards.length-3;b++)for(let c=b+1;c<cards.length-2;c++)for(let d=c+1;d<cards.length-1;d++)for(let e=d+1;e<cards.length;e++){
    const x=evaluate5([cards[a],cards[b],cards[c],cards[d],cards[e]]);
    if(!best||compareHands(x,best)>0)best=x;
  }
  return best;
}
function compareHands(a,b){
  if(a.cat!==b.cat)return a.cat-b.cat;
  const n=Math.max(a.t.length,b.t.length);
  for(let i=0;i<n;i++){const av=a.t[i]||0,bv=b.t[i]||0;if(av!==bv)return av-bv}
  return 0;
}
function makePots(r){
  // 严格按每位玩家本手牌的累计投入分层建立主池/边池。
  // 关键点：弃牌玩家的筹码仍计入底池金额，但永远没有资格赢取该池。
  const contrib=r.players
    .map(p=>({p,amount:Math.max(0,p.totalBet)}))
    .filter(x=>x.amount>0)
    .sort((a,b)=>a.amount-b.amount);
  if(!contrib.length)return [];

  const levels=[...new Set(contrib.map(x=>x.amount))].sort((a,b)=>a-b);
  const pots=[];
  let prev=0;
  for(const level of levels){
    const participants=contrib.filter(x=>x.amount>=level);
    const amount=(level-prev)*participants.length;
    if(amount>0){
      pots.push({
        amount,
        contributors:participants.map(x=>x.p),
        eligible:participants.filter(x=>x.p.inHand&&!x.p.folded).map(x=>x.p)
      });
    }
    prev=level;
  }
  return pots;
}

function returnUnmatchedExcess(r){
  // 任何玩家的累计投入若高于“其他所有玩家中最高的累计投入”，
  // 那个超出的部分都属于未被任何人匹配的筹码，必须退回。
  // 注意：弃牌玩家的投入也算“已匹配的贡献”，因为它仍然属于底池。
  // 例：20k / 40k -> 40k 玩家退回 20k；20k / 40k / 弃牌40k -> 不退。
  const positive=r.players.filter(p=>p.totalBet>0);
  if(positive.length<2)return;
  for(const p of positive){
    const otherMax=Math.max(0,...positive.filter(x=>x.id!==p.id).map(x=>x.totalBet));
    if(p.totalBet>otherMax){
      // 只有当玩家的超额部分确实无法被其他任何贡献匹配时才返还。
      const excess=p.totalBet-otherMax;
      p.totalBet-=excess;
      p.roundBet=Math.max(0,p.roundBet-excess);
      p.chips+=excess;
    }
  }
  setPot(r);
}
function beginShowdown(r){
  if(!rooms.has(r.code)||r.phase!=='playing')return;
  if(r.showdownStarted)return;
  r.showdownStarted=true;
  clearActionTimer(r);
  r.phase='showdown';
  r.revealAllShowdown=alivePlayers(r).length>0 && alivePlayers(r).every(p=>p.allIn);
  for(const p of r.players){if(p.inHand&&!p.folded){p.status='摊牌';p._showdownVisible=!!r.revealAllShowdown;}}
  r.currentPlayerId=null;
  broadcastState(r);
  notice(r,`牌面已发完，${RESULT_SHOW_DELAY/1000}秒后结算，请查看完整牌面`);
  // 公共牌已经完整发完，立即计算结果并进入展示倒计时，避免全押后等待两次。
  clearTimeout(r.showdownTimer);
  r.showdownTimer=null;
  showdown(r);
}
function showdown(r){
  if(!rooms.has(r.code)||r.phase!=='showdown')return;
  clearTimeout(r.showdownTimer);r.showdownTimer=null;
  const board=r.community;
  const alive=alivePlayers(r);
  const results=[];
  const hands=new Map();
  for(const p of alive)hands.set(p.id,bestHand([...p.holeCards,...board]));
  // 在真正建池前，处理两人局的“无法被对手匹配的超额投入”。
  // 这样 20,000 对 40,000 的 all-in 不会把 40,000 都留在底池。
  returnUnmatchedExcess(r);
  const pots=makePots(r);
  const winnings=new Map(r.players.map(p=>[p.id,0]));
  const winnerIds=new Set();
  for(const pot of pots){
    let best=null,winners=[];
    for(const p of pot.eligible){const h=hands.get(p.id);if(!h)continue;const cmp=!best?1:compareHands(h,best);if(cmp>0){best=h;winners=[p]}else if(cmp===0)winners.push(p)}
    if(winners.length){
      const share=Math.floor(pot.amount/winners.length),rem=pot.amount%winners.length;
      // 平局余数筹码按按钮顺时针方向分配，从按钮左侧第一位获胜者开始。
      const orderedWinners=[...winners].sort((a,b)=>{
        const da=(a.seat-r.dealerSeat+MAX_PLAYERS)%MAX_PLAYERS;
        const db=(b.seat-r.dealerSeat+MAX_PLAYERS)%MAX_PLAYERS;
        return da-db;
      });
      orderedWinners.forEach((p,i)=>{const gain=share+(i<rem?1:0);winnings.set(p.id,winnings.get(p.id)+gain);winnerIds.add(p.id)});
    }
  }
  for(const p of r.players){
    if(!p.inHand)continue;
    const net=winnings.get(p.id)-p.totalBet;
    results.push({id:p.id,nickname:p.nickname,handName:hands.get(p.id)?.name||'弃牌',amount:net});
  }
  r.pendingResults=results;
  r.pendingWinnings=winnings;
  r.pendingPot=r.pot;
  r.winnerIds=[...winnerIds];
  for(const p of r.players)p.status=p.folded?'弃牌':(winnerIds.has(p.id)?'获胜':'摊牌');
  broadcastState(r);
  notice(r,`牌面已完整展示，${RESULT_SHOW_DELAY/1000}秒后结算`);
  clearTimeout(r.resultTimer);
  r.resultDeadline=Date.now()+RESULT_SHOW_DELAY;
  r.resultTimer=setTimeout(()=>finalizeShowdown(r),RESULT_SHOW_DELAY);
}
function finalizeShowdown(r){
  if(!rooms.has(r.code)||r.phase!=='showdown')return;
  clearActionTimer(r);clearTimeout(r.resultTimer);r.resultTimer=null;r.resultDeadline=null;
  const winnings=r.pendingWinnings||new Map();
  for(const p of r.players)p.chips+=(winnings.get(p.id)||0);
  r.phase='handEnd';r.currentPlayerId=null;r.revealAllShowdown=true;setPot(r);
  const results=r.pendingResults||[];
  r.lastResults=results;
  for(const p of r.players)p.status=p.folded?'弃牌':'摊牌';
  const winnerPayload=[...(r.winnerIds||[])].map(id=>({id,amount:winnings.get(id)||0}));
  for(const p of r.players.filter(p=>p.socket))io.to(p.socket).emit('showdown',{results,winners:winnerPayload});
  broadcastState(r);
  setTimeout(()=>nextHand(r),3500);
}
function beginNormalResult(r,results,winnerIds){
  if(!rooms.has(r.code)||r.phase!=='playing')return;
  clearActionTimer(r);
  r.phase='showdown';
  r.revealAllShowdown=false;
  r.currentPlayerId=null;
  r.pendingResults=results;
  r.pendingWinnings=new Map();
  r.winnerIds=[...winnerIds];
  for(const p of r.players){if(p.inHand)p.status=p.id===winnerIds[0]?'获胜':'弃牌';}
  broadcastState(r);
  notice(r,`牌局结束，保留当前牌面约 ${FOLD_RESULT_DELAY/1000} 秒后结算`);
  clearTimeout(r.resultTimer);
  r.resultDeadline=Date.now()+FOLD_RESULT_DELAY;
  r.resultTimer=setTimeout(()=>finalizeNormalResult(r,winnerIds),FOLD_RESULT_DELAY);
}
function finalizeNormalResult(r,winnerIds){
  if(!rooms.has(r.code)||r.phase!=='showdown')return;
  clearTimeout(r.resultTimer);r.resultTimer=null;r.resultDeadline=null;
  const winner=r.players.find(p=>p.id===winnerIds[0]);
  if(!winner)return;
  const pot=totalCommitted(r);winner.chips+=pot;r.pot=0;
  const results=r.pendingResults||[];
  r.lastResults=results;r.phase='handEnd';r.currentPlayerId=null;
  for(const p of r.players)p.status=p.id===winner.id?'获胜':(p.inHand?'弃牌':'旁观');
  for(const p of r.players.filter(p=>p.socket))io.to(p.socket).emit('showdown',{results,winners:[{id:winner.id,amount:pot}]});
  broadcastState(r);r.nextHandAt=Date.now()+3500;schedulePersist();setTimeout(()=>nextHand(r),3500);
}
function finishEarly(r){
  const winner=alivePlayers(r)[0];if(!winner)return;
  const pot=totalCommitted(r);
  const results=r.players.filter(p=>p.inHand).map(p=>({id:p.id,nickname:p.nickname,handName:p.id===winner.id?'未摊牌获胜':'弃牌',amount:p.id===winner.id?pot-p.totalBet:-p.totalBet}));
  beginNormalResult(r,results,[winner.id]);
}
function nextHand(r){
  if(!rooms.has(r.code))return;
  r.nextHandAt=null;
  clearActionTimer(r);clearTimeout(r.resultTimer);r.resultTimer=null;r.revealAllShowdown=false;r.showdownStarted=false;r.players.forEach(p=>p._showdownVisible=false);r.pendingResults=null;r.pendingWinnings=null;r.winnerIds=[];

  // 新玩家可以在一局进行中加入，但只作为旁观者参加当前这手牌。
  // 牌局结束进入下一手时，只要他仍在线且有筹码，就正式加入下一手。
  // 这里不能再依赖 p.inHand，因为中途加入的玩家当前手牌本来就是 inHand=false。
  for(const p of r.players){
    if(p.chips<=0){
      p.inHand=false;
      p.ready=false;
      p.status='淘汰';
    }
  }

  const candidates=r.players.filter(p=>p.chips>0&&!p.offline);
  if(candidates.length<2){
    // 只有一名（或没有）存活玩家时，代表整场桌局结束。
    // 回到大厅时开启一场全新的游戏：所有玩家恢复 20,000 筹码，
    // 盲注恢复 100/200，第一手庄位重新随机。
    clearTimeout(r.blindTimer);r.blindTimer=null;r.nextBlindAt=Date.now()+BLIND_INTERVAL;
    r.hasStartedHand=false;
    r.dealerSeat=null;
    r.smallBlind=START_SB;
    r.bigBlind=START_BB;
    r.phase='lobby';
    r.currentPlayerId=null;
    r.pending.clear();
    r.community=[];
    r.pot=0;
    for(const p of r.players){
      p.chips=START_CHIPS;
      p.inHand=false;
      p.ready=false;
      p.folded=false;
      p.allIn=false;
      p.holeCards=[];
      p.totalBet=0;
      p.roundBet=0;
      p.status=p.offline?'离线':'等待准备';
    }
    broadcastState(r);
    notice(r,'整场游戏结束，已回到大厅；所有玩家筹码已重置为 20,000');
    return;
  }

  // 正常手牌结束后不回大厅：在线且有筹码的玩家自动进入下一手。
  // 因此中途加入的第3~9位玩家会从下一手开始正常参与，并按座位顺序轮流行动。
  for(const p of r.players){
    p.ready=p.chips>0&&!p.offline;
    p.inHand=p.chips>0&&!p.offline;
    if(p.inHand)p.status='等待';
  }

  prepareHand(r);
  broadcastState(r);
}
function joinCommon(socket,nickname,code,isCreate=false){
  const r=rooms.get(code);
  if(!r)throw new Error('房间不存在');
  if(r.phase==='playing' && r.players.length>=MAX_PLAYERS)throw new Error('房间已满');
  if(r.phase==='lobby' && r.players.length>=MAX_PLAYERS)throw new Error('房间已满');
  let seat=-1;for(let i=0;i<MAX_PLAYERS;i++)if(!r.players.some(p=>p.seat===i)){seat=i;break}
  if(seat<0)throw new Error('没有空座位');
  const p={id:socket.id,socket:socket.id,token:crypto.randomBytes(16).toString('hex'),nickname:String(nickname||'玩家').slice(0,16),seat,chips:START_CHIPS,ready:false,isHost:false,offline:false,status:r.phase==='playing'?'旁观（下一局加入）':'等待',inHand:false,folded:false,allIn:false,holeCards:[],totalBet:0,roundBet:0,avatar:avatar()};
  if(!r.hostId)r.hostId=p.id;p.isHost=p.id===r.hostId;
  r.players.push(p);socket.join(code);socket.data.roomCode=code;socket.data.playerId=p.id;
  if(isCreate)startBlindTimer(r);
  return r;
}
io.on('connection',socket=>{
  socket.on('createRoom',({nickname})=>{
    try{
      const code=roomCode();const r={code,hostId:null,players:[],phase:'lobby',smallBlind:START_SB,bigBlind:START_BB,nextBlindAt:Date.now()+BLIND_INTERVAL,blindTimer:null,showdownTimer:null,showdownStarted:false,revealAllShowdown:false,resultTimer:null,actionTimer:null,actionDeadline:null,revealShowdown:false,winnerIds:[],deck:[],community:[],dealerSeat:null,hasStartedHand:false,sbSeat:null,bbSeat:null,currentPlayerId:null,street:'',pot:0,minRaiseTo:START_BB,acted:new Set(),pending:new Set()};
      rooms.set(code,r);const joined=joinCommon(socket,nickname,code,true);const p=joined.players.find(x=>x.id===socket.id);schedulePersist();socket.emit('roomCreated',{roomCode:code,token:p.token,state:toPublicState(joined,socket.id)});
    }catch(e){socket.emit('errorMsg',e.message)}
  });
  socket.on('joinRoom',({roomCode,nickname})=>{
    try{
      const r=joinCommon(socket,nickname,String(roomCode));const p=r.players.find(x=>x.id===socket.id);socket.emit('joinedRoom',{roomCode:r.code,token:p.token,state:toPublicState(r,socket.id)});broadcastState(r);
      notice(r,`${socket.data.playerId===r.hostId?'房主':'新玩家'} 加入了房间`);
    }catch(e){socket.emit('errorMsg',e.message)}
  });
  socket.on('resumeRoom',({roomCode,token})=>{
    try{
      const r=rooms.get(String(roomCode));
      if(!r)throw new Error('房间不存在或已解散');
      const p=r.players.find(x=>x.token===String(token));
      if(!p)throw new Error('没有找到可恢复的玩家座位');
      // 允许在旧连接尚未触发 disconnect 时直接接管座位，避免 Wi-Fi/流量切换造成“重新生成身份”。
      const oldId=p.id;
      p.id=socket.id;p.socket=socket.id;p.offline=false;p.reconnectUntil=0;
      socket.data.roomCode=r.code;socket.data.playerId=p.id;socket.data.playerToken=p.token;
      if(r.currentPlayerId===oldId || r._currentPlayerToken===p.token)r.currentPlayerId=p.id;
      if(r.hostId===oldId || p.isHost)r.hostId=p.id;
      p.isHost=!!p.isHost;
      if(r.acted.has(oldId)){r.acted.delete(oldId);r.acted.add(p.id)}
      if(r.pending.has(oldId)){r.pending.delete(oldId);r.pending.add(p.id)}
      if(r._actedTokens?.has(p.token)){r.acted.add(p.id);r._actedTokens.delete(p.token)}
      if(r._pendingTokens?.has(p.token)){r.pending.add(p.id);r._pendingTokens.delete(p.token)}
      if(r._currentPlayerToken===p.token)r._currentPlayerToken=null;
      socket.join(r.code);
      socket.emit('resumedRoom',{roomCode:r.code,token:p.token,state:toPublicState(r,socket.id)});
      if(r.phase==='playing' && r.currentPlayerId===p.id){startActionTimer(r,p);}
      broadcastState(r);
    }catch(e){socket.emit('resumeFailed',e.message)}
  });
  socket.on('toggleReady',()=>{
    try{const r=rooms.get(socket.data.roomCode),p=r?.players.find(x=>x.id===socket.id);if(!p)throw new Error('你不在房间');if(r.phase!=='lobby')throw new Error('游戏进行中');p.ready=!p.ready;broadcastState(r)}catch(e){socket.emit('errorMsg',e.message)}
  });
  socket.on('startGame',()=>{
    try{const r=rooms.get(socket.data.roomCode),p=r?.players.find(x=>x.id===socket.id);if(!r||!p||!p.isHost)throw new Error('只有房主可以开始');startGame(r)}catch(e){socket.emit('errorMsg',e.message)}
  });
  socket.on('revealCards',()=>{
    try{const r=rooms.get(socket.data.roomCode),p=r?.players.find(x=>x.id===socket.id);if(!r||!p)throw new Error('房间不存在');revealOwnCards(r,p)}catch(e){socket.emit('errorMsg',e.message)}
  });
  socket.on('action',a=>{
    try{const r=rooms.get(socket.data.roomCode),p=r?.players.find(x=>x.id===socket.id);if(!r||!p)throw new Error('房间不存在');doAction(r,p,a)}catch(e){socket.emit('errorMsg',e.message)}
  });
  socket.on('chat',text=>{
    const r=rooms.get(socket.data.roomCode),p=r?.players.find(x=>x.id===socket.id);if(!r||!p)return;
    const clean=String(text||'').replace(/\s+/g,' ').trim().slice(0,100);if(!clean)return;
    io.to(r.code).emit('chat',{nickname:p.nickname,text:clean});
  });
  socket.on('leaveRoom',()=>{
    const r=rooms.get(socket.data.roomCode);if(!r)return;
    const p=r.players.find(x=>x.id===socket.id);if(!p)return;
    if(p.isHost){clearTimeout(r.blindTimer);clearActionTimer(r);clearTimeout(r.resultTimer);clearTimeout(r.showdownTimer);rooms.delete(r.code);scheduleDeleteRoom(r.code);io.to(r.code).emit('roomClosed','房主离开，房间已解散');return}
    r.players=r.players.filter(x=>x.id!==socket.id);broadcastState(r);
  });
  socket.on('disconnect',()=>{
    const code=socket.data.roomCode,r=rooms.get(code);if(!r)return;
    // 如果该 token 已经被新连接接管，旧连接的 disconnect 事件不能把新连接再次标成离线。
    const p=r.players.find(x=>x.token===socket.data.playerToken && x.id===socket.id);
    if(!p)return;
    p.socket=null;p.offline=true;p.status='离线';
    p.reconnectUntil=Date.now()+RECONNECT_GRACE;
    if(p.isHost){
      setTimeout(()=>{const rr=rooms.get(code);if(rr&&rr.players.includes(p)&&p.offline&&p.reconnectUntil<=Date.now()){
        clearTimeout(rr.blindTimer);clearActionTimer(rr);clearTimeout(rr.resultTimer);clearTimeout(rr.showdownTimer);rooms.delete(code);scheduleDeleteRoom(code);io.to(code).emit('roomClosed','房主长时间断线，房间已解散');
      }},RECONNECT_GRACE+100);
    } else {
      setTimeout(()=>{const rr=rooms.get(code);if(rr&&rr.players.includes(p)&&p.offline&&p.reconnectUntil<=Date.now()){rr.players=rr.players.filter(x=>x!==p);broadcastState(rr)}},RECONNECT_GRACE+100);
    }
    broadcastState(r);
  });
});
app.get('/health',(req,res)=>res.json({ok:true,rooms:rooms.size}));
initPersistence().then(()=>server.listen(PORT,()=>console.log(`Poker server listening on ${PORT}`))).catch(e=>{console.error('[REDIS] initialization failed:',e);server.listen(PORT,()=>console.log(`Poker server listening on ${PORT}`))});
