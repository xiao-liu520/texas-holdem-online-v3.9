const GSAP_URL='https://cdn.jsdelivr.net/npm/gsap@3.13.0/+esm';
const PIXI_URL='https://cdn.jsdelivr.net/npm/pixi.js@8.14.0/+esm';

class AudioManager{
  constructor(){
    this.ctx=null;this.master=null;this.buses=new Map();this.chipBuffer=null;this.chipLoading=null;
    this.muted=localStorage.getItem('poker_sound')==='off';
  }
  syncMute(){this.muted=localStorage.getItem('poker_sound')==='off';return this.muted}
  unlock(){if(this.syncMute())return;try{this.ensure();if(this.ctx.state==='suspended')this.ctx.resume()}catch(e){}}
  ensure(){
    if(this.ctx)return;
    this.ctx=new (window.AudioContext||window.webkitAudioContext)();
    this.master=this.ctx.createGain();this.master.gain.value=.62;this.master.connect(this.ctx.destination);
    for(const [name,gain] of [['card',.42],['chip',.52],['action',.34],['result',.55],['ui',.25]]){
      const bus=this.ctx.createGain();bus.gain.value=gain;bus.connect(this.master);this.buses.set(name,bus);
    }
    this.loadChip();
  }
  async loadChip(){
    if(this.chipLoading||this.chipBuffer)return;
    this.chipLoading=fetch('/sounds/chip-clack.wav').then(r=>r.arrayBuffer()).then(b=>this.ctx.decodeAudioData(b)).then(b=>{this.chipBuffer=b}).catch(()=>{});
    await this.chipLoading;
  }
  tone(freq,duration=.08,type='sine',gain=.035,delay=0,bus='ui'){
    if(this.muted)return;this.unlock();if(!this.ctx)return;
    const t=this.ctx.currentTime+delay,o=this.ctx.createOscillator(),g=this.ctx.createGain();
    o.type=type;o.frequency.setValueAtTime(freq,t);g.gain.setValueAtTime(.0001,t);g.gain.exponentialRampToValueAtTime(gain,t+.008);g.gain.exponentialRampToValueAtTime(.0001,t+duration);
    o.connect(g);g.connect(this.buses.get(bus)||this.master);o.start(t);o.stop(t+duration+.02);
  }
  chip(){
    if(this.muted)return;this.unlock();if(!this.ctx)return;
    if(this.chipBuffer){const s=this.ctx.createBufferSource(),g=this.ctx.createGain();s.buffer=this.chipBuffer;g.gain.value=.46;s.connect(g);g.connect(this.buses.get('chip'));s.start();}
    else this.tone(180,.06,'square',.025,0,'chip');
  }
  play(kind){
    if(this.syncMute())return;
    if(kind==='deal'){this.tone(720,.055,'triangle',.024,0,'card');this.tone(960,.045,'triangle',.018,.045,'card');return}
    if(kind==='chip'||kind==='bet'){this.chip();return}
    if(kind==='raise'){this.tone(440,.07,'triangle',.028,0,'chip');this.tone(660,.08,'triangle',.022,.07,'chip');this.chip();return}
    if(kind==='allin'){this.tone(130,.18,'sawtooth',.04,0,'action');this.tone(260,.22,'triangle',.035,.11,'result');this.chip();return}
    if(kind==='fold'){this.tone(150,.08,'square',.018,0,'action');return}
    if(kind==='showdown'){this.tone(392,.12,'triangle',.025,0,'result');this.tone(523,.18,'triangle',.032,.12,'result');return}
    if(kind==='flop'){this.tone(330,.08,'triangle',.022,0,'card');this.tone(440,.12,'triangle',.028,.08,'card');return}
    if(kind==='turn'){this.tone(440,.08,'triangle',.024,0,'card');this.tone(554,.14,'triangle',.03,.08,'card');return}
    if(kind==='river'){this.tone(523,.08,'triangle',.026,0,'card');this.tone(659,.16,'triangle',.034,.08,'card');return}
    if(kind==='win'){this.tone(523,.12,'triangle',.035,0,'result');this.tone(659,.12,'triangle',.035,.12,'result');this.tone(784,.18,'triangle',.04,.24,'result');return}
    if(kind==='royal'){this.tone(523,.14,'triangle',.035,0,'result');this.tone(659,.14,'triangle',.035,.12,'result');this.tone(784,.18,'triangle',.04,.24,'result');this.tone(1046,.32,'sine',.035,.42,'result');return}
    if(kind==='lose'){this.tone(330,.13,'sine',.025,0,'result');this.tone(247,.2,'sine',.022,.13,'result');return}
    if(kind==='tie'){this.tone(440,.11,'triangle',.028,0,'result');this.tone(440,.16,'triangle',.028,.13,'result');return}
    if(kind==='countdown'){this.tone(880,.055,'square',.025,0,'ui');return}
    this.tone(420,.06,'sine',.018,0,'action');this.tone(620,.045,'triangle',.012,.055,'action');
  }
}

const audio=new AudioManager();
let gsap=null,pixi=null,pixiApp=null,pixiBoot=null;
const reduced=window.matchMedia('(prefers-reduced-motion: reduce)');
const visualLedger=new Set();

function motionScale(){return reduced.matches?.35:1}
function pointForPlayer(p){
  const el=p?.id===window.latestState?.meId?document.querySelector('#myPlayerInfo'):document.querySelector('.s'+p?.seat);
  if(!el)return null;const r=el.getBoundingClientRect();return [r.left+r.width/2,r.top+r.height/2];
}
function potPoint(){const el=document.querySelector('#pot');if(!el)return [innerWidth/2,innerHeight/2];const r=el.getBoundingClientRect();return [r.left+r.width/2,r.top+r.height/2]}
function gsapTo(target,vars){if(gsap&&!reduced.matches)return gsap.to(target,vars);Object.assign(target,vars);if(vars.onComplete)setTimeout(vars.onComplete,vars.duration*1000||0)}
function flashSeat(p,kind){
  const el=p?.id===window.latestState?.meId?document.querySelector('#myPlayerInfo'):document.querySelector('.s'+p?.seat);if(!el)return;
  el.classList.add('fx-seat-flash');
  const old=el.style.filter;gsapTo(el,{filter:kind==='fold'?'grayscale(.9) brightness(.7)':'brightness(1.35)',duration:.18,yoyo:true,repeat:1,onComplete:()=>{el.style.filter=old;el.classList.remove('fx-seat-flash')}});
}
function chipMove(p,amount,collect=false){
  const from=collect?potPoint():pointForPlayer(p),to=collect?pointForPlayer(p):potPoint();if(!from||!to)return;
  const count=Math.min(window.matchMedia('(max-width: 800px)').matches?3:6,Math.max(2,Math.ceil(Number(amount||0)/1500)));
  for(let i=0;i<count;i++){
    const el=document.createElement('div');el.className='fx-chip-premium';el.style.left=(from[0]-10+Math.random()*12)+'px';el.style.top=(from[1]-10+Math.random()*12)+'px';document.body.appendChild(el);
    const dx=to[0]-from[0]+(Math.random()*18-9),dy=to[1]-from[1]+(Math.random()*18-9);
    gsapTo(el,{x:dx,y:dy,scale:.78,opacity:0,duration:.42*motionScale(),delay:i*.045,onComplete:()=>el.remove()});
  }
}
function showAction(p,status){
  const el=p?.id===window.latestState?.meId?document.querySelector('#myPlayerInfo'):document.querySelector('.s'+p?.seat);if(!el)return;
  const label=document.createElement('div');label.className='fx-action-label';label.textContent=status;el.appendChild(label);gsapTo(label,{y:-8,opacity:0,duration:.7*motionScale(),onComplete:()=>label.remove()});
}
function stageBanner(text,result=false){
  const old=document.querySelector('.fx-stage-banner');if(old)old.remove();
  const el=document.createElement('div');el.className='fx-stage-banner'+(result?' result':'');el.textContent=text;document.body.appendChild(el);
  setTimeout(()=>el.remove(),result?1500:1050);
}
function stageName(count){return count===3?'翻牌 FLOP':count===4?'转牌 TURN':count===5?'河牌 RIVER':''}
async function bootLibraries(){
  if(pixiBoot)return pixiBoot;
  if(window.matchMedia('(max-width: 800px)').matches){pixiBoot=Promise.resolve();return pixiBoot}
  pixiBoot=Promise.allSettled([import(GSAP_URL),import(PIXI_URL)]).then(async ([g,p])=>{
    gsap=g.status==='fulfilled'?(g.value.gsap||g.value.default||g.value):null;
    pixi=p.status==='fulfilled'?p.value:null;
    if(!pixi||!document.querySelector('#fxLayer'))return;
    const {Application}=pixi;pixiApp=new Application();
    await pixiApp.init({resizeTo:window,backgroundAlpha:0,autoStart:false,antialias:false,powerPreference:'low-power'});
    pixiApp.ticker.maxFPS=reduced.matches?24:45;document.querySelector('#fxLayer').appendChild(pixiApp.canvas);pixiApp.stop();
  }).catch(()=>{});
  return pixiBoot;
}
function particleBurst(color='#f2b233',count=10){
  if(reduced.matches)return;
  if(!pixiApp||!pixi){return}
  const {Graphics}=pixi,root=pixiApp.stage,cx=innerWidth/2,cy=innerHeight/2;
  for(let i=0;i<count;i++){
    const g=new Graphics().circle(0,0,2+Math.random()*3).fill(color);g.x=cx;g.y=cy;root.addChild(g);
    const a=Math.random()*Math.PI*2,d=60+Math.random()*180;
    gsapTo(g,{x:cx+Math.cos(a)*d,y:cy+Math.sin(a)*d,alpha:0,scale:.2,duration:.65,onComplete:()=>g.destroy()});
  }
  pixiApp.start();setTimeout(()=>pixiApp?.stop(),900);
}
function royalFX(){
  const layer=document.createElement('div');layer.className='fx-royal';document.body.appendChild(layer);setTimeout(()=>layer.remove(),1100);audio.play('royal');particleBurst('#f4d27c',22);particleBurst('#6ec8ff',10);
}
function actionSound(status){
  if(status==='全押')audio.play('allin');else if(status==='加注')audio.play('raise');else if(status==='弃牌'||status==='超时弃牌')audio.play('fold');else if(['跟注','过牌'].includes(status))audio.play('action');
}
function handKey(s){return [s.roomCode,s.phase,s.street,(s.community||[]).map(c=>c.rank+c.suit).join('')].join('|')}
function ingest(s,old){
  window.latestState=s;bootLibraries();if(!old)return;
  const key=handKey(s);
  const oldCommunity=(old.community||[]).length,newCommunity=(s.community||[]).length;
  if(newCommunity>oldCommunity){
    const stage=stageName(newCommunity);if(stage){stageBanner(stage);audio.play(newCommunity===3?'flop':newCommunity===4?'turn':'river')}
    for(let i=0;i<newCommunity-oldCommunity;i++){audio.play('deal');setTimeout(()=>{document.querySelectorAll('#community .cardface')[oldCommunity+i]?.classList.add('fx-seat-flash')},i*70)}
  }
  const oldMap=new Map((old.players||[]).map(p=>[p.id,p]));
  for(const p of (s.players||[])){
    const op=oldMap.get(p.id);if(!op)continue;
    if((p.roundBet||0)>(op.roundBet||0)){chipMove(p,(p.roundBet||0)-(op.roundBet||0));audio.play(p.status==='加注'?'raise':'chip')}
    if(p.status!==op.status){showAction(p,p.status);actionSound(p.status);flashSeat(p,p.status)}
  }
  if(s.phase==='showdown'&&old.phase!=='showdown'){audio.play('showdown');}
  if(s.phase==='handEnd'&&old.phase!=='handEnd'){for(const p of s.players||[])if((s.winnerIds||[]).includes(p.id)){flashSeat(p,'win');particleBurst('#f4d27c',14)}}
  const names=[s.me?.handName,...(s.players||[]).map(p=>p.handName)];
  if(names.includes('皇家同花顺')&&!visualLedger.has(key+'|royal')){visualLedger.add(key+'|royal');royalFX()}
}
function showdown(d){
  audio.play('showdown');
  const winners=d?.winners||[],me=window.latestState?.meId;
  const outcome=winners.some(w=>w.id===me)?(winners.length>1?'tie':'win'):'lose';
  audio.play(outcome);
  stageBanner(outcome==='win'?'YOU WIN':outcome==='tie'?'SPLIT POT':'SHOWDOWN',true);
  for(const w of winners){const p=(window.latestState?.players||[]).find(x=>x.id===w.id);if(p){chipMove(p,w.amount,true);flashSeat(p,'win')}}
}
function unlock(){audio.unlock()}
window.premiumFX={audio,unlock,play:(kind)=>audio.play(kind),ingest,showdown};

