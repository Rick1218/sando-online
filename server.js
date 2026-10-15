const http = require('http');
const fs = require('fs');
const path = require('path');
const {WebSocketServer} = require('ws');
const PORT = Number(process.env.PORT || 3000);
const html = fs.readFileSync(path.join(__dirname,'index.html'));
const rooms = new Map();
const server = http.createServer((req,res)=>{
  if(req.url === '/health'){res.writeHead(200);res.end('OK');return;}
  res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'});
  res.end(html);
});
const wss = new WebSocketServer({server,path:'/ws',maxPayload:512*1024});
function send(ws,msg){if(ws&&ws.readyState===1)ws.send(JSON.stringify(msg));}
function peers(room){return room.seats.filter(s=>s.ws&&s.ws.readyState===1).length;}
function broadcast(room,msg,except){for(const seat of room.seats)if(seat.ws!==except)send(seat.ws,msg);}
wss.on('connection',ws=>{
 ws.isAlive=true;
 ws.on('pong',()=>{ws.isAlive=true;});
 ws.on('message',raw=>{
  let msg;try{msg=JSON.parse(raw)}catch{return;}
  if(msg.type==='join'){
   const code=String(msg.room||'').toUpperCase();
   if(!/^[A-Z0-9]{4,12}$/.test(code)){send(ws,{type:'error',message:'部屋番号が無効です'});return;}
   let room=rooms.get(code);
   if(!room){room={seats:[{token:null,ws:null},{token:null,ws:null}],state:null};rooms.set(code,room);}
   const token=String(msg.token||'');
   let role=room.seats.findIndex(s=>s.token&&s.token===token);
   if(role<0)role=room.seats.findIndex(s=>!s.token);
   if(role<0){send(ws,{type:'error',message:'この部屋は満員です'});return;}
   const seat=room.seats[role];
   if(seat.ws&&seat.ws!==ws){seat.ws.close();}
   seat.token=seat.token||require('crypto').randomBytes(18).toString('hex');
   seat.ws=ws;ws.room=code;ws.role=role;
   send(ws,{type:'welcome',role,peers:peers(room),state:room.state,token:seat.token});
   broadcast(room,{type:'peers',peers:peers(room)},ws);
  }else if(msg.type==='state'&&ws.room){
   const room=rooms.get(ws.room);
   if(!room||peers(room)!==2||room.seats[ws.role]?.ws!==ws)return;
   const state=msg.state;
   if(!state||typeof state!=='object'||!Array.isArray(state.players)||state.players.length!==2||!Array.isArray(state.board)||state.board.length!==27)return;
   if(room.state&&room.state.turn!==ws.role)return;
   if(!room.state&&ws.role!==0)return;
   room.state=state;
   broadcast(room,{type:'state',state},ws);
  }
 });
 ws.on('close',()=>{
  if(!ws.room)return;
  const room=rooms.get(ws.room);if(!room)return;
  if(room.seats[ws.role]?.ws===ws)room.seats[ws.role].ws=null;
  broadcast(room,{type:'peers',peers:peers(room)});
 });
});
setInterval(()=>{
 for(const ws of wss.clients){
  if(ws.isAlive===false){ws.terminate();continue;}
  ws.isAlive=false;ws.ping();
 }
},25000).unref();
server.listen(PORT,'0.0.0.0',()=>console.log(`三道の切札: http://localhost:${PORT}`));
