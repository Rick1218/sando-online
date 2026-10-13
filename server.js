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
function send(ws,msg){if(ws.readyState===1)ws.send(JSON.stringify(msg));}
function broadcast(room,msg,except){for(const peer of room.clients)if(peer!==except)send(peer,msg);}
wss.on('connection',ws=>{
  ws.on('message',raw=>{
    let msg;try{msg=JSON.parse(raw)}catch{return;}
    if(msg.type==='join'){
      const code=String(msg.room||'').toUpperCase();
      if(!/^[A-Z0-9]{4,12}$/.test(code)){send(ws,{type:'error',message:'部屋番号が無効です'});return;}
      let room=rooms.get(code);
      if(!room){room={clients:[],state:null};rooms.set(code,room);}
      if(room.clients.length>=2){send(ws,{type:'error',message:'この部屋は満員です'});return;}
      room.clients.push(ws);ws.room=code;ws.role=room.clients.length-1;
      send(ws,{type:'welcome',role:ws.role,peers:room.clients.length,state:room.state});
      broadcast(room,{type:'peers',peers:room.clients.length},ws);
    }else if(msg.type==='state'&&ws.room){
      const room=rooms.get(ws.room);
      if(!room||room.clients.length!==2||!room.clients.includes(ws))return;
      const state=msg.state;
      if(!state||typeof state!=='object'||!Array.isArray(state.players)||state.players.length!==2||!Array.isArray(state.board)||state.board.length!==27)return;
      // 送信時の手番は相手に移ることがあるので、直前の状態の操作権限で検証。
      if(room.state&&room.state.turn!==ws.role)return;
      if(!room.state&&ws.role!==0)return;
      room.state=state;
      broadcast(room,{type:'state',state},ws);
    }
  });
  ws.on('close',()=>{
    if(!ws.room)return;
    const room=rooms.get(ws.room);if(!room)return;
    room.clients=room.clients.filter(p=>p!==ws);
    // 役割が入れ替わると操作権限が壊れるため、退出時は部屋を閉じる
    for(const peer of room.clients){send(peer,{type:'error',message:'相手が退出しました。新しい部屋を作成してください'});peer.close();}
    rooms.delete(ws.room);
  });
});
server.listen(PORT,'0.0.0.0',()=>console.log(`三道の切札: http://localhost:${PORT}`));
