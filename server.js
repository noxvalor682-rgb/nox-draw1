const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');
const path = require('path');
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  transports: ['websocket', 'polling'],
  maxHttpBufferSize: 6e6,
  pingInterval: 25000,
  pingTimeout: 20000,
  cors: { origin: '*' }
});
app.use(express.static(path.join(__dirname, 'public'), { maxAge: '1h' }));
app.get('/health', (_req, res) => res.json({ ok: true, app: 'NOX DRAW', rooms: rooms.size }));
const rooms = new Map();
const MAX_PEOPLE = 8;
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function makeCode() {
  let code;
  do { code = Array.from({length:5}, () => CODE_CHARS[crypto.randomInt(CODE_CHARS.length)]).join(''); }
  while (rooms.has(code));
  return code;
}
function cleanName(s) { return String(s || 'نقاش').replace(/[<>]/g,'').trim().slice(0,18) || 'نقاش'; }
function memberList(room) { return [...room.members.values()].map(m => ({id:m.id,name:m.name})); }
function emitMembers(code) { const r=rooms.get(code); if(r) io.to(code).emit('room:members', memberList(r)); }
function join(socket, code, name) {
  const r=rooms.get(code);
  if(!r) return socket.emit('room:error',{message:'این اتاق پیدا نشد'});
  if(r.members.size>=MAX_PEOPLE) return socket.emit('room:error',{message:'ظرفیت اتاق پر شده (حداکثر ۸ نفر)'});
  if(socket.data.room) leave(socket);
  socket.join(code); socket.data.room=code; socket.data.name=cleanName(name);
  r.members.set(socket.id,{id:socket.id,name:socket.data.name});
  socket.emit('room:joined',{code,id:socket.id,members:memberList(r),board:{strokes:r.strokes,background:r.background}});
  socket.to(code).emit('voice:ready',{id:socket.id});
  emitMembers(code);
}
function leave(socket) {
  const code=socket.data.room; if(!code)return;
  const r=rooms.get(code);
  socket.to(code).emit('voice:peer-left',{id:socket.id});
  socket.leave(code); socket.data.room=null;
  if(r){r.members.delete(socket.id);emitMembers(code);if(r.members.size===0)rooms.delete(code);}
}
io.on('connection', socket => {
  socket.on('room:create', ({name}={}) => {
    const code=makeCode(); const r={members:new Map(),strokes:[],background:null,createdAt:Date.now()};
    rooms.set(code,r); socket.join(code); socket.data.room=code; socket.data.name=cleanName(name);
    r.members.set(socket.id,{id:socket.id,name:socket.data.name});
    socket.emit('room:created',{code,id:socket.id,members:memberList(r),board:{strokes:[],background:null}});
    emitMembers(code);
  });
  socket.on('room:join', ({name,code}={}) => join(socket,String(code||'').toUpperCase().slice(0,6),name));
  socket.on('room:leave',()=>leave(socket));
  socket.on('draw:points',m=>{
    const code=socket.data.room,r=rooms.get(code); if(!r||!m||m.room!==code||!Array.isArray(m.points))return;
    const pts=m.points.slice(0,120); const msg={strokeId:String(m.strokeId||'').slice(0,80),points:pts,user:socket.id,tool:m.tool,color:m.color,size:m.size,alpha:m.alpha};
    socket.to(code).volatile.emit('draw:points',msg);
  });
  socket.on('draw:finish',m=>{
    const code=socket.data.room,r=rooms.get(code); if(!r||!m?.item)return;
    const item=m.item; if(!Array.isArray(item.points)||item.points.length>100000)return;
    const safe={...item,user:socket.id}; r.strokes=r.strokes.filter(x=>x.id!==safe.id);r.strokes.push(safe);
    if(r.strokes.length>12000)r.strokes.splice(0,r.strokes.length-12000);
    socket.to(code).emit('draw:finish',{item:safe});
  });
  socket.on('draw:text',m=>{
    const code=socket.data.room,r=rooms.get(code);if(!r||!m?.item)return;
    const x={...m.item,user:socket.id,text:String(m.item.text||'').slice(0,160)};r.strokes.push(x);socket.to(code).emit('draw:text',{item:x});
  });
  socket.on('draw:undo',m=>{
    const code=socket.data.room,r=rooms.get(code);if(!r||!m?.id)return;
    r.strokes=r.strokes.filter(x=>x.id!==m.id);io.to(code).emit('draw:undo',{id:m.id});
  });
  socket.on('draw:clear',()=>{
    const code=socket.data.room,r=rooms.get(code);if(!r)return;r.strokes=[];io.to(code).emit('draw:clear');
  });
  socket.on('board:background',m=>{
    const code=socket.data.room,r=rooms.get(code);if(!r||typeof m?.data!=='string'||m.data.length>5_600_000)return;
    r.background=m.data;io.to(code).emit('board:background',{data:r.background});
  });
  socket.on('chat:send',m=>{
    const code=socket.data.room;if(!rooms.has(code)||typeof m?.text!=='string')return;
    const text=m.text.trim().slice(0,300);if(!text)return;
    io.to(code).emit('chat:message',{name:socket.data.name||'نقاش',text,at:Date.now()});
  });
  socket.on('voice:ready',()=>{const code=socket.data.room;if(code)socket.to(code).emit('voice:ready',{id:socket.id})});
  socket.on('voice:peer',m=>{
    const code=socket.data.room;if(!code||m?.room!==code||!m.to)return;
    io.to(m.to).emit('voice:peer',{id:socket.id});
  });
  socket.on('voice:signal',m=>{
    const code=socket.data.room;if(!code||m?.room!==code||!m.to||!m.data)return;
    io.to(m.to).emit('voice:signal',{from:socket.id,data:m.data});
  });
  socket.on('voice:leave',()=>{const code=socket.data.room;if(code)socket.to(code).emit('voice:peer-left',{id:socket.id})});
  socket.on('disconnect',()=>leave(socket));
});
const PORT=process.env.PORT||3000;
server.listen(PORT,'0.0.0.0',()=>console.log(`NOX DRAW listening on ${PORT}`));
