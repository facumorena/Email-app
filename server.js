// ═══════════════════════════════════════════════════════════════════════════════
// EMAIL RECOVERY — TODO EN UNO
// Corré: node server.js   →   abrí: http://localhost:3001
// ═══════════════════════════════════════════════════════════════════════════════
require("dotenv").config();
const express    = require("express");
const nodemailer = require("nodemailer");
const Database   = require("better-sqlite3");
const cron       = require("node-cron");
const path       = require("path");

const app  = express();
const PORT = process.env.PORT || 3001;
app.use(express.json());

// ── BASE DE DATOS ─────────────────────────────────────────────────────────────
const db = new Database(path.join(__dirname, "queue.db"));
db.exec(`
  CREATE TABLE IF NOT EXISTS email_queue (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    client_name    TEXT    NOT NULL,
    client_email   TEXT    NOT NULL,
    client_type    TEXT    DEFAULT 'activo',
    client_orders  INTEGER DEFAULT 1,
    client_zona    TEXT    DEFAULT '',
    client_rubro   TEXT    DEFAULT '',
    products       TEXT    DEFAULT '',
    company_nombre TEXT    NOT NULL,
    company_web    TEXT    NOT NULL,
    company_desc   TEXT    DEFAULT '',
    abandoned_at   INTEGER NOT NULL,
    email_1_sent   INTEGER DEFAULT 0,
    email_2_sent   INTEGER DEFAULT 0,
    email_3_sent   INTEGER DEFAULT 0,
    email_1_at     INTEGER,
    email_2_at     INTEGER,
    email_3_at     INTEGER,
    error_log      TEXT    DEFAULT '',
    created_at     INTEGER DEFAULT (strftime('%s','now'))
  )
`);

// ── TEMPLATES (server-side, sin template literals para evitar conflictos) ─────
function buildEmails(emp, web, desc, nombre, rubro, zona, tipo, pedidos, products) {
  const firma = "Equipo " + emp + "\n" + web + " · " + desc;
  const prod  = products ? "\nProductos en carrito:\n" + products + "\n" : "";
  if (tipo === "frecuente") return [
    { subject: nombre + ", dejaste productos en tu carrito de " + emp,
      body: "Estimado/a equipo de " + nombre + ",\n\nNotamos que dejaron productos sin confirmar en " + emp + ". Con " + pedidos + " pedidos realizados, son uno de nuestros clientes más activos en " + zona + " y queremos asegurarnos de que este pedido se concrete.\n" + prod + "\nSu carrito sigue reservado en " + web + ".\n\nSaludos cordiales,\n" + firma },
    { subject: "Seguimiento: pedido pendiente de " + nombre,
      body: "Estimado/a equipo de " + nombre + ",\n\nHan pasado 24 horas desde que reservaron su último pedido. Si necesitan asistencia para coordinar entrega en " + zona + " o ajustar cantidades, estamos disponibles de inmediato.\n" + prod + "\nSaludos cordiales,\n" + firma },
    { subject: "Último aviso para " + nombre + " — carrito por vencer",
      body: "Estimado/a equipo de " + nombre + ",\n\nSu carrito en " + emp + " está a punto de expirar. Como reconocimiento a su fidelidad con " + pedidos + " pedidos, les ofrecemos envío prioritario sin cargo si completan en las próximas 24 horas.\n" + prod + "\nSaludos cordiales,\n" + firma },
  ];
  if (tipo === "potencial") return [
    { subject: nombre + ", sus productos en " + emp + " están esperando",
      body: "Estimado/a equipo de " + nombre + ",\n\nLe escribimos desde " + emp + " para informarle que los productos seleccionados aún están disponibles. Nos especializamos en " + desc + ", ideal para negocios de " + rubro + " en " + zona + ".\n" + prod + "\nSi tienen consultas antes de confirmar, estamos disponibles sin compromiso.\n\nSaludos cordiales,\n" + firma },
    { subject: "¿Podemos ayudarles a completar su pedido, " + nombre + "?",
      body: "Estimado/a equipo de " + nombre + ",\n\nNotamos que aún no finalizaron su pedido. Muchos negocios de " + rubro + " nos consultan sobre cantidades y tiempos de entrega en " + zona + ". Nos adaptamos a lo que necesitan.\n" + prod + "\nSaludos cordiales,\n" + firma },
    { subject: "Último llamado — y un beneficio para " + nombre,
      body: "Estimado/a equipo de " + nombre + ",\n\nSu carrito en " + emp + " está próximo a vencer. Para facilitar este primer pedido, les ofrecemos envío sin cargo si lo completan en las próximas 24 horas.\n" + prod + "\nSaludos cordiales,\n" + firma },
  ];
  return [
    { subject: nombre + ", hay productos en " + emp + " esperándoles",
      body: "Estimado/a equipo de " + nombre + ",\n\nNotamos un pedido sin confirmar en " + emp + ". Como cliente activo del rubro " + rubro + " en " + zona + ", ya conocen nuestros productos. Su carrito sigue reservado en " + web + ".\n" + prod + "\nSaludos cordiales,\n" + firma },
    { subject: "Recordatorio " + emp + " para " + nombre + " — 24 horas",
      body: "Estimado/a equipo de " + nombre + ",\n\nHan pasado 24 horas desde que reservaron su pedido. Si surgió alguna duda sobre productos o logística en " + zona + ", nuestro equipo puede orientarles.\n" + prod + "\nSaludos cordiales,\n" + firma },
    { subject: "Última oportunidad — carrito por vencer en " + emp,
      body: "Estimado/a equipo de " + nombre + ",\n\nSu carrito en " + emp + " está a punto de expirar. Les ofrecemos envío sin cargo si completan en las próximas 24 horas.\n" + prod + "\nSaludos cordiales,\n" + firma },
  ];
}

// ── NODEMAILER ────────────────────────────────────────────────────────────────
const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: { user: process.env.GMAIL_USER, pass: process.env.GMAIL_APP_PASSWORD },
});
async function sendEmail(to, subject, body, fromName) {
  return transporter.sendMail({
    from: '"' + fromName + '" <' + process.env.GMAIL_USER + ">",
    to, subject, text: body,
    html: '<pre style="font-family:Georgia,serif;font-size:15px;line-height:1.9;white-space:pre-wrap;max-width:600px">' + body + "</pre>",
  });
}

// ── SCHEDULER (cada minuto) ───────────────────────────────────────────────────
const DELAYS = { e1: 3600, e2: 86400, e3: 259200 };
async function processQueue() {
  const now = Math.floor(Date.now() / 1000);
  const rows = db.prepare(
    "SELECT * FROM email_queue WHERE (email_1_sent=0 AND ?>=abandoned_at+?) OR (email_2_sent=0 AND ?>=abandoned_at+?) OR (email_3_sent=0 AND ?>=abandoned_at+?)"
  ).all(now, DELAYS.e1, now, DELAYS.e2, now, DELAYS.e3);
  for (const r of rows) {
    const emails = buildEmails(r.company_nombre, r.company_web, r.company_desc, r.client_name, r.client_rubro, r.client_zona, r.client_type, r.client_orders, r.products);
    for (const [i, delay, sentField, atField] of [[0,DELAYS.e1,"email_1_sent","email_1_at"],[1,DELAYS.e2,"email_2_sent","email_2_at"],[2,DELAYS.e3,"email_3_sent","email_3_at"]]) {
      if (!r[sentField] && now >= r.abandoned_at + delay) {
        try {
          await sendEmail(r.client_email, emails[i].subject, emails[i].body, r.company_nombre);
          db.prepare("UPDATE email_queue SET " + sentField + "=1," + atField + "=? WHERE id=?").run(now, r.id);
          console.log("✓ Email " + (i+1) + " → " + r.client_name + " <" + r.client_email + ">");
        } catch(e) {
          db.prepare("UPDATE email_queue SET error_log=? WHERE id=?").run(e.message, r.id);
          console.error("✗ Email " + (i+1) + " error (" + r.client_email + "): " + e.message);
        }
      }
    }
  }
}
cron.schedule("* * * * *", processQueue);

// ── API ───────────────────────────────────────────────────────────────────────
app.post("/api/cart-abandoned", (req, res) => {
  const { client_name, client_email, client_type="activo", client_orders=1,
          client_zona="", client_rubro="", products="",
          company_nombre, company_web, company_desc="" } = req.body;
  if (!client_name || !client_email || !company_nombre || !company_web)
    return res.status(400).json({ error: "Faltan campos: client_name, client_email, company_nombre, company_web" });
  const abandoned_at = Math.floor(Date.now() / 1000);
  const r = db.prepare("INSERT INTO email_queue (client_name,client_email,client_type,client_orders,client_zona,client_rubro,products,company_nombre,company_web,company_desc,abandoned_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)").run(client_name,client_email,client_type,client_orders,client_zona,client_rubro,products,company_nombre,company_web,company_desc,abandoned_at);
  const fmt = (s) => new Date(s*1000).toLocaleString("es-UY",{timeZone:"America/Montevideo",day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"});
  console.log("\n📦 Registrado: " + client_name + " <" + client_email + "> ID:" + r.lastInsertRowid);
  res.json({ ok:true, id:r.lastInsertRowid, schedule:{ email_1:fmt(abandoned_at+DELAYS.e1), email_2:fmt(abandoned_at+DELAYS.e2), email_3:fmt(abandoned_at+DELAYS.e3) }});
});

app.post("/api/cart-completed", (req, res) => {
  const { client_email, company_nombre } = req.body;
  if (!client_email) return res.status(400).json({ error: "Se requiere client_email" });
  const r = company_nombre
    ? db.prepare("UPDATE email_queue SET email_1_sent=1,email_2_sent=1,email_3_sent=1 WHERE client_email=? AND company_nombre=? AND email_3_sent=0").run(client_email, company_nombre)
    : db.prepare("UPDATE email_queue SET email_1_sent=1,email_2_sent=1,email_3_sent=1 WHERE client_email=? AND email_3_sent=0").run(client_email);
  res.json({ ok:true, cancelled:r.changes });
});

app.get("/api/dashboard", (_req, res) => {
  const stats = db.prepare("SELECT COUNT(*) as total, COALESCE(SUM(email_1_sent),0) as e1, COALESCE(SUM(email_2_sent),0) as e2, COALESCE(SUM(email_3_sent),0) as e3 FROM email_queue").get();
  const queue = db.prepare("SELECT * FROM email_queue ORDER BY created_at DESC LIMIT 100").all();
  res.json({ stats, queue });
});

app.get("/api/health", (_req, res) => res.json({ ok:true, gmail: process.env.GMAIL_USER || null }));

// ── FRONTEND (React embebido, servido desde el mismo servidor) ────────────────
const HTML = `<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Email Recovery</title>
<script src="https://unpkg.com/react@18/umd/react.development.js"></script>
<script src="https://unpkg.com/react-dom@18/umd/react-dom.development.js"></script>
<script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
</head>
<body style="margin:0;padding:0">
<div id="root"></div>
<script type="text/babel">
const { useState, useEffect } = React;

// ── DATOS BASE ──────────────────────────────────────────────────────────────
const EMPRESAS_BASE = {
  ecopack: {
    id:"ecopack", nombre:"EcoPack Uruguay", password:"eco2024",
    web:"ecopackuy.com", descripcion:"packaging ecológico para tu negocio",
    colores:{ primario:"#2d6a4f", secundario:"#52b788", fondo:"#f5f0e8", suave:"#d8f3dc", medio:"#e8dcc8" },
    emoji:"🌿",
    clientes:[
      { id:"C01", email:"hereford@hereford.com.uy",    nombre:"Hereford",        rubro:"Cafétería",        zona:"Montevideo",    tipo:"frecuente", pedidos:8  },
      { id:"C02", email:"nomade@nomadecafe.com.uy",    nombre:"Nómade Café",     rubro:"Gastronomía",      zona:"Montevideo",    tipo:"activo",    pedidos:5  },
      { id:"C03", email:"puntonatural@gmail.com",      nombre:"Punto Natural",   rubro:"Eventos",          zona:"Canelones",     tipo:"activo",    pedidos:6  },
      { id:"C04", email:"brasasur@gmail.com",          nombre:"Brasa Sur",       rubro:"Retail alimentos", zona:"San José",    tipo:"potencial", pedidos:4  },
      { id:"C05", email:"aromabakery@gmail.com",       nombre:"Aroma Bakery",    rubro:"Cafétería",        zona:"Punta del Este",tipo:"activo",    pedidos:5  },
      { id:"C06", email:"riohotel@riohotel.com.uy",   nombre:"Río Hotel",      rubro:"Helería",         zona:"Colonia",       tipo:"frecuente", pedidos:10 },
      { id:"C07", email:"mentaviandas@gmail.com",      nombre:"Menta Viandas",   rubro:"Hotel",            zona:"Punta del Este",tipo:"activo",    pedidos:5  },
      { id:"C08", email:"costaeventos@gmail.com",      nombre:"Costa Eventos",   rubro:"Cafétería",        zona:"Montevideo",    tipo:"activo",    pedidos:6  },
      { id:"C09", email:"mercadoorganico@gmail.com",   nombre:"Mercado Orgánico",rubro:"Restaurante",     zona:"Rivera",        tipo:"activo",    pedidos:6  },
      { id:"C10", email:"verdelimon@gmail.com",        nombre:"Verde Limón",    rubro:"Retail alimentos", zona:"Salto",         tipo:"frecuente", pedidos:9  },
      { id:"C11", email:"sanorico@gmail.com",          nombre:"Sano y Rico",     rubro:"Cafétería",        zona:"Montevideo",    tipo:"frecuente", pedidos:10 },
      { id:"C12", email:"puertodeli@gmail.com",        nombre:"Puerto Deli",     rubro:"Eventos",          zona:"Colonia",       tipo:"frecuente", pedidos:11 },
      { id:"C13", email:"olivaresto@gmail.com",        nombre:"Oliva Restó",     rubro:"Gastronomía",      zona:"Punta del Este",tipo:"frecuente", pedidos:8  },
      { id:"C14", email:"zenithotel@zenithotel.com.uy",nombre:"Zenit Hotel",     rubro:"Gastronomía",      zona:"Canelones",     tipo:"frecuente", pedidos:10 },
      { id:"C15", email:"gardencafe@gmail.com",        nombre:"Garden Café",     rubro:"Cafétería",        zona:"Montevideo",    tipo:"frecuente", pedidos:11 },
      { id:"C16", email:"ramblafood@gmail.com",        nombre:"Rambla Food",     rubro:"Roticería",       zona:"Paysandú",     tipo:"frecuente", pedidos:11 },
      { id:"C17", email:"fitkitchen@gmail.com",        nombre:"Fit Kitchen",     rubro:"Retail alimentos", zona:"Rocha",         tipo:"activo",    pedidos:6  },
      { id:"C18", email:"ecoferia@gmail.com",          nombre:"Eco Feria",       rubro:"Retail alimentos", zona:"Punta del Este",tipo:"frecuente", pedidos:11 },
    ],
  },
  demostore: {
    id:"demostore", nombre:"Demo Store", password:null,
    web:"demostore.com", descripcion:"tu tienda online",
    colores:{ primario:"#1e3a5f", secundario:"#3b82f6", fondo:"#f0f4ff", suave:"#dbeafe", medio:"#bfdbfe" },
    emoji:"🛍️",
    clientes:[
      { id:"D01", email:"ana.garcia@gmail.com",    nombre:"Ana García",   rubro:"Particular", zona:"Montevideo", tipo:"frecuente", pedidos:12 },
      { id:"D02", email:"luis.torres@empresa.com", nombre:"Luis Torres",   rubro:"Empresa",    zona:"Canelones",  tipo:"activo",    pedidos:5  },
      { id:"D03", email:"maria.suarez@gmail.com",  nombre:"María Suárez", rubro:"Particular", zona:"Maldonado",  tipo:"potencial", pedidos:2  },
      { id:"D04", email:"pablo.rios@empresa.com",  nombre:"Pablo Ríos",   rubro:"Empresa",    zona:"Colonia",    tipo:"activo",    pedidos:7  },
    ],
  },
};
const PALETAS = [
  { primario:"#2d6a4f", secundario:"#52b788", fondo:"#f5f0e8", suave:"#d8f3dc", medio:"#e8dcc8" },
  { primario:"#1e3a5f", secundario:"#3b82f6", fondo:"#f0f4ff", suave:"#dbeafe", medio:"#bfdbfe" },
  { primario:"#7c3aed", secundario:"#a78bfa", fondo:"#f5f3ff", suave:"#ede9fe", medio:"#ddd6fe" },
  { primario:"#b45309", secundario:"#f59e0b", fondo:"#fffbeb", suave:"#fef3c7", medio:"#fde68a" },
  { primario:"#be123c", secundario:"#f43f5e", fondo:"#fff1f2", suave:"#ffe4e6", medio:"#fecdd3" },
  { primario:"#0f766e", secundario:"#14b8a6", fondo:"#f0fdfa", suave:"#ccfbf1", medio:"#99f6e4" },
];
const EMOJIS = ["🌿","🛍️","🍕","☕","🏪","🎁","🌸","⚡","🏋️","💎","🎨","🚀"];
const TIPO_BADGE = {
  frecuente:{ color:"#166534", bg:"#dcfce7" },
  activo:   { color:"#1e40af", bg:"#dbeafe" },
  potencial:{ color:"#92400e", bg:"#fef3c7" },
};

// ── EMAIL GENERATOR (string concat, sin template literals) ──────────────────
function generarEmails(empresa, cliente, manual) {
  var N = String.fromCharCode(10);
  var NN = N + N;
  const emp = empresa.nombre, web = empresa.web, descripcion = empresa.descripcion;
  const firma = "Equipo " + emp + N + web + " · " + descripcion;
  if (!cliente) {
    const nom = manual.nombre, prods = manual.productos;
    return [
      { subject:"Sus productos en " + emp + " están esperando, " + nom,
        body:"Estimado/a " + nom + "," + NN + "Le escribimos desde " + emp + " para informarle que los productos que seleccionó aún están reservados en su carrito:" + NN + prods + NN + "Puede retomar su pedido en cualquier momento en " + web + "." + NN + "Saludos cordiales," + N + firma },
      { subject:"Recordatorio: su pedido en " + emp + " sigue disponible",
        body:"Estimado/a " + nom + "," + NN + "Han pasado 24 horas y su pedido aún está pendiente:" + NN + prods + NN + "Si tiene alguna consulta, nuestro equipo está disponible." + NN + "Saludos cordiales," + N + firma },
      { subject:"Último aviso: su carrito en " + emp + " está por vencer",
        body:"Estimado/a " + nom + "," + NN + "Su carrito está próximo a expirar:" + NN + prods + NN + "Como gesto de bienvenida, le ofrecemos envío sin cargo si completa su pedido en las próximas 24 horas." + NN + "Saludos cordiales," + N + firma },
    ];
  }
  const { nombre, rubro, zona, tipo, pedidos } = cliente;
  if (tipo === "frecuente") return [
    { subject: nombre + ", dejaste productos en tu carrito de " + emp,
      body: "Estimado/a equipo de " + nombre + "," + NN + "Notamos que dejaron productos sin confirmar en " + emp + ". Con " + pedidos + " pedidos realizados, son uno de nuestros clientes más activos en " + zona + " y queremos asegurarnos de que este pedido se concrete." + NN + "Su carrito sigue reservado en " + web + "." + NN + "Saludos cordiales," + N + firma },
    { subject: "Seguimiento: pedido pendiente de " + nombre,
      body: "Estimado/a equipo de " + nombre + "," + NN + "Han pasado 24 horas desde que reservaron su último pedido. Si necesitan asistencia para coordinar entrega en " + zona + " o ajustar cantidades, estamos disponibles de inmediato." + NN + "Saludos cordiales," + N + firma },
    { subject: "Último aviso para " + nombre + " — carrito por vencer",
      body: "Estimado/a equipo de " + nombre + "," + NN + "Su carrito en " + emp + " está a punto de expirar. Como reconocimiento a su fidelidad con " + pedidos + " pedidos, les ofrecemos envío prioritario sin cargo si completan en las próximas 24 horas." + NN + "Saludos cordiales," + N + firma },
  ];
  if (tipo === "potencial") return [
    { subject: nombre + ", sus productos en " + emp + " están esperando",
      body: "Estimado/a equipo de " + nombre + "," + NN + "Le escribimos desde " + emp + " para informarle que los productos seleccionados aún están disponibles. Nos especializamos en " + descripcion + ", ideal para negocios de " + rubro + " en " + zona + "." + NN + "Si tienen consultas antes de confirmar, estamos disponibles sin compromiso." + NN + "Saludos cordiales," + N + firma },
    { subject: "¿Podemos ayudarles a completar su pedido, " + nombre + "?",
      body: "Estimado/a equipo de " + nombre + "," + NN + "Notamos que aún no finalizaron su pedido. Muchos negocios de " + rubro + " nos consultan sobre cantidades y tiempos de entrega en " + zona + ". Nos adaptamos a lo que necesitan." + NN + "Saludos cordiales," + N + firma },
    { subject: "Último llamado — y un beneficio para " + nombre,
      body: "Estimado/a equipo de " + nombre + "," + NN + "Su carrito en " + emp + " está próximo a vencer. Para facilitar este primer pedido, les ofrecemos envío sin cargo si lo completan en las próximas 24 horas." + NN + "Saludos cordiales," + N + firma },
  ];
  return [
    { subject: nombre + ", hay productos en " + emp + " esperándoles",
      body: "Estimado/a equipo de " + nombre + "," + NN + "Notamos un pedido sin confirmar en " + emp + ". Como cliente activo del rubro " + rubro + " en " + zona + ", ya conocen nuestros productos. Su carrito sigue reservado en " + web + "." + NN + "Saludos cordiales," + N + firma },
    { subject: "Recordatorio " + emp + " para " + nombre + " — 24 horas",
      body: "Estimado/a equipo de " + nombre + "," + NN + "Han pasado 24 horas desde que reservaron su pedido. Si surgió alguna duda sobre productos o logística en " + zona + ", nuestro equipo puede orientarles." + NN + "Saludos cordiales," + N + firma },
    { subject: "Última oportunidad — carrito por vencer en " + emp,
      body: "Estimado/a equipo de " + nombre + "," + NN + "Su carrito en " + emp + " está a punto de expirar. Les ofrecemos envío sin cargo si completan en las próximas 24 horas." + NN + "Saludos cordiales," + N + firma },
  ];
}

// ── LOGIN ─────────────────────────────────────────────────────────────────────
function LoginScreen({ onLogin, empresas, onAgregarEmpresa }) {
  const lista = Object.values(empresas);
  const [selId,   setSelId]   = useState(lista[0]?.id || "");
  const [pass,    setPass]    = useState("");
  const [error,   setError]   = useState("");
  const [modal,   setModal]   = useState(false);
  const [nNombre, setNNombre] = useState("");
  const [nWeb,    setNWeb]    = useState("");
  const [nDesc,   setNDesc]   = useState("");
  const [nPass,   setNPass]   = useState("");
  const [nLibre,  setNLibre]  = useState(false);
  const [nPaleta, setNPaleta] = useState(0);
  const [nEmoji,  setNEmoji]  = useState("🏪");
  const [nErr,    setNErr]    = useState("");
  const empActual = empresas[selId] || lista[0];
  function cambiar(id) { setSelId(id); setPass(""); setError(""); }
  function ingresar() {
    if (!empActual) return;
    if (empActual.password === null) { onLogin(empActual); return; }
    if (pass === empActual.password) { onLogin(empActual); }
    else { setError("Contraseña incorrecta."); }
  }
  function crear() {
    if (!nNombre.trim()) { setNErr("El nombre es obligatorio."); return; }
    if (!nWeb.trim())    { setNErr("El sitio web es obligatorio."); return; }
    if (!nLibre && !nPass.trim()) { setNErr("Ingresá una contraseña o marcá acceso libre."); return; }
    const id = nNombre.toLowerCase().replace(/\\s+/g,"_").replace(/[^a-z0-9_]/g,"") + "_" + Date.now();
    onAgregarEmpresa({ id, nombre:nNombre.trim(), password:nLibre?null:nPass.trim(), web:nWeb.trim(), descripcion:nDesc.trim()||"tienda online", colores:PALETAS[nPaleta], emoji:nEmoji, clientes:[] });
    setSelId(id); setModal(false);
    setNNombre(""); setNWeb(""); setNDesc(""); setNPass(""); setNLibre(false); setNPaleta(0); setNEmoji("🏪"); setNErr("");
  }
  const inp = { width:"100%", padding:"10px 12px", background:"rgba(255,255,255,0.07)", border:"1px solid rgba(255,255,255,0.15)", color:"#fff", fontSize:14, boxSizing:"border-box", fontFamily:"Georgia,serif" };
  return (
    <div style={{ minHeight:"100vh", display:"flex", alignItems:"center", justifyContent:"center", background:"linear-gradient(135deg,#0f2018,#1b3a28,#0f2018)", fontFamily:"Georgia,serif", padding:20 }}>
      <style>{\`
        @keyframes fadeUp{from{opacity:0;transform:translateY(14px)}to{opacity:1;transform:translateY(0)}}
        @keyframes mIn{from{opacity:0;transform:scale(.96)}to{opacity:1;transform:scale(1)}}
        .lcard{animation:fadeUp .4s ease}.ebtn:hover{opacity:.8}
        .lbtn:hover{opacity:.85;transform:translateY(-1px)}.lbtn{transition:all .2s}
        input:focus,select:focus{outline:none;border-color:#52b788!important}button{cursor:pointer}
      \`}</style>
      <div className="lcard" style={{ background:"rgba(255,255,255,0.05)", border:"1px solid rgba(82,183,136,0.2)", backdropFilter:"blur(20px)", padding:"40px 34px", width:"100%", maxWidth:400, boxShadow:"0 24px 80px rgba(0,0,0,.4)" }}>
        <div style={{ textAlign:"center", marginBottom:26 }}>
          <div style={{ fontSize:34, marginBottom:8 }}>📧</div>
          <div style={{ color:"#fff", fontSize:20, fontWeight:"bold" }}>Email Recovery</div>
          <div style={{ color:"rgba(255,255,255,.4)", fontSize:10, letterSpacing:".15em", textTransform:"uppercase", marginTop:4 }}>Plataforma multi-marca</div>
        </div>
        <div style={{ marginBottom:16 }}>
          <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:8 }}>
            <span style={{ fontSize:10, letterSpacing:".18em", textTransform:"uppercase", color:"rgba(255,255,255,.45)" }}>Seleccioná tu empresa</span>
            <button className="ebtn" onClick={()=>setModal(true)} style={{ background:"rgba(255,255,255,.08)", border:"1px solid rgba(255,255,255,.15)", color:"rgba(255,255,255,.6)", fontSize:11, padding:"4px 10px", fontFamily:"Georgia,serif" }}>+ Nueva</button>
          </div>
          <div style={{ display:"flex", flexWrap:"wrap", gap:8 }}>
            {lista.map(e => (
              <button key={e.id} className="ebtn" onClick={()=>cambiar(e.id)} style={{ flex:"1 1 calc(50% - 4px)", minWidth:0, padding:"10px 8px", border:"none", background:selId===e.id?"rgba(82,183,136,.25)":"rgba(255,255,255,.06)", color:selId===e.id?"#52b788":"rgba(255,255,255,.5)", outline:selId===e.id?"1px solid #52b788":"1px solid transparent", fontSize:12, fontFamily:"Georgia,serif", borderRadius:2, transition:"all .2s" }}>
                <div style={{ fontSize:18, marginBottom:3 }}>{e.emoji}</div>
                <div style={{ fontSize:11, fontWeight:selId===e.id?"bold":"normal", overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{e.nombre}</div>
              </button>
            ))}
          </div>
        </div>
        {empActual?.password !== null ? (
          <div style={{ marginBottom:16 }}>
            <div style={{ fontSize:10, letterSpacing:".18em", textTransform:"uppercase", color:"rgba(255,255,255,.45)", marginBottom:6 }}>Contraseña</div>
            <input key={selId} type="password" value={pass} onChange={e=>{setPass(e.target.value);setError("");}} onKeyDown={e=>e.key==="Enter"&&ingresar()} placeholder="••••••••" style={inp} />
          </div>
        ) : (
          <div style={{ marginBottom:16, padding:"11px", background:"rgba(82,183,136,.1)", border:"1px solid rgba(82,183,136,.25)", textAlign:"center" }}>
            <span style={{ color:"#52b788", fontSize:12 }}>✓ Acceso libre — sin contraseña</span>
          </div>
        )}
        {error && <p style={{ color:"#f87171", fontSize:12, margin:"0 0 12px", textAlign:"center" }}>⚠ {error}</p>}
        <button className="lbtn" onClick={ingresar} style={{ width:"100%", padding:"12px", background:"linear-gradient(135deg,#52b788,#2d6a4f)", color:"#fff", border:"none", fontSize:12, letterSpacing:".18em", textTransform:"uppercase", fontFamily:"Georgia,serif" }}>Ingresar →</button>
        <p style={{ color:"rgba(255,255,255,.18)", fontSize:11, textAlign:"center", marginTop:16, marginBottom:0 }}>¿Necesás acceso? Contactá al administrador.</p>
      </div>
      {modal && (
        <div onClick={()=>setModal(false)} style={{ position:"fixed", inset:0, background:"rgba(0,0,0,.7)", display:"flex", alignItems:"center", justifyContent:"center", zIndex:1000, padding:20 }}>
          <div onClick={e=>e.stopPropagation()} style={{ background:"#1a2e22", border:"1px solid rgba(82,183,136,.3)", padding:"30px 26px", width:"100%", maxWidth:440, boxShadow:"0 24px 80px rgba(0,0,0,.5)", animation:"mIn .25s ease", maxHeight:"90vh", overflowY:"auto" }}>
            <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:20 }}>
              <div style={{ color:"#fff", fontSize:15, fontWeight:"bold" }}>+ Nueva empresa</div>
              <button onClick={()=>setModal(false)} style={{ background:"none", border:"none", color:"rgba(255,255,255,.4)", fontSize:18 }}>✕</button>
            </div>
            {[["Nombre *",nNombre,setNNombre,"Ej: La Trattoria"],["Sitio web *",nWeb,setNWeb,"Ej: latrattoria.com"],["Descripción",nDesc,setNDesc,"Ej: restaurante italiano"]].map(([label,val,set,ph])=>(
              <div key={label} style={{ marginBottom:12 }}>
                <div style={{ fontSize:10, letterSpacing:".15em", textTransform:"uppercase", color:"rgba(255,255,255,.45)", marginBottom:5 }}>{label}</div>
                <input value={val} onChange={e=>set(e.target.value)} placeholder={ph} style={inp} />
              </div>
            ))}
            <div style={{ marginBottom:12 }}>
              <div style={{ fontSize:10, letterSpacing:".15em", textTransform:"uppercase", color:"rgba(255,255,255,.45)", marginBottom:5 }}>Contraseña</div>
              <input type="password" value={nPass} onChange={e=>setNPass(e.target.value)} placeholder="••••••••" disabled={nLibre} style={{ ...inp, opacity:nLibre?.4:1 }} />
              <label style={{ display:"flex", alignItems:"center", gap:8, marginTop:7, cursor:"pointer" }}>
                <input type="checkbox" checked={nLibre} onChange={e=>setNLibre(e.target.checked)} style={{ accentColor:"#52b788" }} />
                <span style={{ fontSize:12, color:"rgba(255,255,255,.5)" }}>Acceso libre</span>
              </label>
            </div>
            <div style={{ marginBottom:12 }}>
              <div style={{ fontSize:10, letterSpacing:".15em", textTransform:"uppercase", color:"rgba(255,255,255,.45)", marginBottom:6 }}>Ícono</div>
              <div style={{ display:"flex", flexWrap:"wrap", gap:6 }}>
                {EMOJIS.map(em=>(
                  <button key={em} onClick={()=>setNEmoji(em)} style={{ width:32, height:32, fontSize:17, background:nEmoji===em?"rgba(82,183,136,.25)":"rgba(255,255,255,.06)", border:nEmoji===em?"1px solid #52b788":"1px solid transparent", borderRadius:2 }}>{em}</button>
                ))}
              </div>
            </div>
            <div style={{ marginBottom:18 }}>
              <div style={{ fontSize:10, letterSpacing:".15em", textTransform:"uppercase", color:"rgba(255,255,255,.45)", marginBottom:6 }}>Color de marca</div>
              <div style={{ display:"flex", gap:8 }}>
                {PALETAS.map((p,i)=>(
                  <button key={i} onClick={()=>setNPaleta(i)} style={{ width:26, height:26, background:p.primario, border:nPaleta===i?"2px solid #fff":"2px solid transparent", borderRadius:"50%", boxShadow:nPaleta===i?"0 0 0 2px "+p.secundario:"none" }} />
                ))}
              </div>
            </div>
            {nErr && <p style={{ color:"#f87171", fontSize:12, margin:"0 0 12px" }}>⚠ {nErr}</p>}
            <div style={{ display:"flex", gap:10 }}>
              <button onClick={()=>setModal(false)} style={{ flex:1, padding:"10px", background:"rgba(255,255,255,.07)", border:"1px solid rgba(255,255,255,.15)", color:"rgba(255,255,255,.5)", fontSize:12, letterSpacing:".12em", textTransform:"uppercase", fontFamily:"Georgia,serif" }}>Cancelar</button>
              <button onClick={crear} style={{ flex:2, padding:"10px", background:"linear-gradient(135deg,#52b788,#2d6a4f)", border:"none", color:"#fff", fontSize:12, letterSpacing:".12em", textTransform:"uppercase", fontFamily:"Georgia,serif" }}>Crear empresa →</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ── APP PRINCIPAL ─────────────────────────────────────────────────────────────
function AppPrincipal({ empresa, onLogout, onActualizarClientes }) {
  const C = empresa.colores;
  const [vista,   setVista]   = useState("generar");
  const [modo,    setModo]    = useState("db");
  const [busq,    setBusq]    = useState("");
  const [cliSel,  setCliSel]  = useState(null);
  const [mNom,    setMNom]    = useState("");
  const [mEmail,  setMEmail]  = useState("");
  const [mProds,  setMProds]  = useState("");
  const [mErr,    setMErr]    = useState("");
  const [emails,  setEmails]  = useState(null);
  const [tab,     setTab]     = useState(0);
  const [copied,  setCopied]  = useState(false);
  const [progOk,  setProgOk]  = useState("");
  const [progErr, setProgErr] = useState("");
  const [progLoad,setProgLoad]= useState(false);
  const [cola,    setCola]    = useState(null);
  const [colaLoad,setColaLoad]= useState(false);
  const [dbTab,   setDbTab]   = useState("csv");
  const [csvTxt,  setCsvTxt]  = useState("");
  const [csvErr,  setCsvErr]  = useState("");
  const [csvPrev, setCsvPrev] = useState([]);
  const [cNom,    setCNom]    = useState("");
  const [cRub,    setCRub]    = useState("");
  const [cZon,    setCZon]    = useState("");
  const [cTip,    setCTip]    = useState("activo");
  const [cPed,    setCPed]    = useState("");
  const [cEmail,  setCEmail]  = useState("");
  const [cErr,    setCErr]    = useState("");
  const [cOk,     setCOk]     = useState("");

  const TABS = [
    { label:"Email 1", timing:"1 hora después",  icon:"📬" },
    { label:"Email 2", timing:"24 hs después",   icon:"🔔" },
    { label:"Email 3", timing:"72 hs después",   icon:"⏳" },
  ];

  // Cargar cola cuando cambia a esa vista
  useEffect(() => {
    if (vista !== "cola") return;
    cargarCola();
    const t = setInterval(cargarCola, 15000);
    return () => clearInterval(t);
  }, [vista]);

  function cargarCola() {
    setColaLoad(true);
    fetch("/api/dashboard").then(r=>r.json()).then(d=>{ setCola(d); setColaLoad(false); }).catch(()=>setColaLoad(false));
  }

  const filtrados = empresa.clientes.filter(c =>
    c.nombre.toLowerCase().includes(busq.toLowerCase()) ||
    c.rubro.toLowerCase().includes(busq.toLowerCase()) ||
    c.zona.toLowerCase().includes(busq.toLowerCase())
  );
  const stats = {
    total:       empresa.clientes.length,
    frecuentes:  empresa.clientes.filter(c=>c.tipo==="frecuente").length,
    activos:     empresa.clientes.filter(c=>c.tipo==="activo").length,
    potenciales: empresa.clientes.filter(c=>c.tipo==="potencial").length,
  };

  function selCliente(c) { setCliSel(c); setEmails(generarEmails(empresa,c,null)); setTab(0); setCopied(false); setProgOk(""); setProgErr(""); setVista("generar"); }
  function genManual() {
    if (!mNom.trim()||!mProds.trim()) { setMErr("Completá nombre y productos."); return; }
    setMErr(""); setEmails(generarEmails(empresa,null,{nombre:mNom.trim(),productos:mProds.trim()})); setTab(0); setCopied(false); setProgOk(""); setProgErr("");
  }
  function copiar() {
    if (!emails) return;
    navigator.clipboard.writeText("Asunto: "+emails[tab].subject+"\n\n"+emails[tab].body);
    setCopied(true); setTimeout(()=>setCopied(false),2000);
  }

  // ── PROGRAMAR ENVÍO AUTOMÁTICO ──────────────────────────────────────────────
  async function programar() {
    setProgErr(""); setProgOk("");
    const clientEmail = modo==="db" ? cliSel?.email : mEmail;
    if (!clientEmail) { setProgErr("Este cliente no tiene email cargado."); return; }
    const payload = modo==="db" && cliSel ? {
      client_name:    cliSel.nombre,
      client_email:   cliSel.email,
      client_type:    cliSel.tipo,
      client_orders:  cliSel.pedidos,
      client_zona:    cliSel.zona,
      client_rubro:   cliSel.rubro,
      products:       "",
      company_nombre: empresa.nombre,
      company_web:    empresa.web,
      company_desc:   empresa.descripcion,
    } : {
      client_name:    mNom.trim(),
      client_email:   mEmail.trim(),
      products:       mProds.trim(),
      company_nombre: empresa.nombre,
      company_web:    empresa.web,
      company_desc:   empresa.descripcion,
    };
    setProgLoad(true);
    try {
      const res  = await fetch("/api/cart-abandoned", { method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(payload) });
      const data = await res.json();
      setProgLoad(false);
      if (data.ok) {
        setProgOk("✓ Programado — Email 1: " + data.schedule.email_1 + " | Email 2: " + data.schedule.email_2 + " | Email 3: " + data.schedule.email_3);
        setTimeout(()=>setProgOk(""),8000);
      } else { setProgErr(data.error || "Error al programar"); }
    } catch(e) { setProgLoad(false); setProgErr("No se pudo conectar con el servidor"); }
  }

  function parsearCSV(txt) {
    const lineas = txt.trim().split("\n").filter(l=>l.trim());
    if (!lineas.length) return [];
    const primera = lineas[0].toLowerCase();
    const datos = (primera.includes("nombre")||primera.includes("rubro")) ? lineas.slice(1) : lineas;
    return datos.map((l,i)=>{
      const c = l.split(/[,;|\\t]/).map(x=>x.trim().replace(/^"|"$/g,""));
      const ped = parseInt(c[4])||1;
      return { id:"IMP"+Date.now()+i, nombre:c[0]||"Cliente "+(i+1), rubro:c[1]||"Sin rubro", zona:c[2]||"Sin zona", tipo:c[3]||(ped>=8?"frecuente":ped>=5?"activo":"potencial"), pedidos:ped, email:c[5]||"" };
    });
  }
  function prevCSV()   { setCsvErr(""); if(!csvTxt.trim()){setCsvErr("Pegá el CSV primero.");return;} const p=parsearCSV(csvTxt); if(!p.length){setCsvErr("Sin datos válidos.");return;} setCsvPrev(p); }
  function importCSV() { if(!csvPrev.length)return; onActualizarClientes(empresa.id,[...empresa.clientes,...csvPrev]); setCsvPrev([]); setCsvTxt(""); setCsvErr(""); }
  function addManual() {
    if(!cNom.trim()){setCErr("Nombre obligatorio.");return;} setCErr("");
    const n={id:"MAN"+Date.now(),nombre:cNom.trim(),rubro:cRub.trim()||"Sin rubro",zona:cZon.trim()||"Sin zona",tipo:cTip,pedidos:parseInt(cPed)||1,email:cEmail.trim()||""};
    onActualizarClientes(empresa.id,[...empresa.clientes,n]);
    setCNom(""); setCRub(""); setCZon(""); setCTip("activo"); setCPed(""); setCEmail("");
    setCOk("✓ "+n.nombre+" agregado."); setTimeout(()=>setCOk(""),3000);
  }
  function delCliente(id) { onActualizarClientes(empresa.id, empresa.clientes.filter(c=>c.id!==id)); }

  const inputStyle = { width:"100%", padding:"10px 12px", border:"1px solid "+C.medio, background:C.fondo, fontSize:14, color:"#111", boxSizing:"border-box", fontFamily:"Georgia,serif" };

  // Formatear timestamp de cola
  function fmtTs(ts) {
    if (!ts) return "—";
    return new Date(ts*1000).toLocaleString("es-UY",{day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"});
  }

  return (
    <div style={{ minHeight:"100vh", background:C.fondo, fontFamily:"Georgia,serif" }}>
      <style>{\`
        @keyframes fadeIn{from{opacity:0;transform:translateY(5px)}to{opacity:1;transform:translateY(0)}}
        .rc:hover{background:\${C.suave}!important;cursor:pointer}
        input:focus,textarea:focus{outline:none;border-color:\${C.secundario}!important}
        ::-webkit-scrollbar{width:4px}::-webkit-scrollbar-thumb{background:\${C.medio}}
        button{cursor:pointer}
      \`}</style>

      {/* HEADER */}
      <div style={{ background:C.primario, padding:"0 20px", display:"flex", alignItems:"stretch", height:52, gap:4 }}>
        <div style={{ display:"flex", alignItems:"center", gap:10, marginRight:22 }}>
          <div style={{ width:30, height:30, borderRadius:"50%", background:C.secundario, display:"flex", alignItems:"center", justifyContent:"center", fontSize:15 }}>{empresa.emoji}</div>
          <div>
            <div style={{ color:"#fff", fontWeight:"bold", fontSize:13 }}>{empresa.nombre}</div>
            <div style={{ color:"rgba(255,255,255,.45)", fontSize:9, letterSpacing:".12em", textTransform:"uppercase" }}>{empresa.web}</div>
          </div>
        </div>
        {[{k:"generar",label:"✉️ Emails"},{k:"clientes",label:"👥 Clientes"},{k:"basedatos",label:"📂 Base de datos"},{k:"cola",label:"📊 Cola de envío"}].map(({k,label})=>(
          <button key={k} onClick={()=>setVista(k)} style={{ background:"none", border:"none", color:vista===k?"#fff":"rgba(255,255,255,.5)", fontSize:12, letterSpacing:".07em", padding:"0 13px", borderBottom:vista===k?"2px solid "+C.secundario:"2px solid transparent", fontFamily:"Georgia,serif", transition:"all .2s" }}>{label}</button>
        ))}
        <div style={{ flex:1 }} />
        <button onClick={onLogout} style={{ background:"none", border:"none", color:"rgba(255,255,255,.35)", fontSize:11, letterSpacing:".1em", fontFamily:"Georgia,serif" }}>Salir →</button>
      </div>

      <div style={{ maxWidth:780, margin:"0 auto", padding:"24px 20px" }}>

        {/* ── VISTA CLIENTES ── */}
        {vista==="clientes" && (
          <div style={{ animation:"fadeIn .3s" }}>
            <div style={{ display:"grid", gridTemplateColumns:"repeat(4,1fr)", gap:10, marginBottom:18 }}>
              {[{label:"Total",val:stats.total,color:C.primario},{label:"Frecuentes",val:stats.frecuentes,color:"#166534"},{label:"Activos",val:stats.activos,color:"#1e40af"},{label:"Potenciales",val:stats.potenciales,color:"#92400e"}].map(s=>(
                <div key={s.label} style={{ background:"#fff", border:"1px solid "+C.medio, padding:"12px 14px" }}>
                  <div style={{ fontSize:24, fontWeight:"bold", color:s.color }}>{s.val}</div>
                  <div style={{ fontSize:10, color:"#6b7280", marginTop:2, textTransform:"uppercase", letterSpacing:".08em" }}>{s.label}</div>
                </div>
              ))}
            </div>
            <input value={busq} onChange={e=>setBusq(e.target.value)} placeholder="Buscar..." style={{ ...inputStyle, marginBottom:8, background:"#fff" }} />
            <div style={{ background:"#fff", border:"1px solid "+C.medio }}>
              <div style={{ display:"grid", gridTemplateColumns:"2fr 1.2fr 1fr 90px", padding:"9px 14px", background:C.primario, color:"#fff", fontSize:10, letterSpacing:".13em", textTransform:"uppercase" }}>
                <span>Cliente</span><span>Rubro</span><span>Zona</span><span>Perfil</span>
              </div>
              {filtrados.map((c,i)=>{ const b=TIPO_BADGE[c.tipo]||TIPO_BADGE.potencial; return (
                <div key={c.id} className="rc" onClick={()=>selCliente(c)} style={{ display:"grid", gridTemplateColumns:"2fr 1.2fr 1fr 90px", padding:"11px 14px", borderTop:i>0?"1px solid "+C.fondo:"none", transition:"background .15s" }}>
                  <div><div style={{ fontSize:14, color:"#111" }}>{c.nombre}</div><div style={{ fontSize:11, color:"#9ca3af", marginTop:1 }}>{c.pedidos} pedidos</div></div>
                  <div style={{ fontSize:13, color:"#4b5563", display:"flex", alignItems:"center" }}>{c.rubro}</div>
                  <div style={{ fontSize:13, color:"#4b5563", display:"flex", alignItems:"center" }}>{c.zona}</div>
                  <div style={{ display:"flex", alignItems:"center" }}><span style={{ background:b.bg, color:b.color, fontSize:9, padding:"3px 7px", fontWeight:"bold", textTransform:"uppercase" }}>{c.tipo}</span></div>
                </div>
              );})}
            </div>
          </div>
        )}

        {/* ── VISTA BASE DE DATOS ── */}
        {vista==="basedatos" && (
          <div style={{ animation:"fadeIn .3s" }}>
            <div style={{ display:"flex", border:"1px solid "+C.medio, marginBottom:20 }}>
              {[{k:"csv",label:"📋 Pegar CSV",desc:"Desde Excel o Google Sheets"},{k:"manual",label:"+ Agregar uno",desc:"Ingreso individual"},{k:"lista",label:"📄 Ver lista",desc:empresa.clientes.length+" clientes"}].map(({k,label,desc},i)=>(
                <div key={k} onClick={()=>setDbTab(k)} style={{ flex:1, padding:"12px 14px", cursor:"pointer", background:dbTab===k?C.primario:"#fff", color:dbTab===k?"#fff":C.primario, borderRight:i<2?"1px solid "+C.medio:"none", transition:"background .2s" }}>
                  <div style={{ fontWeight:"bold", fontSize:13 }}>{label}</div>
                  <div style={{ fontSize:11, opacity:.7, marginTop:2 }}>{desc}</div>
                </div>
              ))}
            </div>
            {dbTab==="csv" && (
              <div style={{ background:"#fff", border:"1px solid "+C.medio, padding:24 }}>
                <p style={{ fontSize:13, color:"#4b5563", margin:"0 0 12px", lineHeight:1.6 }}>
                  Copiá y pegá el contenido de tu planilla. Columnas:<br/>
                  <code style={{ background:"#f5f5f5", padding:"2px 6px", fontSize:12 }}>Nombre, Rubro, Zona, Tipo (opcional), Pedidos (opcional), Email (opcional)</code>
                </p>
                <textarea value={csvTxt} onChange={e=>{setCsvTxt(e.target.value);setCsvPrev([]);}} placeholder={"Hereford,Cafétería,Montevideo,frecuente,8,hereford@hereford.com.uy\nNómade Café,Gastronomía,Montevideo,activo,5,nomade@cafe.com"} rows={7}
                  style={{ width:"100%", padding:"10px 12px", border:"1px solid "+C.medio, background:"#fafafa", fontSize:13, color:"#111", resize:"vertical", boxSizing:"border-box", fontFamily:"monospace" }} />
                {csvErr && <p style={{ color:"#b91c1c", fontSize:12, margin:"8px 0 0" }}>⚠ {csvErr}</p>}
                {csvPrev.length>0 && (
                  <div style={{ marginTop:14 }}>
                    <div style={{ fontSize:12, color:C.primario, fontWeight:"bold", marginBottom:8 }}>Vista previa — {csvPrev.length} clientes:</div>
                    <div style={{ border:"1px solid "+C.medio, maxHeight:200, overflowY:"auto" }}>
                      <div style={{ display:"grid", gridTemplateColumns:"2fr 1.2fr 1fr 80px", padding:"7px 12px", background:C.primario, color:"#fff", fontSize:10, letterSpacing:".12em", textTransform:"uppercase" }}>
                        <span>Nombre</span><span>Rubro</span><span>Zona</span><span>Tipo</span>
                      </div>
                      {csvPrev.map((c,i)=>{ const b=TIPO_BADGE[c.tipo]||TIPO_BADGE.potencial; return (
                        <div key={i} style={{ display:"grid", gridTemplateColumns:"2fr 1.2fr 1fr 80px", padding:"8px 12px", fontSize:13, borderTop:"1px solid "+C.fondo, color:"#374151" }}>
                          <span>{c.nombre}</span><span>{c.rubro}</span><span>{c.zona}</span>
                          <span style={{ fontSize:10, fontWeight:"bold", color:b.color }}>{c.tipo.toUpperCase()}</span>
                        </div>
                      );})}
                    </div>
                    <button onClick={importCSV} style={{ marginTop:12, background:C.primario, color:"#fff", border:"none", padding:"10px 24px", fontSize:12, letterSpacing:".15em", textTransform:"uppercase" }}>
                      ✓ Importar {csvPrev.length} clientes
                    </button>
                  </div>
                )}
                {csvPrev.length===0 && <button onClick={prevCSV} style={{ marginTop:12, background:C.primario, color:"#fff", border:"none", padding:"10px 24px", fontSize:12, letterSpacing:".15em", textTransform:"uppercase" }}>Vista previa →</button>}
              </div>
            )}
            {dbTab==="manual" && (
              <div style={{ background:"#fff", border:"1px solid "+C.medio, padding:24 }}>
                <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:12, marginBottom:12 }}>
                  {[["Nombre *",cNom,setCNom,"Ej: Café del Sur"],["Rubro",cRub,setCRub,"Ej: Cafétería"],["Zona",cZon,setCZon,"Ej: Montevideo"],["Pedidos históricos",cPed,setCPed,"Ej: 5"],["Email del cliente",cEmail,setCEmail,"Ej: contacto@empresa.com"]].map(([lbl,val,set,ph])=>(
                    <div key={lbl}>
                      <div style={{ fontSize:10, letterSpacing:".15em", textTransform:"uppercase", color:C.primario, marginBottom:5, fontWeight:"bold" }}>{lbl}</div>
                      <input value={val} onChange={e=>set(e.target.value)} placeholder={ph} style={inputStyle} />
                    </div>
                  ))}
                </div>
                <div style={{ marginBottom:16 }}>
                  <div style={{ fontSize:10, letterSpacing:".15em", textTransform:"uppercase", color:C.primario, marginBottom:6, fontWeight:"bold" }}>Perfil</div>
                  <div style={{ display:"flex", gap:8 }}>
                    {["frecuente","activo","potencial"].map(t=>{ const b=TIPO_BADGE[t]; return (
                      <button key={t} onClick={()=>setCTip(t)} style={{ flex:1, padding:"9px", border:"none", fontSize:12, fontWeight:"bold", textTransform:"uppercase", background:cTip===t?b.bg:"#f5f5f5", color:cTip===t?b.color:"#9ca3af", outline:cTip===t?"2px solid "+b.color:"none" }}>{t}</button>
                    );})}
                  </div>
                </div>
                {cErr && <p style={{ color:"#b91c1c", fontSize:12, margin:"0 0 10px" }}>⚠ {cErr}</p>}
                {cOk  && <p style={{ color:"#166534", fontSize:12, margin:"0 0 10px" }}>{cOk}</p>}
                <button onClick={addManual} style={{ background:C.primario, color:"#fff", border:"none", padding:"10px 26px", fontSize:12, letterSpacing:".15em", textTransform:"uppercase" }}>Agregar cliente</button>
              </div>
            )}
            {dbTab==="lista" && (
              empresa.clientes.length===0
                ? <div style={{ background:"#fff", border:"1px solid "+C.medio, padding:40, textAlign:"center", color:"#9ca3af", fontSize:14 }}>Aún no hay clientes.</div>
                : <div style={{ background:"#fff", border:"1px solid "+C.medio }}>
                    <div style={{ display:"grid", gridTemplateColumns:"2fr 1.2fr 1fr 80px 36px", padding:"9px 14px", background:C.primario, color:"#fff", fontSize:10, letterSpacing:".13em", textTransform:"uppercase" }}>
                      <span>Nombre</span><span>Rubro</span><span>Zona</span><span>Perfil</span><span></span>
                    </div>
                    <div style={{ maxHeight:420, overflowY:"auto" }}>
                      {empresa.clientes.map((c,i)=>{ const b=TIPO_BADGE[c.tipo]||TIPO_BADGE.potencial; return (
                        <div key={c.id} style={{ display:"grid", gridTemplateColumns:"2fr 1.2fr 1fr 80px 36px", padding:"10px 14px", borderTop:i>0?"1px solid "+C.fondo:"none", alignItems:"center" }}>
                          <div><div style={{ fontSize:14, color:"#111" }}>{c.nombre}</div><div style={{ fontSize:11, color:"#9ca3af", marginTop:1 }}>{c.pedidos} pedidos</div></div>
                          <div style={{ fontSize:13, color:"#4b5563" }}>{c.rubro}</div>
                          <div style={{ fontSize:13, color:"#4b5563" }}>{c.zona}</div>
                          <div><span style={{ background:b.bg, color:b.color, fontSize:9, padding:"3px 7px", fontWeight:"bold", textTransform:"uppercase" }}>{c.tipo}</span></div>
                          <button onClick={()=>delCliente(c.id)} style={{ background:"none", border:"none", color:"#d1d5db", fontSize:16, padding:0 }} title="Eliminar">✕</button>
                        </div>
                      );})}
                    </div>
                    <div style={{ padding:"10px 14px", borderTop:"1px solid "+C.medio, fontSize:12, color:"#6b7280" }}>{empresa.clientes.length} clientes en total</div>
                  </div>
            )}
          </div>
        )}

        {/* ── VISTA COLA ── */}
        {vista==="cola" && (
          <div style={{ animation:"fadeIn .3s" }}>
            <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:16 }}>
              <div style={{ fontSize:16, fontWeight:"bold", color:C.primario }}>Cola de envío automático</div>
              <button onClick={cargarCola} style={{ background:C.primario, color:"#fff", border:"none", padding:"7px 16px", fontSize:11, letterSpacing:".12em", textTransform:"uppercase" }}>{colaLoad?"… Cargando":"Actualizar"}</button>
            </div>
            {cola && (
              <>
                <div style={{ display:"grid", gridTemplateColumns:"repeat(4,1fr)", gap:10, marginBottom:18 }}>
                  {[{label:"Registrados",val:cola.stats.total,color:C.primario},{label:"Email 1 enviados",val:cola.stats.e1||0,color:"#166534"},{label:"Email 2 enviados",val:cola.stats.e2||0,color:"#1e40af"},{label:"Email 3 enviados",val:cola.stats.e3||0,color:"#92400e"}].map(s=>(
                    <div key={s.label} style={{ background:"#fff", border:"1px solid "+C.medio, padding:"12px 14px" }}>
                      <div style={{ fontSize:24, fontWeight:"bold", color:s.color }}>{s.val}</div>
                      <div style={{ fontSize:10, color:"#6b7280", marginTop:2 }}>{s.label}</div>
                    </div>
                  ))}
                </div>
                {cola.queue.length===0
                  ? <div style={{ background:"#fff", border:"1px solid "+C.medio, padding:40, textAlign:"center", color:"#9ca3af" }}>Sin carritos registrados aún.</div>
                  : <div style={{ background:"#fff", border:"1px solid "+C.medio }}>
                      <div style={{ display:"grid", gridTemplateColumns:"2fr 1.4fr 60px 60px 60px", padding:"9px 14px", background:C.primario, color:"#fff", fontSize:10, letterSpacing:".13em", textTransform:"uppercase" }}>
                        <span>Cliente</span><span>Empresa</span><span>E1</span><span>E2</span><span>E3</span>
                      </div>
                      <div style={{ maxHeight:480, overflowY:"auto" }}>
                        {cola.queue.map((r,i)=>(
                          <div key={r.id} style={{ display:"grid", gridTemplateColumns:"2fr 1.4fr 60px 60px 60px", padding:"10px 14px", borderTop:i>0?"1px solid "+C.fondo:"none", alignItems:"center" }}>
                            <div>
                              <div style={{ fontSize:13, color:"#111", fontWeight:"bold" }}>{r.client_name}</div>
                              <div style={{ fontSize:11, color:"#9ca3af", marginTop:1 }}>{r.client_email}</div>
                              {r.error_log ? <div style={{ fontSize:10, color:"#b91c1c", marginTop:2 }}>⚠ {r.error_log.slice(0,60)}</div> : null}
                            </div>
                            <div style={{ fontSize:12, color:"#4b5563" }}>{r.company_nombre}</div>
                            {[r.email_1_sent, r.email_2_sent, r.email_3_sent].map((sent,j)=>(
                              <div key={j} style={{ textAlign:"center", fontSize:15 }}>{sent ? "✅" : "⏳"}</div>
                            ))}
                          </div>
                        ))}
                      </div>
                    </div>
                }
              </>
            )}
            {!cola && !colaLoad && <div style={{ background:"#fff", border:"1px solid "+C.medio, padding:40, textAlign:"center", color:"#9ca3af" }}>Cargando...</div>}
            <div style={{ marginTop:16, padding:"12px 16px", background:"rgba(0,0,0,.03)", border:"1px solid "+C.medio, fontSize:12, color:"#6b7280", lineHeight:1.7 }}>
              <strong style={{ color:C.primario }}>Cómo funciona:</strong> Cuando un cliente abandona el carrito, lo registrás con el botón "🤖 Programar envío automático" desde la vista Emails. El servidor enva los emails automáticamente a la 1 hora, 24 horas y 72 horas. Los ✅ muestran los emails ya enviados, los ⏳ los pendientes.
            </div>
          </div>
        )}

        {/* ── VISTA GENERAR ── */}
        {vista==="generar" && (
          <div style={{ animation:"fadeIn .3s" }}>
            <div style={{ display:"flex", border:"1px solid "+C.medio, marginBottom:18 }}>
              {[{k:"db",label:"👥 Desde base de clientes"},{k:"manual",label:"✏️ Ingreso manual"}].map(({k,label},i)=>(
                <div key={k} onClick={()=>{setModo(k);setEmails(null);setCliSel(null);setProgOk("");setProgErr("");}} style={{ flex:1, padding:"12px 16px", cursor:"pointer", background:modo===k?C.primario:"#fff", color:modo===k?"#fff":C.primario, borderRight:i===0?"1px solid "+C.medio:"none", fontSize:13, fontWeight:"bold", transition:"background .2s" }}>{label}</div>
              ))}
            </div>

            {modo==="db" && (
              <div style={{ marginBottom:18 }}>
                <input value={busq} onChange={e=>setBusq(e.target.value)} placeholder="Buscar cliente..." style={{ ...inputStyle, marginBottom:8, background:"#fff" }} />
                <div style={{ background:"#fff", border:"1px solid "+C.medio, maxHeight:260, overflowY:"auto" }}>
                  <div style={{ display:"grid", gridTemplateColumns:"2fr 1.2fr 1fr 90px", padding:"8px 14px", background:C.primario, color:"#fff", fontSize:10, letterSpacing:".13em", textTransform:"uppercase", position:"sticky", top:0 }}>
                    <span>Cliente</span><span>Rubro</span><span>Zona</span><span>Perfil</span>
                  </div>
                  {filtrados.map((c,i)=>{ const b=TIPO_BADGE[c.tipo]||TIPO_BADGE.potencial; const sel=cliSel?.id===c.id; return (
                    <div key={c.id} className="rc" onClick={()=>selCliente(c)} style={{ display:"grid", gridTemplateColumns:"2fr 1.2fr 1fr 90px", padding:"10px 14px", borderTop:i>0?"1px solid "+C.fondo:"none", background:sel?C.suave:"#fff", borderLeft:sel?"3px solid "+C.primario:"3px solid transparent", transition:"background .15s" }}>
                      <div><div style={{ fontSize:14, color:"#111", fontWeight:sel?"bold":"normal" }}>{c.nombre}</div><div style={{ fontSize:11, color:"#9ca3af", marginTop:1 }}>{c.pedidos} pedidos</div></div>
                      <div style={{ fontSize:13, color:"#4b5563", display:"flex", alignItems:"center" }}>{c.rubro}</div>
                      <div style={{ fontSize:13, color:"#4b5563", display:"flex", alignItems:"center" }}>{c.zona}</div>
                      <div style={{ display:"flex", alignItems:"center" }}><span style={{ background:b.bg, color:b.color, fontSize:9, padding:"3px 7px", fontWeight:"bold", textTransform:"uppercase" }}>{c.tipo}</span></div>
                    </div>
                  );})}
                  {filtrados.length===0 && <div style={{ padding:20, textAlign:"center", color:"#9ca3af", fontSize:13 }}>Sin resultados.</div>}
                </div>
              </div>
            )}

            {modo==="manual" && (
              <div style={{ background:"#fff", border:"1px solid "+C.medio, padding:22, marginBottom:18 }}>
                <div style={{ marginBottom:13 }}>
                  <div style={{ fontSize:10, letterSpacing:".15em", textTransform:"uppercase", color:C.primario, marginBottom:6, fontWeight:"bold" }}>Nombre del cliente</div>
                  <input value={mNom} onChange={e=>setMNom(e.target.value)} placeholder="Ej: Café del Centro" style={inputStyle} />
                </div>
                <div style={{ marginBottom:13 }}>
                  <div style={{ fontSize:10, letterSpacing:".15em", textTransform:"uppercase", color:C.primario, marginBottom:6, fontWeight:"bold" }}>Email del cliente (para envío automático)</div>
                  <input type="email" value={mEmail} onChange={e=>setMEmail(e.target.value)} placeholder="Ej: contacto@cafedelcentro.com" style={inputStyle} />
                </div>
                <div style={{ marginBottom:16 }}>
                  <div style={{ fontSize:10, letterSpacing:".15em", textTransform:"uppercase", color:C.primario, marginBottom:6, fontWeight:"bold" }}>Productos en el carrito</div>
                  <textarea value={mProds} onChange={e=>setMProds(e.target.value)} rows={3} placeholder={"Ej: 500x Bowl compostable 750ml\n100x Caja kraft chica"} style={{ ...inputStyle, resize:"vertical" }} />
                </div>
                {mErr && <p style={{ color:"#b91c1c", fontSize:12, margin:"0 0 12px" }}>⚠ {mErr}</p>}
                <button onClick={genManual} style={{ background:C.primario, color:"#fff", border:"none", padding:"10px 24px", fontSize:12, letterSpacing:".15em", textTransform:"uppercase" }}>Generar emails</button>
              </div>
            )}

            {/* EMAILS GENERADOS */}
            {emails && (
              <div style={{ animation:"fadeIn .3s" }}>
                <div style={{ display:"flex" }}>
                  {TABS.map((t,i)=>(
                    <div key={i} onClick={()=>{setTab(i);setCopied(false);}} style={{ flex:1, padding:"10px 8px", textAlign:"center", cursor:"pointer", background:tab===i?C.primario:C.medio, color:tab===i?"#fff":C.primario, borderRight:i<2?"1px solid "+(tab===i?C.secundario:C.medio):"none", fontSize:12, transition:"background .2s" }}>
                      <div style={{ fontSize:17 }}>{t.icon}</div>
                      <div style={{ fontWeight:"bold", marginTop:2 }}>{t.label}</div>
                      <div style={{ fontSize:10, opacity:.7, marginTop:1 }}>{t.timing}</div>
                    </div>
                  ))}
                </div>
                <div style={{ background:"#fff", border:"1px solid "+C.medio, borderTop:"none", padding:"22px 24px", animation:"fadeIn .2s" }}>
                  <div style={{ marginBottom:14, paddingBottom:12, borderBottom:"1px solid "+C.fondo }}>
                    <div style={{ fontSize:10, letterSpacing:".18em", textTransform:"uppercase", color:C.secundario, marginBottom:4 }}>Asunto</div>
                    <div style={{ fontSize:16, color:"#111", fontWeight:"bold" }}>{emails[tab].subject}</div>
                  </div>
                  <pre style={{ fontSize:14, color:"#374151", lineHeight:1.85, whiteSpace:"pre-wrap", fontFamily:"Georgia,serif", margin:0 }}>{emails[tab].body}</pre>

                  {/* BOTONES */}
                  <div style={{ marginTop:20, borderTop:"1px solid "+C.fondo, paddingTop:16 }}>

                    {/* Fila 1: copiar + Gmail */}
                    <div style={{ display:"flex", gap:8, flexWrap:"wrap", marginBottom:12 }}>
                      <button onClick={copiar} style={{ background:"transparent", border:"1px solid "+C.medio, color:C.primario, padding:"8px 16px", fontSize:11, letterSpacing:".12em", textTransform:"uppercase" }}>
                        {copied?"✓ Copiado":"Copiar email"}
                      </button>
                      {(cliSel?.email || mEmail) && (
                        <button onClick={()=>{
                          const e = emails[tab];
                          const to   = encodeURIComponent(cliSel?.email || mEmail);
                          const subj = encodeURIComponent(e.subject);
                          const body = encodeURIComponent(e.body);
                          window.open("https://mail.google.com/mail/?view=cm&to="+to+"&su="+subj+"&body="+body,"_blank");
                        }} style={{ background:"#ea4335", color:"#fff", border:"none", padding:"8px 16px", fontSize:11, letterSpacing:".12em", textTransform:"uppercase" }}>
                          ✉️ Abrir en Gmail
                        </button>
                      )}
                      {modo==="db" && !cliSel?.email && (
                        <span style={{ fontSize:11, color:"#9ca3af", display:"flex", alignItems:"center" }}>Sin email cargado</span>
                      )}
                    </div>

                    {/* Fila 2: PROGRAMAR ENVÍO AUTOMÁTICO */}
                    <div style={{ background:C.suave, border:"1px solid "+C.medio, padding:"14px 16px" }}>
                      <div style={{ fontSize:11, letterSpacing:".15em", textTransform:"uppercase", color:C.primario, fontWeight:"bold", marginBottom:8 }}>🤖 Programar envío automático</div>
                      <div style={{ fontSize:12, color:"#4b5563", marginBottom:12, lineHeight:1.6 }}>
                        El servidor enviará los 3 emails automáticamente:<br/>
                        <strong>Email 1</strong> en 1 hora · <strong>Email 2</strong> en 24 horas · <strong>Email 3</strong> en 72 horas
                      </div>
                      <button onClick={programar} disabled={progLoad} style={{ background:C.primario, color:"#fff", border:"none", padding:"10px 22px", fontSize:12, letterSpacing:".15em", textTransform:"uppercase", opacity:progLoad?.6:1 }}>
                        {progLoad?"… Programando":"Programar envío →"}
                      </button>
                      {progOk  && <div style={{ marginTop:10, padding:"8px 12px", background:"#dcfce7", color:"#166534", fontSize:12, lineHeight:1.5 }}>{progOk}</div>}
                      {progErr && <div style={{ marginTop:10, padding:"8px 12px", background:"#fee2e2", color:"#b91c1c", fontSize:12 }}>⚠ {progErr}</div>}
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

      </div>
    </div>
  );
}

// ── ROOT ──────────────────────────────────────────────────────────────────────
function App() {
  const [empresa,  setEmpresa]  = useState(null);
  const [empresas, setEmpresas] = useState(EMPRESAS_BASE);
  function agregarEmpresa(nueva) { setEmpresas(prev=>({...prev,[nueva.id]:nueva})); }
  function actualizarClientes(empId, nuevos) {
    setEmpresas(prev=>({...prev,[empId]:{...prev[empId],clientes:nuevos}}));
    if (empresa?.id===empId) setEmpresa(prev=>({...prev,clientes:nuevos}));
  }
  return empresa
    ? <AppPrincipal empresa={empresa} onLogout={()=>setEmpresa(null)} onActualizarClientes={actualizarClientes} />
    : <LoginScreen onLogin={setEmpresa} empresas={empresas} onAgregarEmpresa={agregarEmpresa} />;
}

ReactDOM.createRoot(document.getElementById("root")).render(<App/>);
</script>
</body>
</html>`;

app.get("/", (_req, res) => res.send(HTML));

// ── INICIO ────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`
╔════════════════════════════════════════════════╗
║   EMAIL RECOVERY — TODO EN UNO                    ║
╚════════════════════════════════════════════════╝
  App:    http://localhost:${PORT}
  Gmail:  ${process.env.GMAIL_USER || "⚠  Configurá GMAIL_USER en .env"}

  Emails automáticos: 1h · 24h · 72h después del abandono
  Scheduler corriendo cada 60 segundos...
`);
  processQueue(); // revisar al arranque
});

