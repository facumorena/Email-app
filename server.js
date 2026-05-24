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


// ── FRONTEND (archivo estático) ───────────────────────────────────────────────
app.use(express.static(path.join(__dirname)));
app.get("/", (_req, res) => res.sendFile(path.join(__dirname, "index.html")));

// ── INICIO ────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log([
    "",
    "╔════════════════════════════════════════════════╗",
    "║   EMAIL RECOVERY — TODO EN UNO                 ║",
    "╚════════════════════════════════════════════════╝",
    "  App:    http://localhost:" + PORT,
    "  Gmail:  " + (process.env.GMAIL_USER || "⚠  Configurá GMAIL_USER en .env"),
    "",
    "  Emails automáticos: 1h · 24h · 72h después del abandono",
    "  Scheduler corriendo cada 60 segundos...",
    ""
  ].join("\n"));
  processQueue(); // revisar al arranque
});
