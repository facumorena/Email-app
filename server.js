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
const DB_PATH = path.join(__dirname, "queue.db");
const db = new Database(DB_PATH);
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
    created_at     INTEGER DEFAULT (strftime('%s','now')),
    order_value    REAL    DEFAULT 0,
    recovered_at   INTEGER,
    recovered      INTEGER DEFAULT 0
  )
`);
// Migración segura para bases de datos existentes
["order_value REAL DEFAULT 0","recovered_at INTEGER","recovered INTEGER DEFAULT 0"].forEach(col => {
  try { db.exec("ALTER TABLE email_queue ADD COLUMN " + col); } catch(e) {}
});

// ── TEMPLATES ────────────────────────────────────────────────────────────────
function buildEmails(emp, web, desc, nombre, rubro, zona, tipo, pedidos, products) {
  const firma = "Equipo " + emp + "\n" + web + " · " + desc;
  const prod  = products ? "\nProductos en carrito:\n" + products + "\n" : "";
  if (tipo === "frecuente") return [
    { subject: nombre + ", dejaste productos en tu carrito de " + emp,
      body: "Estimado/a equipo de " + nombre + ",\n\nNotamos que dejaron productos sin confirmar en " + emp + ". Con " + pedidos + " pedidos realizados, son uno de nuestros clientes más activos en " + zona + ".\n" + prod + "\nSu carrito sigue reservado en " + web + ".\n\nSaludos cordiales,\n" + firma },
    { subject: "Seguimiento: pedido pendiente de " + nombre,
      body: "Estimado/a equipo de " + nombre + ",\n\nHan pasado 24 horas desde que reservaron su último pedido. Si necesitan asistencia para coordinar entrega en " + zona + " o ajustar cantidades, estamos disponibles.\n" + prod + "\nSaludos cordiales,\n" + firma },
    { subject: "Último aviso para " + nombre + " — carrito por vencer",
      body: "Estimado/a equipo de " + nombre + ",\n\nSu carrito en " + emp + " está a punto de expirar. Como reconocimiento a su fidelidad con " + pedidos + " pedidos, les ofrecemos envío prioritario sin cargo si completan en las próximas 24 horas.\n" + prod + "\nSaludos cordiales,\n" + firma },
  ];
  if (tipo === "potencial") return [
    { subject: nombre + ", sus productos en " + emp + " están esperando",
      body: "Estimado/a equipo de " + nombre + ",\n\nLe escribimos desde " + emp + " para informarle que los productos seleccionados aún están disponibles. Nos especializamos en " + desc + ", ideal para negocios de " + rubro + " en " + zona + ".\n" + prod + "\nSi tienen consultas antes de confirmar, estamos disponibles sin compromiso.\n\nSaludos cordiales,\n" + firma },
    { subject: "Podemos ayudarles a completar su pedido, " + nombre,
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
    "SELECT * FROM email_queue WHERE recovered=0 AND ((email_1_sent=0 AND ?>=abandoned_at+?) OR (email_2_sent=0 AND ?>=abandoned_at+?) OR (email_3_sent=0 AND ?>=abandoned_at+?))"
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
          console.error("✗ Error (" + r.client_email + "): " + e.message);
        }
      }
    }
  }
}
cron.schedule("* * * * *", processQueue);

// ── NORMALIZAR PAYLOAD (Shopify, TiendaNube, genérico) ───────────────────────
function normalizePayload(body, headers) {
  // ── Shopify ──────────────────────────────────────────────────────────────
  // Shopify envía X-Shopify-Shop-Domain en el header
  const shopDomain = headers["x-shopify-shop-domain"] || "";
  if (shopDomain || body.line_items) {
    const customer   = body.customer || {};
    const firstName  = customer.first_name || body.billing_address?.first_name || "";
    const lastName   = customer.last_name  || body.billing_address?.last_name  || "";
    const name       = (firstName + " " + lastName).trim() || "Cliente Shopify";
    const email      = body.email || customer.email || "";
    const city       = body.shipping_address?.city || body.billing_address?.city || "";
    const products   = (body.line_items || [])
      .map(i => i.title + (i.variant_title ? " - " + i.variant_title : "") + " x" + i.quantity)
      .join(", ");
    const orderValue = parseFloat(body.total_price || body.subtotal_price || 0);
    return {
      client_name:    name,
      client_email:   email,
      client_type:    "activo",
      client_orders:  customer.orders_count || 1,
      client_zona:    city,
      client_rubro:   "",
      products,
      order_value:    orderValue,
      company_nombre: process.env.COMPANY_NOMBRE || shopDomain || "Mi Tienda",
      company_web:    process.env.COMPANY_WEB    || shopDomain || "",
      company_desc:   process.env.COMPANY_DESC   || "tienda online",
    };
  }
  // ── TiendaNube ──────────────────────────────────────────────────────────
  // TiendaNube envía "contact" y "products"
  if (body.contact && body.store_id) {
    const contact  = body.contact || {};
    const items    = (body.cart?.items || []).map(i => i.name + " x" + i.quantity).join(", ");
    return {
      client_name:    contact.name  || "Cliente TiendaNube",
      client_email:   contact.email || "",
      client_type:    "activo",
      client_orders:  1,
      client_zona:    contact.city || "",
      client_rubro:   "",
      products:       items,
      order_value:    parseFloat(body.cart?.prices?.total || 0),
      company_nombre: process.env.COMPANY_NOMBRE || "Mi Tienda",
      company_web:    process.env.COMPANY_WEB    || "",
      company_desc:   process.env.COMPANY_DESC   || "tienda online",
    };
  }
  // ── Formato nativo Recartify (ya tiene todos los campos) ─────────────────
  return body;
}

// ── API ───────────────────────────────────────────────────────────────────────
app.post("/api/cart-abandoned", (req, res) => {
  const normalized = normalizePayload(req.body, req.headers);
  const { client_name, client_email, client_type="activo", client_orders=1,
          client_zona="", client_rubro="", products="", order_value=0,
          company_nombre, company_web, company_desc="" } = normalized;
  if (!client_name || !client_email || !company_nombre || !company_web)
    return res.status(400).json({ error: "Faltan campos: client_name, client_email, company_nombre, company_web" });
  const abandoned_at = Math.floor(Date.now() / 1000);
  const r = db.prepare("INSERT INTO email_queue (client_name,client_email,client_type,client_orders,client_zona,client_rubro,products,order_value,company_nombre,company_web,company_desc,abandoned_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)").run(client_name,client_email,client_type,client_orders,client_zona,client_rubro,products,order_value,company_nombre,company_web,company_desc,abandoned_at);
  const fmt = (s) => new Date(s*1000).toLocaleString("es-UY",{timeZone:"America/Montevideo",day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"});
  console.log("\n📦 Registrado: " + client_name + " <" + client_email + "> $" + order_value);
  res.json({ ok:true, id:r.lastInsertRowid, schedule:{ email_1:fmt(abandoned_at+DELAYS.e1), email_2:fmt(abandoned_at+DELAYS.e2), email_3:fmt(abandoned_at+DELAYS.e3) }});
});

// Marcar carrito como completado (venta recuperada)
app.post("/api/cart-completed", (req, res) => {
  const { client_email, company_nombre, order_value=0 } = req.body;
  if (!client_email) return res.status(400).json({ error: "Se requiere client_email" });
  const now = Math.floor(Date.now() / 1000);
  // Solo marcar como recuperado si se envió al menos un email (es decir, vino por nuestro medio)
  const condition = company_nombre
    ? "client_email=? AND company_nombre=? AND recovered=0 AND (email_1_sent=1 OR email_2_sent=1 OR email_3_sent=1)"
    : "client_email=? AND recovered=0 AND (email_1_sent=1 OR email_2_sent=1 OR email_3_sent=1)";
  const params = company_nombre ? [now, order_value, client_email, company_nombre] : [now, order_value, client_email];
  const r = db.prepare("UPDATE email_queue SET recovered=1, recovered_at=?, order_value=?, email_1_sent=1,email_2_sent=1,email_3_sent=1 WHERE " + condition).run(...params);
  console.log("💰 Recuperado: " + client_email + " $" + order_value + " (" + r.changes + " registro/s)");
  res.json({ ok:true, recovered: r.changes, order_value });
});

app.get("/api/dashboard", (_req, res) => {
  const stats = db.prepare(`
    SELECT
      COUNT(*) as total,
      COALESCE(SUM(email_1_sent),0) as e1,
      COALESCE(SUM(email_2_sent),0) as e2,
      COALESCE(SUM(email_3_sent),0) as e3,
      COALESCE(SUM(CASE WHEN recovered=1 THEN 1 ELSE 0 END),0) as recovered,
      COALESCE(SUM(CASE WHEN recovered=1 THEN order_value ELSE 0 END),0) as revenue_recovered,
      COALESCE(SUM(order_value),0) as revenue_total
    FROM email_queue
  `).get();

  // Ventas recuperadas por mes (últimos 6 meses)
  const byMonth = db.prepare(`
    SELECT
      strftime('%Y-%m', datetime(recovered_at, 'unixepoch')) as mes,
      COUNT(*) as ventas,
      SUM(order_value) as monto
    FROM email_queue
    WHERE recovered=1 AND recovered_at IS NOT NULL
    GROUP BY mes ORDER BY mes DESC LIMIT 6
  `).all();

  const queue = db.prepare("SELECT * FROM email_queue ORDER BY created_at DESC LIMIT 100").all();
  res.json({ stats, byMonth, queue });
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
  processQueue();
});
