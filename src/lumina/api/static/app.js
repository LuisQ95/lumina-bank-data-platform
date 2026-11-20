/**
 * Página de Lumina Bank: envía transacciones reales a Pub/Sub, sigue su decisión en BigQuery
 * y muestra los datos Gold que expone la API.
 */
"use strict";

const $ = (sel) => document.querySelector(sel);
const fmtNum = new Intl.NumberFormat("es-PE", { maximumFractionDigits: 2 });
const fmtHora = new Intl.DateTimeFormat("es-PE", { timeZone: "America/Lima", hour: "2-digit", minute: "2-digit", second: "2-digit" });

let clientes = {};
let ultimoEvento = null;
let memorystore = false;

/**
 * Crea un elemento del DOM sin interpretar HTML, para que ningún dato pueda inyectar código.
 * @param {string} tag Etiqueta HTML.
 * @param {Object<string, string>} attrs Atributos; `class` se asigna como className.
 * @param {...(Node|string|number)} hijos Nodos o textos hijos.
 * @returns {HTMLElement}
 */
function el(tag, attrs = {}, ...hijos) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") n.className = v; else n.setAttribute(k, v);
  }
  for (const h of hijos.flat()) n.append(h instanceof Node ? h : document.createTextNode(h ?? ""));
  return n;
}

/**
 * Llama a un endpoint de la API y devuelve el estado y el cuerpo JSON.
 * @param {string} ruta Ruta relativa del endpoint.
 * @param {RequestInit} opciones Opciones de fetch.
 * @returns {Promise<{ok: boolean, status: number, cuerpo: any}>}
 */
async function api(ruta, opciones = {}) {
  const r = await fetch(ruta, { headers: { "Content-Type": "application/json" }, ...opciones });
  const cuerpo = await r.json().catch(() => ({}));
  return { ok: r.ok, status: r.status, cuerpo };
}

const esperar = (ms) => new Promise((ok) => setTimeout(ok, ms));
const hora = (iso) => (iso ? fmtHora.format(new Date(iso)) : "");

/**
 * Celda de tabla con formato según el tipo de dato.
 * @param {string} clave Nombre de la columna.
 * @param {*} valor Valor de la celda.
 * @returns {HTMLTableCellElement}
 */
function celda(clave, valor) {
  if (clave === "decision") return el("td", {}, el("span", { class: `etiqueta ${valor}` }, valor));
  if (/_ts$/.test(clave) && valor) return el("td", {}, hora(valor));
  if (typeof valor === "number") return el("td", { class: "num" }, fmtNum.format(valor));
  return el("td", {}, valor === null ? "" : String(valor));
}

/**
 * Reemplaza el contenido de una tabla con filas JSON; las columnas salen de la primera fila.
 * @param {HTMLTableElement} tabla Tabla destino.
 * @param {Object[]} filas Filas devueltas por la API.
 */
function pintarTabla(tabla, filas) {
  tabla.replaceChildren();
  if (!filas.length) { tabla.append(el("tr", {}, el("td", {}, "Sin filas."))); return; }
  const columnas = Object.keys(filas[0]);
  tabla.append(el("thead", {}, el("tr", {}, columnas.map((c) => el("th", {}, c)))));
  tabla.append(el("tbody", {}, filas.map((f) => el("tr", {}, columnas.map((c) => celda(c, f[c]))))));
}

/** Consulta /api/salud y actualiza los indicadores de la API y de Memorystore. */
async function cargarSalud() {
  const { ok, cuerpo } = await api("/api/salud");
  const chipApi = $("#chip-api");
  chipApi.textContent = ok ? `API: en línea · ${cuerpo.proyecto}` : "API: sin respuesta";
  chipApi.className = `chip ${ok ? "ok" : "apagado"}`;
  memorystore = ok && cuerpo.memorystore_configurado;
  const chipRedis = $("#chip-redis");
  chipRedis.textContent = memorystore ? "Memorystore: conectado por VPC" : "Memorystore: sin conexión desde la API";
  chipRedis.className = `chip ${memorystore ? "ok" : "apagado"}`;
}

/** Llena el selector con los clientes del core habilitados para la demo. */
async function cargarClientes() {
  const { ok, cuerpo } = await api("/api/clientes");
  const sel = $("#cliente");
  sel.replaceChildren();
  if (!ok) { sel.append(el("option", { value: "" }, "No se pudieron cargar los clientes")); return; }
  for (const c of cuerpo) {
    clientes[c.id_cliente] = c;
    sel.append(el("option", { value: c.id_cliente }, `${c.id_cliente} · ${c.id_distrito} · límite S/ ${fmtNum.format(c.limite_diario_pen)}`));
  }
  mostrarPerfil();
}

/** Muestra el perfil del cliente elegido y propone su distrito registrado para el GPS y la operación. */
function mostrarPerfil() {
  const c = clientes[$("#cliente").value];
  const dl = $("#perfil");
  dl.replaceChildren();
  if (!c) return;
  for (const [t, v] of [["Distrito registrado", c.id_distrito], ["Límite diario", `S/ ${fmtNum.format(c.limite_diario_pen)}`], ["Segmento", c.segmento]]) {
    dl.append(el("div", {}, el("dt", {}, t), el("dd", {}, v)));
  }
  $("#gps-distrito").value = c.id_distrito;
  $("#distrito").value = c.id_distrito;
}

/**
 * Agrega un paso al seguimiento de la transacción.
 * @param {string} texto Descripción del paso.
 * @param {"hecho"|"esperando"|"fallo"} estado Estado visual.
 * @param {string} detalle Texto secundario opcional.
 * @returns {HTMLLIElement}
 */
function paso(texto, estado, detalle = "") {
  const li = el("li", { class: estado }, texto, detalle ? el("small", {}, detalle) : "");
  $("#pasos").append(li);
  return li;
}

/** Limpia el panel de seguimiento antes de una nueva operación. */
function reiniciarSeguimiento() {
  $("#pasos").replaceChildren();
  $("#resultado").replaceChildren();
  $("#estado-redis").replaceChildren();
}

/**
 * Tarjeta con la decisión que Dataflow escribió en BigQuery.
 * @param {Object} d Fila de silver_banco.stream_decisiones_dataflow.
 * @returns {HTMLElement}
 */
function tarjetaDecision(d) {
  return el("div", { class: `decision ${d.decision}` },
    el("h3", {}, d.decision.replace("_", " ")),
    el("div", { class: "metricas" },
      el("div", {}, "Riesgo", el("b", {}, fmtNum.format(d.riesgo))),
      el("div", {}, "Tx último minuto", el("b", {}, d.vel_1m)),
      el("div", {}, "Salto de GPS", el("b", {}, d.salto_gps ? "Sí" : "No")),
      el("div", {}, "Monto", el("b", {}, `S/ ${fmtNum.format(d.monto_pen)}`)),
      el("div", {}, "Latencia de punta a punta", el("b", {}, `${fmtNum.format(d.latencia_e2e_ms)} ms`)),
      el("div", {}, "Filas con este id", el("b", {}, d.filas_mismo_id))));
}

/**
 * Muestra el estado del cliente en Memorystore si la API tiene ruta a la VPC.
 * @param {string} idCliente Cliente de la transacción.
 * @param {string} idTx Transacción cuya clave de deduplicación se verifica.
 */
async function mostrarRedis(idCliente, idTx) {
  const caja = $("#estado-redis");
  caja.replaceChildren();
  if (!memorystore) {
    caja.append(el("p", { class: "aviso" }, "La API no tiene ruta a Memorystore en este despliegue. El estado se verifica con redis-cli desde la VM."));
    return;
  }
  const { ok, cuerpo } = await api(`/api/estado/${encodeURIComponent(idCliente)}?id_transaccion=${encodeURIComponent(idTx)}`);
  if (!ok) { caja.append(el("p", { class: "aviso" }, cuerpo.error || "Memorystore no respondió.")); return; }
  const filas = [
    ["Clave dedup de la transacción", cuerpo.transaccion_registrada_en_dedup ? "registrada" : "no existe"],
    ["GPS del cliente", cuerpo.gps_distrito || "sin dato"],
    ["Transacciones en los últimos 5 min", cuerpo.transacciones_ultimos_5_min],
    ["Acumulado del día (S/)", cuerpo.limites.acumulado_dia_pen ? fmtNum.format(Number(cuerpo.limites.acumulado_dia_pen)) : "0"],
    ["Última decisión", cuerpo.limites.ultima_decision || ""],
  ];
  caja.append(el("h3", {}, `Estado en Memorystore (${cuerpo.redis_host})`),
    el("table", {}, el("tbody", {}, filas.map(([k, v]) => el("tr", {}, el("td", {}, k), el("td", {}, String(v)))))));
}

/**
 * Consulta la decisión cada 2 s hasta que aparece en BigQuery o vence el plazo.
 * @param {string} idTx Id de la transacción publicada.
 * @param {number} segundos Plazo máximo de espera.
 * @returns {Promise<Object|null>} La fila de la decisión, o null si no llegó.
 */
async function esperarDecision(idTx, segundos = 90) {
  const li = paso("Esperando la decisión de Dataflow en BigQuery…", "esperando");
  const inicio = Date.now();
  while (Date.now() - inicio < segundos * 1000) {
    const { ok, status, cuerpo } = await api(`/api/decisiones/${idTx}`);
    if (ok) {
      li.className = "hecho";
      li.replaceChildren("Decisión registrada en BigQuery", el("small", {}, `${((Date.now() - inicio) / 1000).toFixed(1)} s después de publicar`));
      return cuerpo;
    }
    if (status !== 404) { li.className = "fallo"; li.textContent = cuerpo.error || "Error al consultar BigQuery"; return null; }
    await esperar(2000);
  }
  li.className = "fallo";
  li.replaceChildren("No llegó la decisión", el("small", {}, "Verifica que el job de Dataflow esté en ejecución."));
  return null;
}

/** @returns {Object<string, string>} Valores actuales del formulario de transacción. */
function datosFormulario() {
  return Object.fromEntries(new FormData($("#form-tx")).entries());
}

/**
 * Publica una transacción mediante la API y registra el paso.
 * @param {Object} datos Cuerpo para POST /api/transacciones.
 * @returns {Promise<Object|null>} El evento publicado, o null si la API lo rechazó.
 */
async function publicarTx(datos) {
  const { ok, cuerpo } = await api("/api/transacciones", { method: "POST", body: JSON.stringify(datos) });
  if (!ok) { paso(cuerpo.error || "No se pudo publicar", "fallo"); return null; }
  paso(`Publicada en Pub/Sub: ${cuerpo.evento.id_transaccion}`, "hecho", `message_id ${cuerpo.pubsub_message_id} · ${hora(new Date(cuerpo.evento.epoch_ms).toISOString())}`);
  return cuerpo.evento;
}

/**
 * Envía la transacción del formulario y muestra su decisión y el estado en Memorystore.
 * @param {SubmitEvent} ev Evento del formulario.
 */
async function enviar(ev) {
  ev.preventDefault();
  reiniciarSeguimiento();
  $("#btn-enviar").disabled = true;
  try {
    const evento = await publicarTx(datosFormulario());
    if (!evento) return;
    ultimoEvento = evento;
    $("#btn-reintento").disabled = false;
    const d = await esperarDecision(evento.id_transaccion);
    if (d) {
      $("#resultado").append(tarjetaDecision(d));
      await mostrarRedis(evento.id_cliente, evento.id_transaccion);
      cargarRecientes();
    }
  } finally {
    $("#btn-enviar").disabled = false;
  }
}

/** Reenvía la última transacción con el mismo id y verifica que BigQuery siga con una sola fila. */
async function reintento() {
  if (!ultimoEvento) return;
  reiniciarSeguimiento();
  $("#btn-reintento").disabled = true;
  const e = ultimoEvento;
  const evento = await publicarTx({
    id_transaccion: e.id_transaccion, epoch_ms: e.epoch_ms, id_cliente: e.id_cliente, id_distrito: e.id_distrito,
    canal: e.canal, tipo_operacion: e.type, monto_pen: e.amount,
  });
  if (evento) {
    paso("Reintento enviado con el mismo id. Se espera 20 s a que Dataflow lo procese…", "esperando");
    await esperar(20000);
    const { ok, cuerpo } = await api(`/api/decisiones/${evento.id_transaccion}`);
    if (ok) {
      const filtrado = cuerpo.filas_mismo_id === 1;
      paso(filtrado ? "BigQuery sigue con 1 sola fila: Redis descartó el duplicado" : `BigQuery tiene ${cuerpo.filas_mismo_id} filas con este id`, filtrado ? "hecho" : "fallo");
      $("#resultado").append(tarjetaDecision(cuerpo));
      await mostrarRedis(evento.id_cliente, evento.id_transaccion);
    }
  }
  $("#btn-reintento").disabled = false;
}

/** Publica cuatro transacciones seguidas del mismo cliente para activar la regla de velocidad. */
async function rafaga() {
  reiniciarSeguimiento();
  $("#btn-rafaga").disabled = true;
  const datos = datosFormulario();
  const eventos = [];
  for (let i = 0; i < 4; i++) {
    const e = await publicarTx(datos);
    if (e) eventos.push(e);
  }
  const validos = (await Promise.all(eventos.map((e) => esperarDecision(e.id_transaccion)))).filter(Boolean);
  if (validos.length) {
    const filas = validos.map((d) => ({ id_transaccion: d.id_transaccion, vel_1m: d.vel_1m, riesgo: d.riesgo, decision: d.decision }));
    const tabla = el("table");
    pintarTabla(tabla, filas);
    $("#resultado").append(tabla);
    await mostrarRedis(validos[0].id_cliente, validos[0].id_transaccion);
    cargarRecientes();
  }
  $("#btn-rafaga").disabled = false;
}

/** Publica la ubicación del celular del cliente, que Dataflow guarda en Memorystore. */
async function reportarGps() {
  const id = $("#cliente").value;
  if (!id) return;
  const { ok, cuerpo } = await api("/api/gps", { method: "POST", body: JSON.stringify({ id_cliente: id, id_distrito: $("#gps-distrito").value }) });
  reiniciarSeguimiento();
  if (ok) {
    paso(`GPS publicado en Pub/Sub: ${id} está en ${cuerpo.evento.id_distrito_actual}`, "hecho",
      "Dataflow lo guarda en Memorystore. Si la operación ocurre en otro distrito, sube el riesgo.");
  } else {
    paso(cuerpo.error || "No se pudo publicar el GPS", "fallo");
  }
}

/** Actualiza la tabla de decisiones recientes y los indicadores por tipo de decisión. */
async function cargarRecientes() {
  const [rec, res] = await Promise.all([api("/api/decisiones/recientes?limite=15"), api("/api/decisiones/resumen?horas=720")]);
  if (rec.ok) pintarTabla($("#tabla-recientes"), rec.cuerpo);
  const caja = $("#resumen");
  caja.replaceChildren();
  if (!res.ok) return;
  const total = res.cuerpo.reduce((s, r) => s + r.transacciones, 0);
  caja.append(el("div", { class: "kpi" }, el("span", {}, "Decisiones en los últimos 30 días"), el("b", {}, fmtNum.format(total))));
  for (const r of res.cuerpo) {
    caja.append(el("div", { class: "kpi" }, el("span", {}, r.decision), el("b", {}, fmtNum.format(r.transacciones)),
      el("span", {}, `p50 ${fmtNum.format(r.latencia_p50_ms)} ms · p95 ${fmtNum.format(r.latencia_p95_ms)} ms`)));
  }
}

/**
 * Carga la tabla Gold de la pestaña elegida.
 * @param {HTMLButtonElement} boton Pestaña con el endpoint en data-gold.
 */
async function cargarGold(boton) {
  for (const b of document.querySelectorAll("[data-gold]")) b.setAttribute("aria-selected", String(b === boton));
  const ruta = boton.dataset.gold;
  $("#gold-endpoint").textContent = `GET ${ruta}`;
  const { ok, cuerpo } = await api(ruta);
  pintarTabla($("#tabla-gold"), ok ? cuerpo : []);
}

document.addEventListener("DOMContentLoaded", () => {
  $("#form-tx").addEventListener("submit", enviar);
  $("#cliente").addEventListener("change", mostrarPerfil);
  $("#btn-reintento").addEventListener("click", reintento);
  $("#btn-rafaga").addEventListener("click", rafaga);
  $("#btn-gps").addEventListener("click", reportarGps);
  for (const b of document.querySelectorAll("[data-gold]")) b.addEventListener("click", () => cargarGold(b));
  cargarSalud();
  cargarClientes();
  cargarRecientes();
  cargarGold(document.querySelector("[data-gold]"));
  setInterval(cargarRecientes, 10000);
});
