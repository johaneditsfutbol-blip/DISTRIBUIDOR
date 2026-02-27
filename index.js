const express = require('express');
const axios = require('axios');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ============================================================================
// 1. MEMORIA Y ESTADO (LA ARMADURA Y LA COLA)
// ============================================================================

const OBREROS = [
    { id: 1, url: 'https://obrero-1-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false },
    { id: 2, url: 'https://obrero-2-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false },
    { id: 3, url: 'https://obrero-3-1-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false },
    { id: 4, url: 'https://obrero-4-2-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false },
    { id: 5, url: 'https://obrero-5-3-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false },
    { id: 6, url: 'https://obrero-6-4-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false },
    { id: 7, url: 'https://obrero-7-5-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false }
];

const MAX_LOGS = 1000;
let HISTORIAL = [];

// --- SISTEMA DE ENCOLAMIENTO ---
const COLA_DE_ESPERA = [];
const TIMEOUT_SALA_ESPERA = 45000; // 45 segundos

const esperarTurno = (requestId, etiqueta, esBackground = false) => {
    return new Promise((resolve, reject) => {
        let timeoutCola = null;
        
        // Si no es un proceso en background (pago anticipado), aplicamos el límite de tiempo
        if (!esBackground) {
            timeoutCola = setTimeout(() => {
                const index = COLA_DE_ESPERA.findIndex(c => c.resolve === resolve);
                if (index !== -1) COLA_DE_ESPERA.splice(index, 1);
                reject(new Error("TIMEOUT_COLA"));
            }, TIMEOUT_SALA_ESPERA);
        }

        COLA_DE_ESPERA.push({ resolve, timeoutCola, requestId, etiqueta });
    });
};
// ---------------------------------------

const agregarLog = (reqId, tipo, mensaje, obreroId = 'SYS', duracion = null) => {
    HISTORIAL.unshift({
        tiempo: Date.now(), reqId, tipo, mensaje, obreroId, duracion
    });
    if (HISTORIAL.length > MAX_LOGS) HISTORIAL.pop();

    const duracionStr = duracion ? ` (${duracion}ms)` : '';
    const obreroStr = obreroId !== 'SYS' ? `[OB-${obreroId}]` : '[SYS]';
    let icono = 'ℹ️';
    if (tipo === 'NUEVA') icono = '📥';
    if (tipo === 'EXITO') icono = '✅';
    if (tipo === 'ERROR') icono = '❌';
    if (tipo === 'ALERTA') icono = '🚨';
    if (tipo === 'COLA') icono = '⏳';
    
    console.log(`${icono} [REQ: ${reqId}] ${obreroStr} ${mensaje}${duracionStr}`);
};

const formatoLogConsola = (titulo, objeto) => {
    try {
        const str = JSON.stringify(objeto, null, 2);
        if (str.length > 500) return `    └─ ${titulo}: { ... [Objeto Grande] Claves: ${Object.keys(objeto).join(', ')} ... }`;
        return `    └─ ${titulo}:\n${str.split('\n').map(l => `        ${l}`).join('\n')}`;
    } catch (e) { return `    └─ ${titulo}: [No se parseable]`; }
};

// ============================================================================
// 2. API INTERNA DEL CENTRO DE COMANDO (C2)
// ============================================================================

app.get('/api/tactico/estado', (req, res) => {
    res.json({ obreros: OBREROS, historial: HISTORIAL, encolados: COLA_DE_ESPERA.length });
});

app.post('/api/tactico/orden66/:id', (req, res) => {
    const id = parseInt(req.params.id);
    const obrero = OBREROS.find(o => o.id === id);
    if (!obrero) return res.status(404).json({ error: "Obrero fantasma." });

    agregarLog('MANUAL', 'ALERTA', 'Orden 66 ejecutada manualmente.', id);
    obrero.activo = false;
    obrero.fallos = 99; 

    axios.post(`${obrero.url}/orden-66`, {}, { 
        headers: { 'x-comandante-secret': 'IcaroSoft_Destruccion_Inminente_2026' }, 
        timeout: 3000 
    }).catch(() => {});

    setTimeout(() => {
        obrero.activo = true;
        obrero.fallos = 0;
        agregarLog('SYS', 'INFO', 'Obrero resucitado tras purga manual.', id);
    }, 40000);

    res.json({ success: true, message: "Misil disparado." });
});

app.post('/api/tactico/revivir/:id', (req, res) => {
    const id = parseInt(req.params.id);
    const obrero = OBREROS.find(o => o.id === id);
    if (!obrero) return res.status(404).json({ error: "Obrero fantasma." });

    agregarLog('MANUAL', 'INFO', 'Desfibrilador aplicado manualmente.', id);
    obrero.activo = true;
    obrero.fallos = 0;
    obrero.cocinandoHasta = 0;
    obrero.buscandoServicios = false;

    res.json({ success: true, message: "Obrero resucitado." });
});

// ============================================================================
// 3. DASHBOARD VISUAL (HTML/JS)
// ============================================================================

app.get('/status', (req, res) => {
    const html = `
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Comandante - C2</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
        <style>
            body { background-color: #0b1120; color: #e2e8f0; font-family: 'Consolas', 'Courier New', monospace; }
            .glass-panel { background: rgba(30, 41, 59, 0.6); backdrop-filter: blur(8px); border: 1px solid rgba(56, 189, 248, 0.2); }
            .neon-text { text-shadow: 0 0 8px rgba(56, 189, 248, 0.6); }
            .scan-line { width: 100%; height: 2px; background: rgba(56, 189, 248, 0.3); position: absolute; top: 0; animation: scan 3s linear infinite; }
            @keyframes scan { 0% { top: 0; opacity: 0; } 10% { opacity: 1; } 90% { opacity: 1; } 100% { top: 100%; opacity: 0; } }
            ::-webkit-scrollbar { width: 6px; }
            ::-webkit-scrollbar-track { background: #0b1120; }
            ::-webkit-scrollbar-thumb { background: #38bdf8; border-radius: 3px; }
        </style>
    </head>
    <body class="p-4 md:p-8 min-h-screen relative overflow-x-hidden flex flex-col">
        <div class="fixed inset-0 pointer-events-none z-0"><div class="scan-line"></div></div>

        <div class="max-w-7xl mx-auto w-full relative z-10 flex-grow flex flex-col">
            <header class="flex flex-col md:flex-row justify-between items-center mb-6 pb-4 border-b border-sky-900/50">
                <div class="flex items-center gap-4 mb-4 md:mb-0">
                    <div class="bg-sky-500/20 p-3 rounded-lg border border-sky-500/50">
                        <i class="fa-solid fa-satellite-dish text-3xl text-sky-400 neon-text"></i>
                    </div>
                    <div>
                        <h1 class="text-2xl md:text-3xl font-bold text-white tracking-widest">COMANDANTE <span class="text-sky-400">V4.5</span></h1>
                        <p class="text-sky-500/70 text-sm">CENTRO DE MANDO Y ENCOLAMIENTO</p>
                    </div>
                </div>
                <div class="flex flex-col items-end gap-2">
                    <div class="flex items-center gap-3">
                        <div id="badge-cola" class="flex items-center gap-2 bg-indigo-900/40 px-3 py-1.5 rounded-full border border-indigo-700/50 transition-colors">
                            <i class="fa-solid fa-users text-indigo-400"></i>
                            <span id="txt-cola" class="text-xs font-bold tracking-wider text-indigo-400">COLA: 0</span>
                        </div>
                        <div class="flex items-center gap-3 bg-slate-800/80 px-4 py-2 rounded-full border border-slate-700">
                            <span class="relative flex h-3 w-3">
                                <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                                <span class="relative inline-flex rounded-full h-3 w-3 bg-green-500"></span>
                            </span>
                            <span class="text-sm font-semibold tracking-wider text-green-400">ENLACE ACTIVO</span>
                        </div>
                    </div>
                    <span id="txt-actualizacion" class="text-xs text-slate-500 mt-2">Última lectura: Calculando...</span>
                </div>
            </header>
            
            <div id="grid-obreros" class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4 mb-6"></div>

            <div class="glass-panel rounded-xl p-4 border-t-4 border-t-slate-500 bg-slate-900/80 flex-grow flex flex-col shadow-2xl">
                <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-3 border-b border-slate-700 pb-3 gap-3">
                    <h2 class="text-lg font-bold text-white flex items-center whitespace-nowrap">
                        <i class="fa-solid fa-terminal mr-2 text-slate-400"></i> Auditoría
                    </h2>
                    
                    <div class="flex flex-col sm:flex-row gap-2 w-full md:w-auto">
                        <div class="relative w-full sm:w-64">
                            <div class="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none">
                                <i class="fa-solid fa-search text-slate-500 text-xs"></i>
                            </div>
                            <input type="text" id="input-busqueda" placeholder="Buscar ID Cliente, REQ o mensaje..." class="bg-slate-950 border border-slate-700 text-slate-300 text-xs rounded-lg focus:ring-sky-500 focus:border-sky-500 block w-full pl-8 p-2 placeholder-slate-600 outline-none transition-all">
                        </div>
                        <select id="filtro-tipo" class="bg-slate-950 border border-slate-700 text-slate-300 text-xs rounded-lg focus:ring-sky-500 focus:border-sky-500 block w-full sm:w-32 p-2 outline-none cursor-pointer">
                            <option value="ALL">Todos los Tipos</option>
                            <option value="ERROR">❌ Errores</option>
                            <option value="EXITO">✅ Éxitos</option>
                            <option value="COLA">⏳ En Cola</option>
                            <option value="ALERTA">🚨 Alertas</option>
                            <option value="NUEVA">📥 Entrantes</option>
                            <option value="INFO">ℹ️ Info</option>
                        </select>
                    </div>
                </div>

                <div id="terminal-logs" class="overflow-y-auto flex-grow h-72 font-mono text-xs space-y-1.5 pr-2 mb-3"></div>

                <div class="flex justify-between items-center text-xs text-slate-400 border-t border-slate-700 pt-3 mt-auto">
                    <span id="txt-resultados">Mostrando 0 resultados</span>
                    <div class="flex items-center gap-1">
                        <button id="btn-prev" class="px-3 py-1 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded border border-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"><i class="fa-solid fa-chevron-left"></i></button>
                        <span id="txt-paginacion" class="px-3 py-1 font-bold text-white bg-slate-950 rounded border border-slate-800">1 / 1</span>
                        <button id="btn-next" class="px-3 py-1 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded border border-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"><i class="fa-solid fa-chevron-right"></i></button>
                    </div>
                </div>
            </div>
        </div>

        <script>
            // --- ESTADO LOCAL DEL DASHBOARD ---
            let historialGlobal = [];
            let paginaActual = 1;
            const LOGS_POR_PAGINA = 50;

            const grid = document.getElementById('grid-obreros');
            const terminal = document.getElementById('terminal-logs');
            const txtActualizacion = document.getElementById('txt-actualizacion');
            const txtCola = document.getElementById('txt-cola');
            const badgeCola = document.getElementById('badge-cola');
            
            // Elementos de UI de filtrado y paginación
            const inputBusqueda = document.getElementById('input-busqueda');
            const filtroTipo = document.getElementById('filtro-tipo');
            const btnPrev = document.getElementById('btn-prev');
            const btnNext = document.getElementById('btn-next');
            const txtPaginacion = document.getElementById('txt-paginacion');
            const txtResultados = document.getElementById('txt-resultados');

            // --- LISTENERS DE FILTROS ---
            inputBusqueda.addEventListener('input', () => { paginaActual = 1; renderizarTerminal(); });
            filtroTipo.addEventListener('change', () => { paginaActual = 1; renderizarTerminal(); });
            
            btnPrev.addEventListener('click', () => { 
                if (paginaActual > 1) { paginaActual--; renderizarTerminal(); } 
            });
            btnNext.addEventListener('click', () => { 
                paginaActual++; renderizarTerminal(); 
            });

            const formatearHora = (ms) => {
                const d = new Date(ms);
                return d.toLocaleTimeString('es-VE', { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0');
            };

            async function escanearEscuadron() {
                try {
                    const respuesta = await fetch('/api/tactico/estado');
                    const data = await respuesta.json(); 
                    
                    renderizarObreros(data.obreros);
                    
                    // Actualizamos la base de datos local y re-renderizamos sin perder la página
                    historialGlobal = data.historial;
                    renderizarTerminal();
                    
                    txtCola.innerText = 'COLA: ' + data.encolados;
                    if (data.encolados > 0) {
                        badgeCola.classList.replace('bg-indigo-900/40', 'bg-indigo-600/80');
                        badgeCola.classList.replace('border-indigo-700/50', 'border-indigo-400');
                        txtCola.classList.replace('text-indigo-400', 'text-white');
                    } else {
                        badgeCola.classList.replace('bg-indigo-600/80', 'bg-indigo-900/40');
                        badgeCola.classList.replace('border-indigo-400', 'border-indigo-700/50');
                        txtCola.classList.replace('text-white', 'text-indigo-400');
                    }
                    
                    const ahora = new Date();
                    txtActualizacion.innerText = \`Última lectura: \${ahora.toLocaleTimeString('es-VE', {hour12: false})}.\${ahora.getMilliseconds()}\`;
                    txtActualizacion.classList.remove('text-red-500');
                } catch (error) {
                    txtActualizacion.innerText = "❌ ERROR DE CONEXIÓN";
                    txtActualizacion.classList.add('text-red-500');
                }
            }

            function renderizarObreros(obreros) {
                const tiempoActual = Date.now();
                let htmlTemp = '';

                obreros.forEach(o => {
                    const isVivo = o.activo;
                    const isCocinando = o.cocinandoHasta > tiempoActual;
                    const segCoccion = isCocinando ? Math.ceil((o.cocinandoHasta - tiempoActual) / 1000) : 0;
                    
                    let colorFondo = isVivo ? 'bg-slate-800/40' : 'bg-red-950/40';
                    let badgeEstado = isVivo 
                        ? \`<span class="bg-green-500/20 text-green-400 px-2 py-1 rounded text-[10px] border border-green-500/30"><i class="fa-solid fa-check mr-1"></i>ACTIVO</span>\`
                        : \`<span class="bg-red-500/20 text-red-400 px-2 py-1 rounded text-[10px] border border-red-500/30 animate-pulse"><i class="fa-solid fa-skull mr-1"></i>CUARENTENA</span>\`;

                    let badgesOp = '';
                    if (isCocinando) badgesOp += \`<span class="bg-orange-500/20 text-orange-400 px-2 py-1 rounded text-[10px] border border-orange-500/30"><i class="fa-solid fa-fire mr-1"></i>\${segCoccion}s</span> \`;
                    if (o.buscandoServicios) badgesOp += \`<span class="bg-blue-500/20 text-blue-400 px-2 py-1 rounded text-[10px] border border-blue-500/30"><i class="fa-solid fa-search mr-1"></i>SRV</span>\`;

                    const porcentajeCarga = Math.min((o.carga / 10) * 100, 100);
                    let colorCarga = o.carga > 5 ? 'bg-red-500' : (o.carga > 2 ? 'bg-yellow-500' : 'bg-green-500');

                    htmlTemp += \`
                    <div class="glass-panel rounded-lg p-4 border-t-2 \${isVivo ? 'border-t-green-500' : 'border-t-red-600'} flex flex-col \${colorFondo} transition-all duration-300">
                        <div class="flex justify-between items-center mb-2">
                            <h2 class="text-lg font-bold text-white">OBRERO-\${o.id}</h2>
                            <div class="text-xl font-black \${o.fallos > 0 ? 'text-red-400' : 'text-slate-600'}" title="Fallos consecutivos">\${o.fallos}</div>
                        </div>
                        <div class="flex gap-1 flex-wrap mb-3">\${badgeEstado}\${badgesOp}</div>

                        <div class="mb-3 bg-slate-900/80 p-2 rounded border border-slate-700/50">
                            <div class="flex justify-between text-[10px] text-slate-400 mb-1">
                                <span>Carga: \${o.carga}</span>
                            </div>
                            <div class="w-full bg-slate-800 rounded-full h-1 overflow-hidden">
                                <div class="\${colorCarga} h-1 rounded-full transition-all duration-500" style="width: \${porcentajeCarga}%"></div>
                            </div>
                        </div>

                        <div class="grid grid-cols-2 gap-2 mt-auto">
                            <button onclick="ejecutarOrden(\${o.id})" class="bg-red-900/40 hover:bg-red-600/60 text-red-400 hover:text-white border border-red-800/50 py-1.5 rounded text-[10px] font-bold transition-colors">
                                <i class="fa-solid fa-radiation"></i> PURGAR
                            </button>
                            <button onclick="revivir(\${o.id})" class="bg-emerald-900/40 hover:bg-emerald-600/60 text-emerald-400 hover:text-white border border-emerald-800/50 py-1.5 rounded text-[10px] font-bold transition-colors">
                                <i class="fa-solid fa-bolt"></i> REVIVIR
                            </button>
                        </div>
                    </div>
                    \`;
                });
                grid.innerHTML = htmlTemp;
            }

            function renderizarTerminal() {
                // 1. Obtener valores de filtros
                const busqueda = inputBusqueda.value.toLowerCase().trim();
                const tipoFiltrado = filtroTipo.value;

                // 2. Aplicar Filtros al historial global
                let logsFiltrados = historialGlobal.filter(log => {
                    const coincideTipo = tipoFiltrado === 'ALL' || log.tipo === tipoFiltrado;
                    const textoCompleto = \`\${log.reqId} \${log.mensaje} \${log.obreroId}\`.toLowerCase();
                    const coincideBusqueda = busqueda === '' || textoCompleto.includes(busqueda);
                    return coincideTipo && coincideBusqueda;
                });

                // 3. Lógica de Paginación
                const totalPaginas = Math.max(1, Math.ceil(logsFiltrados.length / LOGS_POR_PAGINA));
                if (paginaActual > totalPaginas) paginaActual = totalPaginas;

                const indexInicio = (paginaActual - 1) * LOGS_POR_PAGINA;
                const indexFin = indexInicio + LOGS_POR_PAGINA;
                const logsPaginados = logsFiltrados.slice(indexInicio, indexFin);

                // 4. Actualizar UI de Controles
                txtPaginacion.innerText = \`\${paginaActual} / \${totalPaginas}\`;
                txtResultados.innerText = \`Total: \${logsFiltrados.length} eventos (Historial: \${historialGlobal.length})\`;
                btnPrev.disabled = paginaActual === 1;
                btnNext.disabled = paginaActual === totalPaginas;

                // 5. Renderizar Logs
                let htmlTemp = '';
                if(logsPaginados.length === 0) {
                    terminal.innerHTML = '<div class="text-slate-500 italic mt-4 text-center">No se encontraron registros para esta búsqueda...</div>';
                    return;
                }

                logsPaginados.forEach(log => {
                    let colorBase = 'text-slate-300';
                    let bgBadge = 'bg-slate-800 text-slate-400 border-slate-600';
                    let icono = 'fa-info-circle';

                    if (log.tipo === 'NUEVA') { bgBadge = 'bg-blue-900/50 text-blue-400 border-blue-700'; icono = 'fa-arrow-right-to-bracket'; }
                    if (log.tipo === 'EXITO') { bgBadge = 'bg-green-900/50 text-green-400 border-green-700'; icono = 'fa-check'; colorBase = 'text-green-100'; }
                    if (log.tipo === 'ERROR') { bgBadge = 'bg-red-900/50 text-red-400 border-red-700'; icono = 'fa-xmark'; colorBase = 'text-red-200'; }
                    if (log.tipo === 'ALERTA') { bgBadge = 'bg-orange-900/50 text-orange-400 border-orange-700'; icono = 'fa-triangle-exclamation'; colorBase = 'text-orange-200'; }
                    if (log.tipo === 'COLA') { bgBadge = 'bg-indigo-900/50 text-indigo-400 border-indigo-700'; icono = 'fa-hourglass-half'; colorBase = 'text-indigo-200'; }
                    
                    const duracionBadge = log.duracion ? \`<span class="text-slate-500 ml-2">(\${log.duracion}ms)</span>\` : '';
                    const obreroTag = log.obreroId !== 'SYS' ? \`<span class="text-sky-400 font-bold ml-2">[OB-\${log.obreroId}]</span>\` : '<span class="text-purple-400 font-bold ml-2">[SYS]</span>';

                    htmlTemp += \`
                    <div class="flex items-start gap-2 p-1.5 hover:bg-slate-800/60 rounded transition-colors border-b border-slate-800/50 pb-2">
                        <div class="text-slate-500 w-24 shrink-0 mt-0.5">\${formatearHora(log.tiempo)}</div>
                        <div class="w-20 shrink-0 text-center border \${bgBadge} rounded text-[9px] font-bold py-0.5 mt-0.5"><i class="fa-solid \${icono}"></i> \${log.tipo}</div>
                        <div class="text-slate-400 w-12 shrink-0 text-center mt-0.5">[\${log.reqId}]</div>
                        <div class="\${colorBase} break-all flex-grow leading-tight">\${log.mensaje} \${obreroTag}\${duracionBadge}</div>
                    </div>
                    \`;
                });
                terminal.innerHTML = htmlTemp;
            }

            async function ejecutarOrden(id) {
                if(!confirm(\`⚠️ Purga del OBRERO-\${id}. Se destruirá el contenedor. ¿Proceder?\`)) return;
                try { await fetch(\`/api/tactico/orden66/\${id}\`, { method: 'POST' }); } catch(e) {}
            }
            async function revivir(id) {
                try { await fetch(\`/api/tactico/revivir/\${id}\`, { method: 'POST' }); } catch(e) {}
            }

            escanearEscuadron();
            setInterval(escanearEscuadron, 1000); 
        </script>
    </body>
    </html>
    `;
    res.send(html);
});

// ============================================================================
// 4. EL CEREBRO DEL ENRUTADOR (NÚCLEO)
// ============================================================================

app.all('*', async (req, res) => {
    if (req.originalUrl === '/favicon.ico' || req.originalUrl.startsWith('/api/tactico')) return res.status(204).end();

    let intentos = 0;
    let exito = false;
    let errorFinal = null;
    let obreroElegido = null; 
    const inicioReloj = Date.now();
    let obrerosDescartados = []; 

    const requestId = Math.random().toString(36).substring(2, 7).toUpperCase();
    
    let etiquetaCliente = "";
    if (req.method === 'POST') {
        if (req.path === '/pagar' && req.body && req.body.id) idCliente = req.body.id;
        else if (req.path === '/pagar-vidanet' && req.body && req.body.datos && req.body.datos.cedula) idCliente = req.body.datos.cedula;
    } else if (req.method === 'GET') {
        if (req.query && req.query.id) idCliente = req.query.id;
        else if (req.query && req.query.cedula) idCliente = req.query.cedula;
    }
    
    const etiqueta = idCliente ? `[👤 ${idCliente}] ` : "";

    const log = (tipo, mensaje, obreroId = 'SYS', duracion = null) => {
        agregarLog(requestId, tipo, `${etiqueta}${mensaje}`, obreroId, duracion);
    };

    log('NUEVA', `Solicitud ${req.method} ${req.path}`);
    
    if (req.method !== 'GET' && Object.keys(req.body).length > 0) {
        const rBody = JSON.parse(JSON.stringify(req.body)); 
        if (rBody.datos && rBody.datos.rutaImagen) {
             rBody.datos.rutaImagen = "[Imagen Oculta solo en el Log]";
        }
        console.log(formatoLogConsola(`${etiqueta}Body [${requestId}]`, rBody));
    }

    // --- BANDERA DE ESTADO (Para la respuesta anticipada) ---
    let respuestaEnviada = false;
    const esRutaPago = (req.path === '/pagar' || req.path === '/pagar-vidanet');

    while (intentos < 3 && !exito) {
        const ahora = Date.now();
        const obrerosDisponibles = OBREROS.filter(o => {
            if (!o.activo || obrerosDescartados.includes(o.id)) return false;
            if (ahora < o.cocinandoHasta) return false;
            if (req.path === '/buscar-servicios' && o.buscandoServicios) return false;
            return true;
        });
        
        // --- LÓGICA DE COLA CON RESPUESTA ANTICIPADA ---
        if (obrerosDisponibles.length === 0) {
            
            // Si es un pago y no hemos respondido, enviamos el 200 OK y activamos la bandera
            if (esRutaPago && !respuestaEnviada) {
                res.status(200).json({ status: "OK", message: "Procesando solicitud en background..." });
                respuestaEnviada = true;
                log('COLA', `Enviada respuesta anticipada. Esperando Obrero (Pos: ${COLA_DE_ESPERA.length + 1})`);
            } else if (!esRutaPago) {
                // Si es consulta, operamos normal
                log('COLA', `Escuadrón ocupado. Entrando a Sala de Espera (Pos: ${COLA_DE_ESPERA.length + 1})`);
            }

            try {
                // Pasamos 'respuestaEnviada' como tercer parámetro a esperarTurno
                await esperarTurno(requestId, etiqueta, respuestaEnviada); 
                continue; 
            } catch (err) {
                if (err.message === "TIMEOUT_COLA") {
                    log('ERROR', `Misión Abortada: Tiempo en sala de espera superado.`);
                    // Solo respondemos error 503 si NO hemos enviado la respuesta 200 OK anticipada
                    if (!respuestaEnviada) {
                        return res.status(503).json({ success: false, message: "Líneas saturadas. Por favor intenta de nuevo." });
                    }
                    // Si ya habíamos respondido 200 OK, simplemente morimos en silencio
                    return; 
                }
            }
        }

        const menorCarga = Math.min(...obrerosDisponibles.map(o => o.carga));
        const empatados = obrerosDisponibles.filter(o => o.carga === menorCarga);
        obreroElegido = empatados[Math.floor(Math.random() * empatados.length)];

        // --- CORTACIRCUITOS DINÁMICO (TIMEOUT) ---
        const limiteTiempo = req.path === '/buscar-servicios' ? 17000 : 120000;
        // ------------------------------------------

        try {
            obreroElegido.carga++;
            
            if (req.path === '/buscar-servicios') {
                obreroElegido.buscandoServicios = true;
            }

            console.log(`  [>> REQ: ${requestId}] ${etiqueta}Intentando Obrero ${obreroElegido.id} (Intento ${intentos + 1}/3)`);

            const respuesta = await axios({
                method: req.method,
                url: `${obreroElegido.url}${req.originalUrl}`,
                data: req.method !== 'GET' ? req.body : undefined,
                headers: { 'Content-Type': 'application/json' },
                timeout: limiteTiempo // <-- SE APLICA EL LÍMITE DE TIEMPO AQUÍ
            });

            const duracion = Date.now() - inicioReloj;
            
            log('EXITO', `Respuesta HTTP ${respuesta.status} devuelta.`, obreroElegido.id, duracion);
            if(respuesta.data) console.log(formatoLogConsola(`${etiqueta}Respuesta [${requestId}]`, respuesta.data));

            if (req.path === '/pagar') {
                obreroElegido.cocinandoHasta = Date.now() + 60000;
                log('INFO', `Let Him Cook (60s)`, obreroElegido.id);
            } else if (req.path === '/pagar-vidanet') {
                obreroElegido.cocinandoHasta = Date.now() + 15000;
                log('INFO', `Let Him Cook (15s)`, obreroElegido.id);
            }

            obreroElegido.fallos = 0;
            
            // Si la respuesta NO fue enviada anticipadamente (ej. consultas), la enviamos ahora
            if (!respuestaEnviada) {
                res.status(respuesta.status).json(respuesta.data);
            }
            
            exito = true;

        } catch (error) {
            const statusError = error.response ? error.response.status : 500; 
            let msjResumido = error.message;
            
            // --- INTERCEPTOR DE TIMEOUT ---
            if (error.code === 'ECONNABORTED' && error.message.includes('timeout')) {
                msjResumido = `TIMEOUT: Obrero congelado. No respondió en ${limiteTiempo / 1000}s.`;
            } else if(error.response && error.response.data && typeof error.response.data === 'object' && error.response.data.error) {
                msjResumido = error.response.data.error; 
            } else if (error.response && error.response.data) {
                msjResumido = typeof error.response.data === 'object' ? JSON.stringify(error.response.data) : error.response.data;
            }

            if (req.path === '/consultar-deudas-vidanet') {
                log('INFO', `Vidanet rechazó consulta: ${msjResumido.substring(0, 50)}`, obreroElegido.id);
                const dataRespuesta = error.response && error.response.data ? error.response.data : { success: false, error: error.message };
                
                // Retornamos el error si no hemos respondido aún
                if (!respuestaEnviada) return res.status(statusError).json(dataRespuesta);
                return;
            }

            intentos++;
            obreroElegido.fallos++;
            obrerosDescartados.push(obreroElegido.id);

            log('ERROR', `Fallo Intento ${intentos}/3 (HTTP ${statusError}): ${msjResumido.substring(0,60)}`, obreroElegido.id);

            if (obreroElegido.fallos >= 2) {
                log('ALERTA', `CIRCUIT BREAKER: Obrero en cuarentena (2 fallos). Enviando Purga.`, obreroElegido.id);
                obreroElegido.activo = false;
                
                axios.post(`${obreroElegido.url}/orden-66`, {}, { headers: { 'x-comandante-secret': 'IcaroSoft_Destruccion_Inminente_2026' }, timeout: 5000 }).catch(() => {});

                const idParaRevivir = obreroElegido.id;
                setTimeout(() => {
                    const obj = OBREROS.find(o => o.id === idParaRevivir);
                    if(obj) {
                        obj.activo = true;
                        obj.fallos = 0;
                        agregarLog('SYS', 'INFO', `Fin de Cuarentena (40s). Obrero resucitado.`, idParaRevivir);
                    }
                }, 40000); 
            }
            errorFinal = msjResumido;

        } finally {
            if (obreroElegido) {
                obreroElegido.carga--;
                if (req.path === '/buscar-servicios') {
                    obreroElegido.buscandoServicios = false;
                    log('INFO', 'Candado Liberado: Termina búsqueda de servicios.', obreroElegido.id);
                }
            }

            if (COLA_DE_ESPERA.length > 0) {
                const siguienteEnFila = COLA_DE_ESPERA.shift();
                if (siguienteEnFila.timeoutCola) clearTimeout(siguienteEnFila.timeoutCola); 
                agregarLog(siguienteEnFila.requestId, 'INFO', `${siguienteEnFila.etiqueta}Saliendo de la sala de espera. Evaluando obreros...`);
                siguienteEnFila.resolve(); 
            }
        }
    }

    if (!exito) {
        log('ERROR', 'Misión Fallida: Reintentos agotados.');
        // Solo respondemos el error final si no enviamos el "Fake 200 OK" antes
        if (!respuestaEnviada) {
            res.status(500).json({ success: false, message: "El escuadrón está inestable. Reintentos agotados.", detalle: errorFinal });
        }
    }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`\n======================================`);
    console.log(`🚀 COMANDANTE V4.6 (FAIL FAST & QUEUE)`);
    console.log(`📡 Puerto: ${PORT}`);
    console.log(`🤖 Obreros: ${OBREROS.length}`);
    console.log(`======================================\n`);
    agregarLog('SYS', 'INFO', `Sistema Inicializado. ${OBREROS.length} Obreros reportándose.`);
});
