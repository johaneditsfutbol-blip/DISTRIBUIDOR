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

// --- NUEVA BÓVEDA EXCLUSIVA PARA LOS JEFES (PAGOS EXITOSOS) ---
const MAX_PAGOS = 2000;
let PAGOS_EXITOSOS = [];

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
    res.json({ obreros: OBREROS, historial: HISTORIAL, encolados: COLA_DE_ESPERA.length, pagos: PAGOS_EXITOSOS });
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
// 3. DASHBOARD VISUAL (HTML/JS) - V5 PREMIUM COMMAND CENTER
// ============================================================================

app.get('/status', (req, res) => {
    const html = `
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>CX-NEXUS | Command Center</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
        <link href="https://fonts.googleapis.com/css2?family=Fira+Code:wght@300;400;500;600&family=Rajdhani:wght@400;500;600;700&display=swap" rel="stylesheet">
        
        <script>
            tailwind.config = {
                theme: {
                    extend: {
                        colors: {
                            gold: { 400: '#F3E5AB', 500: '#D4AF37', 600: '#AA8C2C', 900: '#4A3D13' },
                            dark: { 800: '#121212', 900: '#0A0A0A', 950: '#050505' }
                        },
                        fontFamily: {
                            hud: ['"Rajdhani"', 'sans-serif'],
                            mono: ['"Fira Code"', 'monospace']
                        }
                    }
                }
            }
        </script>
        <style>
            body { background-color: #050505; color: #a1a1aa; overflow-x: hidden; }
            .hud-panel { 
                background: linear-gradient(145deg, #0a0a0a 0%, #050505 100%);
                border: 1px solid #222;
                box-shadow: inset 0 0 20px rgba(0,0,0,0.5);
                position: relative;
            }
            .hud-panel::before { content: ''; position: absolute; top: -1px; left: -1px; width: 20px; height: 20px; border-top: 1px solid #D4AF37; border-left: 1px solid #D4AF37; opacity: 0.7; }
            .hud-panel::after { content: ''; position: absolute; bottom: -1px; right: -1px; width: 20px; height: 20px; border-bottom: 1px solid #D4AF37; border-right: 1px solid #D4AF37; opacity: 0.7; }
            .gold-glow { text-shadow: 0 0 10px rgba(212, 175, 55, 0.4); }
            .border-glow { box-shadow: 0 0 15px rgba(212, 175, 55, 0.15); border-color: rgba(212, 175, 55, 0.4); }
            .radar-line { width: 100%; height: 1px; background: linear-gradient(90deg, transparent, rgba(212,175,55,0.3), transparent); position: absolute; top: 0; animation: scan 4s linear infinite; pointer-events: none;}
            @keyframes scan { 0% { top: 0; opacity: 0; } 10% { opacity: 1; } 90% { opacity: 1; } 100% { top: 100%; opacity: 0; } }
            ::-webkit-scrollbar { width: 4px; }
            ::-webkit-scrollbar-track { background: #050505; }
            ::-webkit-scrollbar-thumb { background: #333; }
            ::-webkit-scrollbar-thumb:hover { background: #D4AF37; }
            .micro-data { font-size: 0.55rem; letter-spacing: 0.1em; color: #555; text-transform: uppercase; }
            
            /* TABS CSS */
            .tab-btn { transition: all 0.3s ease; }
            .tab-active { color: #D4AF37; border-bottom: 2px solid #D4AF37; text-shadow: 0 0 8px rgba(212, 175, 55, 0.4); }
            .tab-inactive { color: #555; border-bottom: 2px solid transparent; }
            .tab-inactive:hover { color: #888; border-color: #333; }
        </style>
    </head>
    <body class="p-4 md:p-6 min-h-screen relative flex flex-col font-mono selection:bg-gold-500 selection:text-black">
        <div class="fixed inset-0 z-0 pointer-events-none opacity-20" style="background-image: radial-gradient(#333 1px, transparent 1px); background-size: 30px 30px;"></div>
        <div class="radar-line z-0"></div>

        <div class="max-w-7xl mx-auto w-full relative z-10 flex-grow flex flex-col">
            <header class="flex flex-col md:flex-row justify-between items-end mb-6 pb-4 border-b border-dark-800">
                <div class="flex items-center gap-4 w-full md:w-auto mb-4 md:mb-0">
                    <div class="relative w-12 h-12 flex items-center justify-center border border-gold-500/40 bg-gold-900/20">
                        <i class="fa-solid fa-layer-group text-2xl text-gold-500 animate-pulse"></i>
                        <div class="absolute -top-1 -right-1 w-2 h-2 bg-gold-500"></div>
                    </div>
                    <div>
                        <h1 class="text-3xl font-bold text-white tracking-widest font-hud gold-glow uppercase">NEXUS<span class="text-gold-500 font-light">_CORE</span></h1>
                        <div class="flex gap-3 text-[10px] tracking-widest text-gold-600 font-hud mt-1">
                            <span>SYS_ID: OP-77X</span> <span>|</span> <span>NODE: RAILWAY_PRD</span>
                        </div>
                    </div>
                </div>

                <div class="flex flex-col items-end gap-2 w-full md:w-auto">
                    <div class="flex items-center gap-4 font-hud text-xs tracking-widest">
                        <div id="badge-cola" class="flex items-center gap-2 border border-dark-800 bg-dark-900 px-3 py-1 transition-all duration-300">
                            <i class="fa-solid fa-server text-gray-500" id="icon-cola"></i>
                            <span id="txt-cola" class="text-gray-500">QUEUE: 00</span>
                        </div>
                        <div class="flex items-center gap-2 border border-gold-500/30 bg-gold-900/10 px-3 py-1">
                            <span class="relative flex h-2 w-2">
                                <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-gold-400 opacity-75"></span>
                                <span class="relative inline-flex rounded-full h-2 w-2 bg-gold-500"></span>
                            </span>
                            <span class="text-gold-400 font-semibold">UPLINK STABLE</span>
                        </div>
                    </div>
                    <div class="flex gap-2 text-[9px] text-gray-600 tracking-wider">
                        <span>LATENCY: <span class="text-green-500">12ms</span></span>
                        <span id="txt-actualizacion">SYNC: 00:00:00</span>
                    </div>
                </div>
            </header>
            
            <div id="grid-obreros" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4 mb-6"></div>

            <div class="flex gap-6 mb-2 font-hud tracking-widest uppercase text-sm px-2">
                <button id="btn-tab-telemetry" class="tab-btn tab-active pb-1 flex items-center gap-2" onclick="switchTab('telemetry')">
                    <i class="fa-solid fa-satellite-dish"></i> TELEMETRY_LOG
                </button>
                <button id="btn-tab-ledger" class="tab-btn tab-inactive pb-1 flex items-center gap-2" onclick="switchTab('ledger')">
                    <i class="fa-solid fa-vault"></i> PAYMENT_LEDGER
                </button>
            </div>

            <div class="hud-panel p-4 flex-grow flex flex-col relative overflow-hidden h-[450px]">
                
                <div id="view-telemetry" class="w-full h-full flex flex-col">
                    <div class="flex flex-col sm:flex-row justify-between items-start sm:items-end mb-4 pb-3 border-b border-dark-800 gap-4">
                        <div>
                            <span class="micro-data">MAX_BUFFER: 1000 STRINGS</span>
                        </div>
                        <div class="flex gap-2 w-full sm:w-auto font-hud text-xs">
                            <div class="relative w-full sm:w-64">
                                <div class="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none"><i class="fa-solid fa-magnifying-glass text-gold-600"></i></div>
                                <input type="text" id="input-busqueda-tel" placeholder="QUERY ID / REQ..." class="bg-dark-900 border border-dark-800 text-gray-300 placeholder-gray-700 block w-full pl-8 p-1.5 focus:border-gold-500 outline-none transition-colors">
                            </div>
                            <select id="filtro-tipo-tel" class="bg-dark-900 border border-dark-800 text-gold-500 block w-full sm:w-32 p-1.5 outline-none cursor-pointer font-semibold">
                                <option value="ALL">ALL_EVENTS</option>
                                <option value="ERROR">ERRORS</option>
                                <option value="EXITO">SUCCESS</option>
                                <option value="COLA">QUEUED</option>
                                <option value="INFO">INFO_TRACE</option>
                            </select>
                        </div>
                    </div>
                    <div id="terminal-logs" class="overflow-y-auto flex-grow space-y-1 pr-2 mb-3"></div>
                    <div class="flex justify-between items-center border-t border-dark-800 pt-2 font-hud tracking-widest uppercase">
                        <span id="txt-resultados-tel" class="text-gold-500 text-xs font-bold">MATCHES: 0</span>
                        <div class="flex items-center gap-1">
                            <button id="btn-prev-tel" class="w-8 h-6 bg-dark-900 text-gold-600 border border-dark-800 hover:border-gold-500 disabled:opacity-20 transition-all"><i class="fa-solid fa-caret-left"></i></button>
                            <span id="txt-paginacion-tel" class="px-4 text-xs font-bold text-white bg-black border border-dark-800 h-6 flex items-center">1 / 1</span>
                            <button id="btn-next-tel" class="w-8 h-6 bg-dark-900 text-gold-600 border border-dark-800 hover:border-gold-500 disabled:opacity-20 transition-all"><i class="fa-solid fa-caret-right"></i></button>
                        </div>
                    </div>
                </div>

                <div id="view-ledger" class="w-full h-full flex flex-col hidden">
                    <div class="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4 border-b border-dark-800 pb-4">
                        <div class="bg-dark-900 border border-dark-800 p-2 text-center">
                            <div class="micro-data mb-1 text-gray-500">TOTAL PAGOS REGISTRADOS</div>
                            <div class="text-2xl font-hud font-bold text-white" id="stat-total">00</div>
                        </div>
                        <div class="bg-dark-900 border border-sky-900/30 p-2 text-center relative overflow-hidden">
                            <div class="absolute -right-2 -top-2 text-sky-500/10 text-4xl"><i class="fa-solid fa-globe"></i></div>
                            <div class="micro-data mb-1 text-sky-500">PAGOS VIDANET</div>
                            <div class="text-2xl font-hud font-bold text-sky-400" id="stat-vidanet">00</div>
                        </div>
                        <div class="bg-dark-900 border border-indigo-900/30 p-2 text-center relative overflow-hidden">
                            <div class="absolute -right-2 -top-2 text-indigo-500/10 text-4xl"><i class="fa-solid fa-building"></i></div>
                            <div class="micro-data mb-1 text-indigo-500">PAGOS ICAROSOFT</div>
                            <div class="text-2xl font-hud font-bold text-indigo-400" id="stat-icaro">00</div>
                        </div>
                        <div class="bg-dark-900 border border-dark-800 p-2 text-center">
                            <div class="micro-data mb-1 text-gray-500">TIEMPO PROMEDIO (AVG)</div>
                            <div class="text-2xl font-hud font-bold text-green-500" id="stat-avg">0.0s</div>
                        </div>
                    </div>

                    <div class="flex flex-col sm:flex-row justify-between items-start sm:items-end mb-4 gap-4">
                        <span class="font-hud tracking-widest text-gold-500 text-sm"><i class="fa-solid fa-list-check mr-2"></i>HISTÓRICO RECIENTE</span>
                        <div class="flex gap-2 w-full sm:w-auto font-hud text-xs">
                            <div class="relative w-full sm:w-64">
                                <div class="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none"><i class="fa-solid fa-magnifying-glass text-gold-600"></i></div>
                                <input type="text" id="input-busqueda-led" placeholder="CÉDULA / REQ ID..." class="bg-dark-900 border border-dark-800 text-gray-300 placeholder-gray-700 block w-full pl-8 p-1.5 focus:border-gold-500 outline-none">
                            </div>
                            <select id="filtro-tipo-led" class="bg-dark-900 border border-dark-800 text-gold-500 block w-full sm:w-32 p-1.5 outline-none cursor-pointer font-semibold">
                                <option value="ALL">AMBOS SISTEMAS</option>
                                <option value="VIDANET">SOLO VIDANET</option>
                                <option value="ICAROSOFT">SOLO ICAROSOFT</option>
                            </select>
                        </div>
                    </div>

                    <div id="ledger-logs" class="overflow-y-auto flex-grow space-y-2 pr-2 mb-3"></div>

                    <div class="flex justify-between items-center border-t border-dark-800 pt-2 font-hud tracking-widest uppercase">
                        <span id="txt-resultados-led" class="text-gold-500 text-xs font-bold">MATCHES: 0</span>
                        <div class="flex items-center gap-1">
                            <button id="btn-prev-led" class="w-8 h-6 bg-dark-900 text-gold-600 border border-dark-800 hover:border-gold-500 disabled:opacity-20 transition-all"><i class="fa-solid fa-caret-left"></i></button>
                            <span id="txt-paginacion-led" class="px-4 text-xs font-bold text-white bg-black border border-dark-800 h-6 flex items-center">1 / 1</span>
                            <button id="btn-next-led" class="w-8 h-6 bg-dark-900 text-gold-600 border border-dark-800 hover:border-gold-500 disabled:opacity-20 transition-all"><i class="fa-solid fa-caret-right"></i></button>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <script>
            // --- ESTADO LOCAL DEL DASHBOARD ---
            let historialGlobal = [];
            let pagosGlobal = [];
            let vistaActual = 'telemetry';
            
            let pagTel = 1, pagLed = 1;
            const LIMIT_TEL = 50, LIMIT_LED = 20;

            const grid = document.getElementById('grid-obreros');
            const terminal = document.getElementById('terminal-logs');
            const ledger = document.getElementById('ledger-logs');
            const txtActualizacion = document.getElementById('txt-actualizacion');
            const badgeCola = document.getElementById('badge-cola');
            const txtCola = document.getElementById('txt-cola');
            const iconCola = document.getElementById('icon-cola');

            // Listeners Pestañas
            function switchTab(tab) {
                vistaActual = tab;
                document.getElementById('view-telemetry').classList.toggle('hidden', tab !== 'telemetry');
                document.getElementById('view-ledger').classList.toggle('hidden', tab !== 'ledger');
                
                const btnTel = document.getElementById('btn-tab-telemetry');
                const btnLed = document.getElementById('btn-tab-ledger');
                
                if(tab === 'telemetry') {
                    btnTel.className = "tab-btn tab-active pb-1 flex items-center gap-2";
                    btnLed.className = "tab-btn tab-inactive pb-1 flex items-center gap-2";
                } else {
                    btnTel.className = "tab-btn tab-inactive pb-1 flex items-center gap-2";
                    btnLed.className = "tab-btn tab-active pb-1 flex items-center gap-2";
                }
                renderizarVistas();
            }

            // Listeners Telemetría
            document.getElementById('input-busqueda-tel').addEventListener('input', () => { pagTel = 1; renderizarTelemetria(); });
            document.getElementById('filtro-tipo-tel').addEventListener('change', () => { pagTel = 1; renderizarTelemetria(); });
            document.getElementById('btn-prev-tel').addEventListener('click', () => { if(pagTel > 1) { pagTel--; renderizarTelemetria(); } });
            document.getElementById('btn-next-tel').addEventListener('click', () => { pagTel++; renderizarTelemetria(); });

            // Listeners Ledger
            document.getElementById('input-busqueda-led').addEventListener('input', () => { pagLed = 1; renderizarLedger(); });
            document.getElementById('filtro-tipo-led').addEventListener('change', () => { pagLed = 1; renderizarLedger(); });
            document.getElementById('btn-prev-led').addEventListener('click', () => { if(pagLed > 1) { pagLed--; renderizarLedger(); } });
            document.getElementById('btn-next-led').addEventListener('click', () => { pagLed++; renderizarLedger(); });

            const formatearHora = (ms, formatoCompleto = false) => {
                const d = new Date(ms);
                if (formatoCompleto) {
                    const meses = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
                    return \`\${d.getDate().toString().padStart(2,'0')} \${meses[d.getMonth()]} \${d.getFullYear()} | \${d.toLocaleTimeString('en-US', { hour12: false })}\`;
                }
                return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute:'2-digit', second:'2-digit' }) + '.' + String(d.getMilliseconds()).padStart(3, '0');
            };

            async function escanearEscuadron() {
                try {
                    const respuesta = await fetch('/api/tactico/estado');
                    const data = await respuesta.json(); 
                    
                    renderizarObreros(data.obreros);
                    historialGlobal = data.historial;
                    pagosGlobal = data.pagos || [];
                    renderizarVistas();
                    
                    // UI Cola
                    txtCola.innerText = 'QUEUE: ' + data.encolados.toString().padStart(2, '0');
                    if (data.encolados > 0) {
                        badgeCola.className = 'flex items-center gap-2 border border-gold-500/50 bg-gold-900/20 px-3 py-1 transition-all duration-300 border-glow';
                        iconCola.className = 'fa-solid fa-network-wired text-gold-400 animate-pulse';
                        txtCola.className = 'text-gold-400 font-bold';
                    } else {
                        badgeCola.className = 'flex items-center gap-2 border border-dark-800 bg-dark-900 px-3 py-1 transition-all duration-300';
                        iconCola.className = 'fa-solid fa-server text-gray-600';
                        txtCola.className = 'text-gray-600';
                    }
                    
                    const ahora = new Date();
                    txtActualizacion.innerText = \`SYNC: \${ahora.toLocaleTimeString('en-US', {hour12: false})}\`;
                    txtActualizacion.classList.replace('text-red-500', 'text-gray-600');
                } catch (error) {}
            }

            function renderizarVistas() {
                if(vistaActual === 'telemetry') renderizarTelemetria();
                else renderizarLedger();
            }

            function renderizarObreros(obreros) {
                const tiempoActual = Date.now();
                let htmlTemp = '';
                obreros.forEach(o => {
                    const isVivo = o.activo;
                    const isCocinando = o.cocinandoHasta > tiempoActual;
                    const segCoccion = isCocinando ? Math.ceil((o.cocinandoHasta - tiempoActual) / 1000) : 0;
                    const bordeEstado = isVivo ? 'border-dark-800 hover:border-gold-500/30' : 'border-red-900/50 border-glow';
                    
                    let statusIcon = isVivo ? \`<i class="fa-solid fa-check text-green-500 text-[10px]"></i> <span class="text-green-500">OPR_RDY</span>\` : \`<i class="fa-solid fa-skull text-red-500 text-[10px] animate-pulse"></i> <span class="text-red-500">QUARANTINE</span>\`;
                    let opsBadges = '';
                    if (isCocinando) opsBadges += \`<div class="border border-amber-500/30 bg-amber-900/10 text-amber-500 px-1.5 py-0.5"><i class="fa-solid fa-fire animate-pulse mr-1"></i>\${segCoccion}s</div>\`;
                    if (o.buscandoServicios) opsBadges += \`<div class="border border-sky-500/30 bg-sky-900/10 text-sky-400 px-1.5 py-0.5"><i class="fa-solid fa-satellite-dish animate-pulse mr-1"></i>SRV</div>\`;

                    const porcentajeCarga = Math.min((o.carga / 10) * 100, 100);
                    let colorBarra = o.carga > 5 ? 'bg-red-500' : (o.carga > 2 ? 'bg-amber-500' : 'bg-gold-500');

                    htmlTemp += \`
                    <div class="hud-panel p-3 border \${bordeEstado} transition-colors flex flex-col justify-between min-h-[140px]">
                        <div class="flex justify-between items-start mb-2">
                            <div>
                                <h2 class="text-lg font-bold text-gray-200 font-hud tracking-widest leading-none">WK_0\${o.id}</h2>
                                <div class="font-hud tracking-widest text-[9px] mt-1 \${isVivo ? '' : 'gold-glow'}">\${statusIcon}</div>
                            </div>
                            <div class="text-right">
                                <div class="text-2xl font-light font-hud leading-none \${o.fallos > 0 ? 'text-red-500' : 'text-gray-700'}">\${o.fallos.toString().padStart(2, '0')}</div>
                                <div class="micro-data">ERRORS</div>
                            </div>
                        </div>
                        <div class="flex gap-1 mb-2 text-[8px] font-hud tracking-widest uppercase h-4">\${opsBadges}</div>
                        <div class="mb-3">
                            <div class="flex justify-between font-hud text-[9px] tracking-widest text-gray-500 mb-1">
                                <span>LOAD_BAL</span><span class="text-gray-300">\${o.carga} REQ</span>
                            </div>
                            <div class="w-full bg-dark-900 border border-dark-800 h-1">
                                <div class="\${colorBarra} h-full transition-all duration-500" style="width: \${porcentajeCarga}%"></div>
                            </div>
                        </div>
                        <div class="grid grid-cols-2 gap-2 mt-auto font-hud text-[10px] tracking-widest uppercase">
                            <button onclick="ejecutarOrden(\${o.id})" class="bg-dark-900 text-gray-500 border border-dark-800 hover:border-red-500 hover:text-red-500 py-1 transition-colors group">
                                <i class="fa-solid fa-radiation group-hover:animate-spin"></i> PURGE
                            </button>
                            <button onclick="revivir(\${o.id})" class="bg-dark-900 text-gray-500 border border-dark-800 hover:border-gold-500 hover:text-gold-400 py-1 transition-colors">
                                <i class="fa-solid fa-bolt"></i> FORCE_UP
                            </button>
                        </div>
                    </div>\`;
                });
                grid.innerHTML = htmlTemp;
            }

            function renderizarTelemetria() {
                const busqueda = document.getElementById('input-busqueda-tel').value.toLowerCase().trim();
                const tipoFiltrado = document.getElementById('filtro-tipo-tel').value;

                let logsFiltrados = historialGlobal.filter(log => {
                    const coincideTipo = tipoFiltrado === 'ALL' || log.tipo === tipoFiltrado;
                    const textoCompleto = \`\${log.reqId} \${log.mensaje} \${log.obreroId}\`.toLowerCase();
                    return coincideTipo && (busqueda === '' || textoCompleto.includes(busqueda));
                });

                const totalPaginas = Math.max(1, Math.ceil(logsFiltrados.length / LIMIT_TEL));
                if (pagTel > totalPaginas) pagTel = totalPaginas;

                document.getElementById('txt-paginacion-tel').innerText = \`\${pagTel.toString().padStart(2, '0')} / \${totalPaginas.toString().padStart(2, '0')}\`;
                document.getElementById('txt-resultados-tel').innerText = \`MATCHES: \${logsFiltrados.length.toString().padStart(4, '0')}\`;
                document.getElementById('btn-prev-tel').disabled = pagTel === 1;
                document.getElementById('btn-next-tel').disabled = pagTel === totalPaginas;

                let htmlTemp = '';
                if(logsFiltrados.length === 0) {
                    terminal.innerHTML = '<div class="text-gray-700 italic mt-4 text-center font-hud tracking-widest text-sm">NO_DATA_FOUND // AWAITING_INPUT</div>';
                    return;
                }

                logsFiltrados.slice((pagTel - 1) * LIMIT_TEL, pagTel * LIMIT_TEL).forEach(log => {
                    let colorBase = 'text-gray-400';
                    let bgBadge = 'text-gray-500';
                    let iconClass = 'fa-solid fa-microchip';

                    if (log.tipo === 'NUEVA') { bgBadge = 'text-sky-400'; iconClass = 'fa-solid fa-arrow-right-to-bracket'; }
                    if (log.tipo === 'EXITO') { bgBadge = 'text-green-500'; iconClass = 'fa-solid fa-check-double'; colorBase = 'text-gray-300'; }
                    if (log.tipo === 'ERROR') { bgBadge = 'text-red-500'; iconClass = 'fa-solid fa-triangle-exclamation'; colorBase = 'text-red-400'; }
                    if (log.tipo === 'ALERTA') { bgBadge = 'text-orange-500 animate-pulse'; iconClass = 'fa-solid fa-radiation'; colorBase = 'text-orange-400'; }
                    if (log.tipo === 'COLA') { bgBadge = 'text-indigo-400'; iconClass = 'fa-solid fa-layer-group'; colorBase = 'text-indigo-300'; }
                    if (log.tipo === 'INFO') { bgBadge = 'text-gold-500'; iconClass = 'fa-solid fa-terminal'; colorBase = 'text-gold-100'; }

                    const duracionStr = log.duracion ? \`<span class="text-gray-600 ml-2">[\${log.duracion}ms]</span>\` : '';
                    const obreroTag = log.obreroId !== 'SYS' ? \`<span class="text-gold-500 ml-2 border border-gold-500/20 px-1">WK_\${log.obreroId.toString().padStart(2,'0')}</span>\` : '<span class="text-gray-600 ml-2 border border-dark-800 px-1">SYS</span>';

                    htmlTemp += \`
                    <div class="flex items-start gap-3 p-1 hover:bg-dark-800/50 transition-colors border-l-2 border-transparent hover:border-gold-500">
                        <div class="text-gray-600 w-24 shrink-0 mt-0.5 text-[10px] tracking-wider">\${formatearHora(log.tiempo)}</div>
                        <div class="\${bgBadge} w-4 shrink-0 text-center mt-0.5"><i class="\${iconClass}"></i></div>
                        <div class="text-gray-500 w-14 shrink-0 text-center mt-0.5 tracking-wider text-[10px] border border-dark-800 bg-dark-900">\${log.reqId}</div>
                        <div class="\${colorBase} break-all flex-grow leading-snug text-[11px]">\${log.mensaje} \${obreroTag}\${duracionStr}</div>
                    </div>\`;
                });
                terminal.innerHTML = htmlTemp;
            }

            function renderizarLedger() {
                const busqueda = document.getElementById('input-busqueda-led').value.toLowerCase().trim();
                const tipoFiltrado = document.getElementById('filtro-tipo-led').value;

                // Actualizar Stats
                let cVidanet = 0, cIcaro = 0, sumTime = 0;
                pagosGlobal.forEach(p => {
                    if(p.sistema === 'VIDANET') cVidanet++; else cIcaro++;
                    if(p.duracion) sumTime += p.duracion;
                });
                const totalP = pagosGlobal.length;
                document.getElementById('stat-total').innerText = totalP.toString().padStart(2, '0');
                document.getElementById('stat-vidanet').innerText = cVidanet.toString().padStart(2, '0');
                document.getElementById('stat-icaro').innerText = cIcaro.toString().padStart(2, '0');
                document.getElementById('stat-avg').innerText = totalP > 0 ? (sumTime / totalP / 1000).toFixed(1) + 's' : '0.0s';

                let filtrados = pagosGlobal.filter(p => {
                    const coincideTipo = tipoFiltrado === 'ALL' || p.sistema === tipoFiltrado;
                    const textoCompleto = \`\${p.cliente} \${p.reqId}\`.toLowerCase();
                    return coincideTipo && (busqueda === '' || textoCompleto.includes(busqueda));
                });

                const totalPaginas = Math.max(1, Math.ceil(filtrados.length / LIMIT_LED));
                if (pagLed > totalPaginas) pagLed = totalPaginas;

                document.getElementById('txt-paginacion-led').innerText = \`\${pagLed.toString().padStart(2, '0')} / \${totalPaginas.toString().padStart(2, '0')}\`;
                document.getElementById('txt-resultados-led').innerText = \`MATCHES: \${filtrados.length.toString().padStart(4, '0')}\`;
                document.getElementById('btn-prev-led').disabled = pagLed === 1;
                document.getElementById('btn-next-led').disabled = pagLed === totalPaginas;

                let htmlTemp = '';
                if(filtrados.length === 0) {
                    ledger.innerHTML = '<div class="text-gray-700 italic mt-8 text-center font-hud tracking-widest text-sm">NO_PAYMENTS_REGISTERED</div>';
                    return;
                }

                filtrados.slice((pagLed - 1) * LIMIT_LED, pagLed * LIMIT_LED).forEach(p => {
                    const esVidanet = p.sistema === 'VIDANET';
                    const colorSis = esVidanet ? 'text-sky-400' : 'text-indigo-400';
                    const bgSis = esVidanet ? 'bg-sky-900/20 border-sky-400/30' : 'bg-indigo-900/20 border-indigo-400/30';
                    const segs = p.duracion ? (p.duracion / 1000).toFixed(1) + 's' : 'N/A';

                    htmlTemp += \`
                    <div class="flex items-center justify-between border border-dark-800 bg-dark-900/50 p-2.5 hover:border-gold-500/50 transition-colors">
                        <div class="flex items-center gap-3">
                            <div class="w-8 h-8 rounded-full bg-green-900/20 border border-green-500/30 flex items-center justify-center">
                                <i class="fa-solid fa-check text-green-500"></i>
                            </div>
                            <div>
                                <div class="text-gold-400 font-bold tracking-widest text-sm">\${p.cliente}</div>
                                <div class="micro-data text-gray-500 mt-0.5">\${formatearHora(p.tiempo, true)} | REQ: \${p.reqId}</div>
                            </div>
                        </div>
                        <div class="text-right">
                            <div class="\${colorSis} text-[10px] font-bold border \${bgSis} px-1.5 py-0.5 rounded-sm inline-block">\${p.sistema}</div>
                            <div class="micro-data text-gray-500 mt-1 font-mono">TIME: <span class="text-gray-300">\${segs}</span></div>
                        </div>
                    </div>\`;
                });
                ledger.innerHTML = htmlTemp;
            }

            async function ejecutarOrden(id) {
                if(!confirm(\`WARNING: INIT PURGE PROTOCOL ON WORKER_\${id}? (Destroys container)\`)) return;
                try { await fetch(\`/api/tactico/estado\`); await fetch(\`/api/tactico/orden66/\${id}\`, { method: 'POST' }); } catch(e) {}
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

            // --- INYECCIÓN DEL REGISTRO PARA LOS JEFES ---
            if (esRutaPago) {
                PAGOS_EXITOSOS.unshift({
                    reqId: requestId,
                    tiempo: Date.now(),
                    cliente: idCliente || 'DESCONOCIDO',
                    sistema: req.path === '/pagar' ? 'ICAROSOFT' : 'VIDANET',
                    duracion: duracion
                });
                if (PAGOS_EXITOSOS.length > MAX_PAGOS) PAGOS_EXITOSOS.pop();
            }
            // ---------------------------------------------

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
