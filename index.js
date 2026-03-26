const express = require('express');
const axios = require('axios');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js'); // <-- EL CAÑÓN

const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' })); // Aumentamos límite por las imágenes Base64 de Icaro
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// ============================================================================
// 🚀 NÚCLEO DE DATOS: SUPABASE Y PERRO RASTREADOR
// ============================================================================
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

async function inyectarPagoEnSupabase(reqPath, reqBody, idCliente, logFunc) {
    let documentoFinal = idCliente;
    let payload = {};

    try {
        if (reqPath === '/pagar') {
            // 🔍 PERRO RASTREADOR: Icaro manda solo números. Buscamos a qué letra pertenece.
            const { data, error } = await supabase
                .from('clientes')
                .select('documento_cliente')
                .ilike('documento_cliente', `%${idCliente}`)
                .limit(1)
                .single();

            if (data && data.documento_cliente) {
                documentoFinal = data.documento_cliente;
            } else {
                // Fallback de titanio: Si el cliente no existe aún, forzamos 'V' para no romper
                documentoFinal = `V${idCliente}`; 
            }

            const d = reqBody.datos || {};
            
            // 🎯 ESTRUCTURA UNIFICADA: ICAROSOFT
            payload = {
                documento_cliente: documentoFinal.toUpperCase().replace(/[^A-Z0-9]/g, ''),
                metodo_pago: d.tipoPago || 'NO_ESPECIFICADO',    // <-- Extracción de tu JSON
                banco_origen: d.formaPago || 'NO_ESPECIFICADO',  // <-- Extracción de tu JSON
                referencia: d.referencia || 'SIN_REF',
                monto_bs: parseFloat(d.monto) || null,           // Si no hay monto, declaramos null
                fecha_pago: d.fecha || null,
                url_comprobante: d.rutaImagen || null,           
                direccion_reportada: d.direccion || null,
                id_deuda_pagada: null,                           // Icarosoft no envía el ID de la factura
                origen_reporte: 'ICAROSOFT',
                estado: 'REGISTRADO'                             // Marcador inicial
            };

        } else if (reqPath === '/pagar-vidanet') {
            // 🧩 VIDANET: Armamos la cédula uniendo letra y número
            const d = reqBody.datos || {};
            documentoFinal = `${d.letra || 'V'}${d.cedula || idCliente}`;

            // ⏱️ GENERADOR AUTÓNOMO DE FECHA (Hora Caracas)
            const hoy = new Date();
            const dia = hoy.getDate().toString().padStart(2, '0');
            const mes = (hoy.getMonth() + 1).toString().padStart(2, '0');
            const anio = hoy.getFullYear();
            const fechaGenerada = `${dia}/${mes}/${anio}`;

            // 🎯 ESTRUCTURA UNIFICADA: VIDANET
            payload = {
                documento_cliente: documentoFinal.toUpperCase().replace(/[^A-Z0-9]/g, ''),
                metodo_pago: 'Portal Vidanet',
                banco_origen: d.banco || 'NO_ESPECIFICADO',
                referencia: d.referencia || 'SIN_REF',
                monto_bs: null,                                  // <-- El fantasma táctico (no más ceros)
                fecha_pago: fechaGenerada,      
                url_comprobante: null,                           // Vidanet no envía foto
                direccion_reportada: null,                       // Vidanet no envía dirección
                id_deuda_pagada: d.id_deuda || null,             // NULL obligatorio si no existe
                origen_reporte: 'VIDANET',
                estado: 'FACTURADO'                              // Vidanet nace aprobado
            };
        }

        // 💥 DISPARO A LA BASE DE DATOS
        const { error: errInsert } = await supabase.from('registro_pagos').insert([payload]);
        
        if (errInsert) {
            logFunc('ERROR', `Fallo al inyectar pago en Supabase: ${errInsert.message}`);
        } else {
            logFunc('EXITO', `Respaldado en Supabase (Ref: ${payload.referencia})`);
        }
    } catch (error) {
        logFunc('ERROR', `Excepción en Perro Rastreador: ${error.message}`);
    }
}

// ============================================================================
// 1. MEMORIA Y ESTADO (LA ARMADURA Y LA COLA)
// ============================================================================

const NOMBRE_PROYECTO = 'mindful-quietude';
const ENV_PROD_ID = 'b154738b-4e07-42c5-85d5-fc0077cf0c61';

const OBREROS = [
    { id: 1, url: 'https://obrero-2-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false, rwServiceId: '4c16d6ce-8b1d-4d1b-a1b5-a886e11ce6ea', rwEnvId: ENV_PROD_ID, rwProjectName: NOMBRE_PROYECTO },
    { id: 2, url: 'https://obrero-4-2-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false, rwServiceId: '7cf96869-4def-4f0d-a69b-94cc44aefb88', rwEnvId: ENV_PROD_ID, rwProjectName: NOMBRE_PROYECTO },
    { id: 3, url: 'https://obrero-5-3-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false, rwServiceId: 'a340646a-9c71-4a89-8bae-6c59ff9864ae', rwEnvId: ENV_PROD_ID, rwProjectName: NOMBRE_PROYECTO },
    { id: 4, url: 'https://obrero-6-4-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false, rwServiceId: '9be77316-1d28-4d0b-ad80-621c4edd13a3', rwEnvId: ENV_PROD_ID, rwProjectName: NOMBRE_PROYECTO },
    { id: 5, url: 'https://obrero-7-5-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false, rwServiceId: '7adc6cb6-ffe7-42a2-9ee9-22171b5266b6', rwEnvId: ENV_PROD_ID, rwProjectName: NOMBRE_PROYECTO }
];

const MAX_LOGS = 15000;
let HISTORIAL = [];

// --- 🛸 ESCUADRÓN FANTASMA (CRONOS) ---
const ESCUADRON_CRONOS = [
    { nombre: 'SERVICIOS', url: 'https://servicios-production-9681.up.railway.app', serviceId: '9c7e701b-6f56-4b76-a6c7-4bc4e3ac7f3d', fallos: 0, ignorarHasta: 0 },
    { nombre: 'FACTURAS', url: 'https://facturas-production-2ab1.up.railway.app', serviceId: 'a0bcb1ce-b45c-40e4-b2f1-367e03ca925b', fallos: 0, ignorarHasta: 0 }
];

// --- NUEVA BÓVEDA EXCLUSIVA PARA LOS JEFES (PAGOS EXITOSOS) ---
const MAX_PAGOS = 2000;
let PAGOS_EXITOSOS = [];

// --- SISTEMA DE ENCOLAMIENTO ---
const COLA_DE_ESPERA = [];
const TIMEOUT_SALA_ESPERA = 45000; // 45 segundos

// --- SISTEMA DE CACHÉ EN MEMORIA ---
const CACHE_SERVICIOS = new Map();
const TIEMPO_CACHE = 5 * 60 * 1000; // 5 minutos en milisegundos

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

// --- SISTEMA DE PERRO GUARDIÁN (WATCHDOG & WEBHOOKS) ---
const PAGOS_EN_VUELO = new Map();

const esperarWebhook = (reqId, timeoutMs) => {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            PAGOS_EN_VUELO.delete(reqId);
            reject(new Error("WATCHDOG_TIMEOUT"));
        }, timeoutMs);
        PAGOS_EN_VUELO.set(reqId, { resolve, timer });
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

// --- NUEVO: RECEPTOR DE RADIO DE LOS OBREROS ---
app.post('/api/tactico/webhook', (req, res) => {
    const { reqId, exito, mensaje, sistema } = req.body;
    res.status(200).send("Recibido base");
    
    if (PAGOS_EN_VUELO.has(reqId)) {
        const mision = PAGOS_EN_VUELO.get(reqId);
        clearTimeout(mision.timer); // Apagamos el Perro Guardián
        PAGOS_EN_VUELO.delete(reqId);
        mision.resolve({ exito, mensaje, sistema }); // Despertamos al Comandante
    }
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

// --- EL DISPARO DE RESURRECCIÓN (API RAILWAY GRAPHQL) ---
async function reiniciarObreroDesdeRailway(obrero) {
    const token = process.env.RAILWAY_API_TOKEN; 
    
    if (!token) {
        agregarLog('SYS', 'ERROR', `Falta RAILWAY_API_TOKEN en el .env. Imposible hacer redeploy del Obrero ${obrero.id}.`);
        return;
    }

    agregarLog('SYS', 'INFO', `Enviando orden suprema de REDEPLOY a la API de Railway para el Obrero ${obrero.id}...`);

    try {
        const queryGraphQL = `
            mutation serviceInstanceRedeploy($serviceId: String!, $environmentId: String!) {
                serviceInstanceRedeploy(serviceId: $serviceId, environmentId: $environmentId)
            }
        `;

        const respuesta = await axios.post('https://backboard.railway.com/graphql/v2', {
            query: queryGraphQL,
            variables: {
                serviceId: obrero.rwServiceId,
                environmentId: obrero.rwEnvId
            }
        }, {
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            }
        });

        if (respuesta.data.errors) {
            agregarLog('SYS', 'ERROR', `Railway rechazó el redeploy: ${respuesta.data.errors[0].message}`);
        } else {
            agregarLog('SYS', 'INFO', `Redeploy disparado con éxito en la infraestructura para el Obrero ${obrero.id}. Esperando confirmación de la nube...`);
        }
    } catch (error) {
        agregarLog('SYS', 'ERROR', `Fallo de comunicación HTTP con la API de Railway: ${error.message}`);
    }
}

// --- ⚡ EL DISPARO DE RESURRECCIÓN PARA CRONOS ---
async function reiniciarCronosDesdeRailway(cronos) {
    
    // ⚠️ CRÍTICO: Usamos el token exclusivo del proyecto Cronos, no el de los Obreros
    const token = process.env.RAILWAY_API_TOKEN_CRONOS; 
    
    if (!token) return agregarLog('SYS', 'ERROR', `Falta RAILWAY_API_TOKEN_CRONOS. Imposible revivir a CRONOS ${cronos.nombre}.`);

    agregarLog('SYS', 'ALERTA', `☠️ Disparando misil de REDEPLOY a la API de Railway para CRONOS ${cronos.nombre}...`);

    try {
        // El GraphQL exacto adaptado de tu cURL
        const queryGraphQL = `mutation { deployService(input: {serviceId: "${cronos.serviceId}"}) { id status } }`;
        
        const respuesta = await axios.post('https://api.railway.app/graphql', { 
            query: queryGraphQL 
        }, {
            headers: { 
                'Authorization': `Bearer ${token}`, 
                'Content-Type': 'application/json' 
            }
        });

        if (respuesta.data && respuesta.data.errors) {
            agregarLog('SYS', 'ERROR', `Railway rechazó el redeploy de CRONOS: ${respuesta.data.errors[0].message}`);
        } else {
            agregarLog('SYS', 'EXITO', `🔥 REDEPLOY DISPARADO para CRONOS ${cronos.nombre}. Entrando en gracia de 4 minutos para que despierte...`);
            // 🛑 EL PERIODO DE GRACIA: Ignoramos su pulso por 4 minutos mientras Railway reconstruye y levanta
            cronos.ignorarHasta = Date.now() + (4 * 60 * 1000); 
        }
    } catch (error) {
        agregarLog('SYS', 'ERROR', `Fallo al contactar API de Railway para CRONOS: ${error.message}`);
    }
}

// --- ADUANA DE INFRAESTRUCTURA (RAILWAY WEBHOOKS) ---
app.post('/api/tactico/railway-webhook', async (req, res) => {
    res.status(200).send("Recibido");

    const payload = req.body;
    const tipo = payload.type;
    const status = payload.details ? payload.details.status : '';
    
    // 1. Ignorar mensajes que no sean de nuestro proyecto
    if (!payload.resource || !payload.resource.project || payload.resource.project.name !== NOMBRE_PROYECTO) return;

    // 2. Identificar el obrero usando la ruta exacta del JSON que viste
    const serviceId = payload.resource.service ? payload.resource.service.id : null;
    const obrero = serviceId ? OBREROS.find(o => o.rwServiceId === serviceId) : null;

    // --- CASO A: MUERTE (CRASH) ---
    if (tipo === 'Deployment.crashed') {
        agregarLog('SYS', 'ALERTA', `🚨 RAILWAY CRASH DETECTADO. Iniciando pase de lista...`);
        for (let o of OBREROS) {
            if (!o.activo) continue; 
            try { await axios.get(`${o.url}/`, { timeout: 3500 }); } 
            catch (error) {
                if (error.response && error.response.status === 502) {
                    agregarLog('SYS', 'ERROR', `🎯 CADÁVER CONFIRMADO: Obrero ${o.id} (502). Redeploying...`);
                    o.activo = false;
                    reiniciarObreroDesdeRailway(o);
                }
            }
        }
    } 
    
    // --- CASO B: RESURRECCIÓN (DEPLOYED + SUCCESS) ---
    // Filtramos exactamente por el tipo y estatus que mandó tu webhook.site
    else if (tipo === 'Deployment.deployed' && status === 'SUCCESS') {
        if (obrero) {
            agregarLog('SYS', 'EXITO', `🔥 ESCUDO PROTECTOR: Obrero ${obrero.id} desplegado. Calentando motores (40s)...`);
            obrero.activo = true;
            obrero.fallos = 0;
            // Bloqueo de seguridad para que inicie sus 4 navegadores
            obrero.cocinandoHasta = Date.now() + 40000;
            obrero.buscandoServicios = false;
        } else {
            // Fallback: Si no detectamos el serviceId, barremos a los inactivos
            for (let o of OBREROS.filter(obs => !obs.activo)) {
                try {
                    await axios.get(`${o.url}/`, { timeout: 3500 });
                    agregarLog('SYS', 'EXITO', `🔥 ESCUDO PROTECTOR: Obrero ${o.id} resucitado. Calentando (40s)...`);
                    o.activo = true;
                    o.fallos = 0;
                    o.cocinandoHasta = Date.now() + 40000;
                } catch (e) {}
            }
        }
    }
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
                    <div class="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4 border-b border-dark-800 pb-4">
                        <div class="bg-dark-900 border border-dark-800 p-2 text-center">
                            <div class="micro-data mb-1 text-gray-500">TOTAL PAGOS</div>
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
                        <div class="bg-dark-900 border border-red-900/30 p-2 text-center relative overflow-hidden">
                            <div class="absolute -right-2 -top-2 text-red-500/10 text-4xl"><i class="fa-solid fa-triangle-exclamation"></i></div>
                            <div class="micro-data mb-1 text-red-500">RECHAZOS VIDANET</div>
                            <div class="text-2xl font-hud font-bold text-red-500" id="stat-rechazos-vidanet">00</div>
                        </div>
                        <div class="bg-dark-900 border border-dark-800 p-2 text-center">
                            <div class="micro-data mb-1 text-gray-500">TIEMPO MEDIO</div>
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
                let cVidanet = 0, cIcaro = 0, cRechazadosVid = 0, sumTime = 0;
                
                pagosGlobal.forEach(p => {
                    // Lógica segura y precisa
                    if (p.sistema === 'VIDANET') {
                        cVidanet++;
                    } else if (p.sistema === 'ICAROSOFT') {
                        cIcaro++;
                    } else if (p.sistema && p.sistema.includes('VIDANET') && p.sistema.includes('RECHAZADO')) {
                        cRechazadosVid++;
                    }
                    if(p.duracion) sumTime += p.duracion;
                });
                
                // ASIGNACIÓN DOM CORREGIDA (Sin variables duplicadas)
                const totalP = pagosGlobal.length;
                document.getElementById('stat-total').innerText = totalP.toString().padStart(2, '0');
                document.getElementById('stat-vidanet').innerText = cVidanet.toString().padStart(2, '0');
                document.getElementById('stat-icaro').innerText = cIcaro.toString().padStart(2, '0');
                document.getElementById('stat-rechazos-vidanet').innerText = cRechazadosVid.toString().padStart(2, '0');
                document.getElementById('stat-avg').innerText = totalP > 0 ? (sumTime / totalP / 1000).toFixed(1) + 's' : '0.0s';

                let filtrados = pagosGlobal.filter(p => {
                    const coincideTipo = tipoFiltrado === 'ALL' || p.sistema.includes(tipoFiltrado);
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
                    // Lógica visual avanzada para la lista
                    const esVidanet = p.sistema.includes('VIDANET');
                    const esRechazo = p.sistema.includes('RECHAZADO');
                    
                    let colorSis = esVidanet ? 'text-sky-400' : 'text-indigo-400';
                    let bgSis = esVidanet ? 'bg-sky-900/20 border-sky-400/30' : 'bg-indigo-900/20 border-indigo-400/30';
                    let iconStatus = '<i class="fa-solid fa-check text-green-500"></i>';
                    let bgIcon = 'bg-green-900/20 border-green-500/30';

                    // Si es rechazado, lo pintamos de rojo intenso
                    if (esRechazo) {
                        colorSis = 'text-red-400';
                        bgSis = 'bg-red-900/20 border-red-500/50';
                        iconStatus = '<i class="fa-solid fa-xmark text-red-500"></i>';
                        bgIcon = 'bg-red-900/20 border-red-500/50';
                    }

                    const segs = p.duracion ? (p.duracion / 1000).toFixed(1) + 's' : 'N/A';

                    htmlTemp += \`
                    <div class="flex items-center justify-between border border-dark-800 bg-dark-900/50 p-2.5 hover:border-gold-500/50 transition-colors">
                        <div class="flex items-center gap-3">
                            <div class="w-8 h-8 rounded-full \${bgIcon} flex items-center justify-center">
                                \${iconStatus}
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
// 3.5. ENDPOINTS DE ALTA VELOCIDAD (SUPABASE DIRECTO)
// ============================================================================

// ⚡ 1. BÚSQUEDA DE SERVICIOS (Para la App y para Vivian)
app.get('/buscar-servicios', async (req, res) => {
    const inicioReloj = Date.now();
    const idBusqueda = req.query.id || req.query.cedula;
    const origen = req.query.origen; 
    const reqId = Math.random().toString(36).substring(2, 7).toUpperCase();
    
    if (!idBusqueda) return res.status(400).json({ success: false, error: 'Falta ID' });

    const numId = idBusqueda.replace(/\D/g, ''); 

        try {
            // 🧠 CEREBRO DE ENRUTAMIENTO: ¿Es Cédula o es Teléfono de Vivian (58...)?
            let querySupabase = supabase.from('clientes').select('*');
            
            if (numId.startsWith('58') && numId.length >= 11) {
                // Modo Francotirador: Sacamos los últimos 10 dígitos para ignorar formatos raros (0412, +58, etc)
                const telLimpio = numId.slice(-10);
                querySupabase = querySupabase.or(`telefono_movil.ilike.%${telLimpio}%,telefono_fijo.ilike.%${telLimpio}%`);
            } else {
                // Modo Clásico: Búsqueda por cédula
                querySupabase = querySupabase.ilike('documento_cliente', `%${numId}`);
            }

            const { data: cliente } = await querySupabase.limit(1).single();

            // 🛑 BLOQUEO DE SEGURIDAD: Si no hay cliente, abortamos misión inmediatamente
        if (!cliente) {
            agregarLog(reqId, 'ALERTA', `[⚡ SPEED-API] [👤 ${idBusqueda}] Cliente no encontrado.`, 'SYS');
            return res.status(404).json({ 
                success: false, 
                error: 'Cliente no encontrado en la base de datos. Por favor verifica los datos o contacta a soporte.' 
            });
        }

        const docCliente = cliente.documento_cliente;
        const nombreCliente = cliente.nombre_cliente;

        const { data: servicios } = await supabase
            .from('servicios')
            .select('*')
            .eq('documento', docCliente);

        let deudaTotalNum = 0;
        let cantidadSuspendidos = 0;

        const arrServicios = (servicios || []).map((s, index) => {
            if (s.saldo && s.saldo !== "N/A" && s.saldo !== "0,00" && s.saldo !== "0.00") {
                let saldoNum = parseFloat(s.saldo.replace(/\./g, '').replace(',', '.'));
                if (!isNaN(saldoNum)) deudaTotalNum += saldoNum;
            }
            if (s.estado && s.estado.toLowerCase().includes('suspendido')) cantidadSuspendidos++;

            return {
                // --- ESTRUCTURA CLÁSICA (INTOCABLE PARA LA APP/VIVIAN) ---
                numero_servicio: index + 1,
                plan: s.plan || "Plan no especificado",
                ip: s.ip_servicio || "N/A",
                estado: s.estado || "N/A",
                saldo: s.saldo || "0,00",
                fecha_corte: s.fecha_corte_actual || "N/A",
                direccion: s.direccion_servicio || "No detectada",
                
                // --- EXPANSIÓN DE TITANIO (DATOS NUEVOS EXTRAÍDOS POR CRONOS) ---
                id_servicio_cliente: s.id_servicio_cliente || null,
                serial_onu: s.serial_onu || null,
                usuario_pppoe: s.usuario_pppoe || null,
                clave_pppoe: s.clave_pppoe || null,
                nodo: s.nodo || null,
                puerto_pon: s.puerto_pon || null,
                nap: s.nap || null,
                puerto: s.puerto || null,
                servicio_vip: s.servicio_vip || null,
                sucursal: s.sucursal || null,
                coordenadas: s.coordenadas || null,
                fecha_instalacion: s.fecha_instalacion || null,
                instalador: s.instalador || null
            };
        });

        // Agregamos el correo y mantenemos la estructura base idéntica
        const baseData = {
            id_busqueda: idBusqueda,
            nombre_cliente: nombreCliente,
            codigo_cliente: docCliente,
            movil: cliente ? cliente.telefono_movil : "N/A",
            fijo: cliente ? cliente.telefono_fijo : "N/A",
            link_pago: "https://vidanet.icarosoft.com/Login/",
            e_mail: cliente ? cliente.e_mail : null 
        };

        const duracion = Date.now() - inicioReloj;
        // 🎯 INYECCIÓN DE RADAR: Agregamos la etiqueta del cliente al log
        agregarLog(reqId, 'EXITO', `[⚡ SPEED-API] [👤 ${idBusqueda}] Servicios entregados a ${origen || 'App'} (${arrServicios.length} ítems)`, 'SYS', duracion);

        if (origen === 'vivian') {
            const serviciosObj = {};
            arrServicios.forEach((s, idx) => { serviciosObj[`servicio_${idx + 1}`] = s; });

            return res.json({
                success: true,
                data: {
                    ...baseData,
                    servicios: serviciosObj,
                    resumen: {
                        total_servicios: arrServicios.length,
                        tiene_deuda: deudaTotalNum > 0,
                        deuda_total_usd: deudaTotalNum > 0 ? deudaTotalNum.toFixed(2).replace('.', ',') : "0,00",
                        hay_suspendidos: cantidadSuspendidos > 0,
                        cantidad_suspendidos: cantidadSuspendidos,
                        todos_activos: cantidadSuspendidos === 0
                    }
                }
            });
        } else {
            return res.json({ success: true, data: { ...baseData, servicios: arrServicios } });
        }
    } catch (error) {
        agregarLog(reqId, 'ERROR', `[⚡ SPEED-API] Fallo en servicios: ${error.message}`, 'SYS');
        return res.status(500).json({ success: false, error: error.message });
    }
});

// ⚡ 2. BÚSQUEDA DE FACTURAS Y TRANSACCIONES (Para la App)
app.get('/buscar-finanzas', async (req, res) => {
    const inicioReloj = Date.now();
    const idBusqueda = req.query.id || req.query.cedula;
    const reqId = Math.random().toString(36).substring(2, 7).toUpperCase();
    
    if (!idBusqueda) return res.status(400).json({ success: false, error: 'Falta ID' });
    const numId = idBusqueda.replace(/\D/g, '');

    try {
        const { data: cliente } = await supabase
            .from('clientes')
            .select('documento_cliente')
            .ilike('documento_cliente', `%${numId}`)
            .limit(1)
            .single();

        // 🛑 BLOQUEO DE SEGURIDAD: Si no hay cliente, abortamos misión inmediatamente
        if (!cliente) {
            agregarLog(reqId, 'ALERTA', `[⚡ SPEED-API] [👤 ${idBusqueda}] Cliente no encontrado (Finanzas).`, 'SYS');
            return res.status(404).json({ 
                success: false, 
                error: 'No pudimos localizar tu historial. Verifica tu número de identificación.' 
            });
        }

        const docCliente = cliente.documento_cliente;

        const { data: facturas } = await supabase
            .from('facturas')
            .select('*')
            .or(`cod_cliente.ilike.%${numId}%,rif_fiscal.ilike.%${numId}%`)
            .order('f_emision', { ascending: false })
            .limit(20); 

        const { data: transacciones } = await supabase
            .from('registro_pagos')
            .select('*')
            .eq('documento_cliente', docCliente)
            .order('fecha_pago', { ascending: false })
            .limit(20);

        const arrFacturas = (facturas || []).map(f => ({
            // --- CLÁSICO ---
            numero: f.nro_control || f.nro_notificacion || f.id_ventas || "00000",
            fecha: f.f_emision || "N/A",
            estado: f.status || "DESCONOCIDO",
            monto: f.total_factura || "0,00",
            saldo: f.saldo || "0,00",
            
            // --- NUEVO ---
            id_ventas: f.id_ventas || null,
            nro_fiscal: f.nro_fiscal || null,
            descripcion: f.descripcion || null,
            exento: f.exento || "0,00",
            base_imp: f.base_imp || "0,00",
            iva: f.iva || "0,00",
            total_fact_bsd: f.total_fact_bsd || "0,00",
            pago: f.pago || null,
            razon_social: f.razon_social || null,
            rif_fiscal: f.rif_fiscal || null
        }));

        const arrTransacciones = (transacciones || []).map(t => ({
            // --- CLÁSICO ---
            // 🎯 Ahora el "tipo" jala el método de pago real guardado en Supabase
            tipo: t.metodo_pago || t.origen_reporte || "PAGO", 
            
            // 🎯 Y la "forma" muestra únicamente el banco o plataforma (ej: Binance)
            forma: t.banco_origen || "N/A",
            referencia: t.referencia || "SIN_REF",
            monto_bs: t.monto_bs ? parseFloat(t.monto_bs).toLocaleString('es-VE', { minimumFractionDigits: 2 }) : "0,00",
            fecha: t.fecha_pago || "N/A",
            // 🛡️ LÓGICA TÁCTICA DE ESTADO: Si tiene estado (Vidanet) lo usa. Si es Icarosoft, dice "REPORTADO".
            status: t.estado ? t.estado : (t.origen_reporte === 'ICAROSOFT' ? 'REGISTRADO' : 'PENDIENTE'),
            
            // --- NUEVO ---
            url_comprobante: t.url_comprobante || null,
            direccion_reportada: t.direccion_reportada || null,
            id_deuda_pagada: t.id_deuda_pagada || null
        }));

        const duracion = Date.now() - inicioReloj;
        // 🎯 INYECCIÓN DE RADAR: Agregamos la etiqueta del cliente al log
        agregarLog(reqId, 'EXITO', `[⚡ SPEED-API] [👤 ${idBusqueda}] Facturas entregadas a App`, 'SYS', duracion);

        return res.json({
            success: true,
            data: {
                id: idBusqueda,
                facturas: arrFacturas,
                transacciones: arrTransacciones
            }
        });
    } catch (error) {
        agregarLog(reqId, 'ERROR', `[⚡ SPEED-API] Fallo en facturas: ${error.message}`, 'SYS');
        return res.status(500).json({ success: false, error: error.message });
    }
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
    
    // Al declararla con 'let', la hacemos estrictamente PRIVADA para esta petición
    let idCliente = ""; 
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

    // ====================================================================
    // 🧠 INTERCEPTOR DE CACHÉ (LECTURA A LA VELOCIDAD DE LA LUZ)
    // ====================================================================
    if (req.path === '/buscar-servicios' && idCliente) {
        const cacheHit = CACHE_SERVICIOS.get(idCliente);
        if (cacheHit && (Date.now() - cacheHit.timestamp < TIEMPO_CACHE)) {
            const segsRestantes = Math.ceil((TIEMPO_CACHE - (Date.now() - cacheHit.timestamp)) / 1000);
            log('EXITO', `Cache Hit. Sirviendo datos de memoria (Expira en ${segsRestantes}s)`, 'SYS', 0);
            return res.status(200).json(cacheHit.data);
        }
    }
    // ====================================================================
    
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
            
            // 🛑 EL CANDADO DE TITANIO: Bloqueo absoluto por carga. 
            // Si el Obrero tiene 1 sola petición de CUALQUIER tipo, se vuelve invisible.
            if (o.carga > 0) return false; 
            
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

        // --- CORTACIRCUITOS DINÁMICO (TIMEOUT RED) ---
        // Aumentamos el límite de red a 130s para darle oportunidad al Watchdog (120s) de actuar primero
        const limiteTiempo = req.path === '/buscar-servicios' ? 15000 : 130000;
        // ------------------------------------------

        try {
            obreroElegido.carga++;
            
            if (req.path === '/buscar-servicios') {
                obreroElegido.buscandoServicios = true;
            }

            console.log(`  [>> REQ: ${requestId}] ${etiqueta}Intentando Obrero ${obreroElegido.id} (Intento ${intentos + 1}/3)`);

            // --- 🎯 INYECCIÓN VITAL: RESPUESTA ANTICIPADA SIEMPRE ---
            // Si es un pago, no importa si hay cola o no, enviamos el 200 OK INMEDIATAMENTE
            if (esRutaPago && !respuestaEnviada) {
                res.status(200).json({ status: "OK", message: "Procesando solicitud en background..." });
                respuestaEnviada = true;
                log('INFO', `Respuesta HTTP 200 (Anticipada) enviada al bot.`);
            }
            // --------------------------------------------------------

            // --- 🧹 ADUANA DE LIMPIEZA CENTRAL (COMANDANTE) ---
            // Detectamos y aniquilamos los saltos de línea (\n) SOLO para los registros de Icaro
            if (req.method === 'POST' && req.path === '/pagar' && req.body && req.body.datos) {
                if (typeof req.body.datos.direccion === 'string') {
                    req.body.datos.direccion = req.body.datos.direccion.replace(/[\r\n]+/g, ' ').replace(/\s+/g, ' ').trim();
                }
            }
            // --------------------------------------------------

            // --- FIX RAILWAY PROXY: Forzamos HTTPS para evitar pérdida de datos por redirección 301 ---
            const protocoloReal = req.headers['x-forwarded-proto'] || 'https'; 
            const webhookUrl = `${protocoloReal}://${req.get('host')}/api/tactico/webhook`;
            const reqBody = req.method !== 'GET' ? { ...req.body, reqId: requestId, webhookUrl } : undefined;
            // ------------------------------------------------------------------------------------------

            const respuesta = await axios({
                method: req.method,
                url: `${obreroElegido.url}${req.originalUrl}`,
                data: reqBody,
                headers: { 'Content-Type': 'application/json' },
                timeout: limiteTiempo 
            });

            // Si es un pago, activamos los cronómetros y pausamos la ejecución
            if (esRutaPago) {
                // TIEMPOS ACTUALIZADOS DEL PERRO GUARDIÁN (120s y 60s)
                if (req.path === '/pagar') {
                    obreroElegido.cocinandoHasta = Date.now() + 125000;
                    log('INFO', `Let Him Cook (120s) - Esperando Reporte Radio...`, obreroElegido.id);
                } else if (req.path === '/pagar-vidanet') {
                    obreroElegido.cocinandoHasta = Date.now() + 65000;
                    log('INFO', `Let Him Cook (60s) - Esperando Reporte Radio...`, obreroElegido.id);
                }

                const timeoutWatchdog = req.path === '/pagar' ? 120000 : 60000;
                
                // 🛑 EL COMANDANTE SE PAUSA AQUÍ HASTA QUE EL OBRERO AVISE O EL PERRO MUERDA 🛑
                const rMision = await esperarWebhook(requestId, timeoutWatchdog);
                
                const duracion = Date.now() - inicioReloj;

                if (rMision.exito) {
                    log('EXITO', `Misión completada: ${rMision.mensaje}`, obreroElegido.id, duracion);
                    PAGOS_EXITOSOS.unshift({
                        reqId: requestId, tiempo: Date.now(), cliente: idCliente || 'DESCONOCIDO',
                        sistema: rMision.sistema, duracion: duracion
                    });

                    // 🛰️ DESPLIEGUE A SUPABASE (El registro silencioso)
                    // Le pasamos req.body original porque ahí viene la imagen sin censurar
                    inyectarPagoEnSupabase(req.path, req.body, idCliente, log);

                } else {
                    // Falló. ¿Es culpa del cliente o técnica?
                    const msgL = rMision.mensaje.toLowerCase();
                    const esCulpaCliente = msgL.includes("referencia") || msgL.includes("dirección") || msgL.includes("no existe") || msgL.includes("abortado");

                    if (esCulpaCliente) {
                        log('ALERTA', `Rechazado (Cliente): ${rMision.mensaje}`, obreroElegido.id, duracion);
                        PAGOS_EXITOSOS.unshift({
                            reqId: requestId, tiempo: Date.now(), cliente: idCliente || 'DESCONOCIDO',
                            sistema: `${rMision.sistema} (RECHAZADO)`, duracion: duracion
                        });
                        // Como es culpa del cliente, NO reintentamos. Se termina la misión.
                    } else {
                        // Error técnico. Lanzamos error para que el bloque 'catch' lo atrape y reasigne el pago
                        throw new Error(`Fallo interno del Obrero: ${rMision.mensaje}`);
                    }
                }
                if (PAGOS_EXITOSOS.length > MAX_PAGOS) PAGOS_EXITOSOS.pop();

                } else {
                // Lógica normal para Consultas de Saldo (Sincrónicas)
                const duracion = Date.now() - inicioReloj;
                log('EXITO', `Respuesta HTTP ${respuesta.status} devuelta.`, obreroElegido.id, duracion);
                
                // --- 🤖 ADAPTADOR DE SALIDA PARA VIVIAN (CON CEREBRO Y EN USD) ---
                // Si la petición viene de Vivian y trae un Array de servicios, lo aplanamos a Objeto y resumimos.
                if (req.query.origen === 'vivian' && respuesta.data && respuesta.data.data && Array.isArray(respuesta.data.data.servicios)) {
                    log('INFO', 'Traduciendo y resumiendo datos para Vivian...', obreroElegido.id);
                    
                    const serviciosObj = {};
                    const arrayOriginal = respuesta.data.data.servicios;
                    
                    let deudaTotalNum = 0;
                    let cantidadSuspendidos = 0;
                    
                    // Convertimos el Array en un Objeto y calculamos el resumen
                    arrayOriginal.forEach((servicio, index) => {
                        serviciosObj[`servicio_${index + 1}`] = servicio;
                        
                        // Calcular deudas en DÓLARES (ej. "112,00" -> 112.00 para poder sumar)
                        if (servicio.saldo && servicio.saldo !== "N/A" && servicio.saldo !== "0,00" && servicio.saldo !== "0.00") {
                            let saldoLimpio = servicio.saldo.replace(/\./g, '').replace(',', '.');
                            let saldoNum = parseFloat(saldoLimpio);
                            if (!isNaN(saldoNum)) deudaTotalNum += saldoNum;
                        }
                        
                        // Contar servicios suspendidos
                        if (servicio.estado && servicio.estado.toLowerCase().includes('suspendido')) {
                            cantidadSuspendidos++;
                        }
                    });
                    
                    // Reemplazamos el array con el nuevo objeto de servicios aplanados
                    respuesta.data.data.servicios = serviciosObj;
                    
                    // 🧠 INYECTAMOS EL RESUMEN EJECUTIVO PARA VIVIAN (TODO EN UNA SOLA CAJA)
                    respuesta.data.data.resumen = {
                        total_servicios: arrayOriginal.length, // <-- AHORA SÍ ESTÁ DENTRO DEL RESUMEN
                        tiene_deuda: deudaTotalNum > 0,
                        deuda_total_usd: deudaTotalNum > 0 ? deudaTotalNum.toFixed(2).replace('.', ',') : "0,00",
                        hay_suspendidos: cantidadSuspendidos > 0,
                        cantidad_suspendidos: cantidadSuspendidos,
                        todos_activos: cantidadSuspendidos === 0
                    };
                }
                

            // ====================================================================
            // 💾 GUARDAR EN CACHÉ SI FUE UNA BÚSQUEDA EXITOSA
            // ====================================================================
            if (req.path === '/buscar-servicios' && idCliente && respuesta.status === 200) {
                CACHE_SERVICIOS.set(idCliente, {
                    timestamp: Date.now(),
                    data: respuesta.data
                });
            }

                if(respuesta.data) console.log(formatoLogConsola(`${etiqueta}Respuesta [${requestId}]`, respuesta.data));
                
                if (!respuestaEnviada) res.status(respuesta.status).json(respuesta.data);
            }

            obreroElegido.fallos = 0;
            exito = true;

        } catch (error) {
            const statusError = error.response ? error.response.status : 500; 
            let msjResumido = error.message;
            
            // --- INTERCEPTORES DE FALLA ---
            if (error.message === "WATCHDOG_TIMEOUT") {
                msjResumido = `☠️ WATCHDOG: Obrero caído. No reportó por radio a tiempo (Timeout superado). Reasignando...`;
            } else if (error.code === 'ECONNABORTED' && error.message.includes('timeout')) {
                msjResumido = `TIMEOUT: Obrero abortado a los ${limiteTiempo / 1000}s. Petición huérfana detectada.`;
                obreroElegido.fallos = 99; // ☢️ INYECCIÓN LETAL: Obligamos al Comandante a ejecutar la Orden 66 más abajo para limpiar el choque de trenes.
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

            log('ERROR', `Fallo Intento ${intentos}/3: ${msjResumido.substring(0,80)}`, obreroElegido.id);

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

// ============================================================================
// 5. EL NÚCLEO AUTÓNOMO (SELF-HEALING HEARTBEAT) - "EL JAQUE MATE"
// ============================================================================
const INTERVALO_PATRULLAJE = 45000; // El Comandante sale a patrullar cada 45 segundos

setInterval(async () => {
    // Solo patrullamos si no hay un caos absoluto de tráfico (evita sobrecargar)
    if (COLA_DE_ESPERA.length > 5) return; 

    agregarLog('SYS', 'INFO', 'Iniciando patrullaje autónomo. Escaneando signos vitales del escuadrón...');
    
    for (let obrero of OBREROS) {
        // No molestamos a los obreros que están en medio de un pago largo (Cocinando)
        if (Date.now() < obrero.cocinandoHasta) continue;

        try {
            const inicio = Date.now();
            // Disparamos un ping silencioso a la raíz del Obrero (o a cualquier ruta rápida)
            await axios.get(`${obrero.url}/`, { timeout: 8000 });
            const latencia = Date.now() - inicio;

            // EL MILAGRO: Si el obrero estaba en Cuarentena/Muerto pero respondió, lo revivimos en silencio.
            if (!obrero.activo) {
                obrero.activo = true;
                obrero.fallos = 0;
                agregarLog('SYS', 'EXITO', `AUTO-RESURRECCIÓN: Obrero ${obrero.id} respondió al latido. Reintegrado a las filas.`, obrero.id, latencia);
            }

        } catch (error) {
            // EL CASTIGO: El obrero no respondió al latido.
            if (obrero.activo) {
                obrero.fallos++;
                const statusHttp = error.response ? error.response.status : null;
                const motivo = statusHttp ? `HTTP ${statusHttp}` : 'Timeout/Network';
                
                agregarLog('SYS', 'ALERTA', `PATRULLAJE: Obrero ${obrero.id} no tiene pulso (${motivo}). Falla silenciosa sumada (${obrero.fallos}/2).`, obrero.id);
                
                // Si falla 2 latidos, el Comandante actúa preventivamente
                if (obrero.fallos >= 2) {
                    obrero.activo = false;
                    
                    if (statusHttp === 502) {
                        // ☢️ MUERTE CEREBRAL (HARD KILL) -> Redeploy en Railway
                        agregarLog('SYS', 'ERROR', `AUTO-PURGA: Obrero ${obrero.id} devolvió 502. Disparando REDEPLOY a la nube.`, obrero.id);
                        reiniciarObreroDesdeRailway(obrero);
                        // 🛑 NOTA VITAL: NO hay setTimeout aquí. Esperamos pacientemente el Webhook de 'success' de Railway para revivirlo.
                    } else {
                        // 🛡️ ATASCADO (SOFT KILL) -> Orden 66 interna
                        agregarLog('SYS', 'ERROR', `AUTO-PURGA: Obrero ${obrero.id} atascado. Ejecutando Orden 66 preventiva.`, obrero.id);
                        axios.post(`${obrero.url}/orden-66`, {}, { headers: { 'x-comandante-secret': 'IcaroSoft_Destruccion_Inminente_2026' }, timeout: 5000 }).catch(() => {});
                        
                        // Aquí SÍ programamos resurrección rápida porque el contenedor sigue vivo, solo le lavamos el cerebro
                        const idParaRevivir = obrero.id;
                        setTimeout(() => {
                            const obj = OBREROS.find(o => o.id === idParaRevivir);
                            if(obj) {
                                obj.activo = true;
                                obj.fallos = 0;
                                agregarLog('SYS', 'INFO', `Fin de cuarentena preventiva (40s). Obrero ${idParaRevivir} limpio y resucitado.`);
                            }
                        }, 40000); 
                    }
                }
            }
        }
    }

    // --- 🫀 NUEVO: ESCANEO DE SUPERVIVENCIA CRONOS ---
    for (let cronos of ESCUADRON_CRONOS) {
        // Respetamos ciegamente el periodo de gracia post-redeploy
        if (Date.now() < cronos.ignorarHasta) continue; 

        try {
            await axios.get(`${cronos.url}/`, { timeout: 10000 });
            // Si estaba fallando pero volvió solo, lo celebramos
            if (cronos.fallos > 0) {
                agregarLog('SYS', 'EXITO', `CRONOS ${cronos.nombre} ha vuelto en línea por su cuenta.`);
                cronos.fallos = 0;
            }
        } catch (error) {
            cronos.fallos++;
            agregarLog('SYS', 'ALERTA', `PATRULLAJE: CRONOS ${cronos.nombre} sin pulso HTTP (${cronos.fallos}/3 latidos fallidos).`);

            // Si falla 3 latidos seguidos (aprox 2 minutos en silencio absoluto), apretamos el gatillo
            if (cronos.fallos >= 3) {
                reiniciarCronosDesdeRailway(cronos);
                cronos.fallos = 0; // Se reinicia mientras cuenta el periodo de gracia
            }
        }
    }

}, INTERVALO_PATRULLAJE);

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`\n======================================`);
    console.log(`🚀 COMANDANTE V4.8 (WATCHDOG & WEBHOOKS)`);
    console.log(`📡 Puerto: ${PORT}`);
    console.log(`🤖 Obreros: ${OBREROS.length}`);
    console.log(`======================================\n`);
    agregarLog('SYS', 'INFO', `Sistema Inicializado. ${OBREROS.length} Obreros reportándose.`);
});
