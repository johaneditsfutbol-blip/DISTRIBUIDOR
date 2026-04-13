const express = require('express');
const axios = require('axios');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js'); // <-- EL CAÑÓN

const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' })); // Aumentamos límite por las imágenes Base64 de Icaro
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// ============================================================================
// ESCUDO DE AUTENTICACIÓN (API KEY)
// ============================================================================
app.use((req, res, next) => {
    // 1. Excluimos el panel de control y los webhooks internos para no romper la infraestructura
    if (req.path === '/status' || req.path.startsWith('/api/tactico')) {
        return next();
    }

    // 2. Excepción táctica: Si la solicitud viene de Vivian, le abrimos la puerta sin pedir llave
    const origen = req.query.origen || (req.body && req.body.origen);
    if (origen === 'vivian') {
        return next();
    }

    // 3. Verificación de la llave para el resto de mortales (App)
    const tokenHeader = req.headers['x-api-key'];
    const tokenReal = process.env.API_SECRET_TOKEN;

    if (!tokenHeader || tokenHeader !== tokenReal) {
        return res.status(401).json({ 
            success: false, 
            error: "Acceso denegado. Faltan credenciales de seguridad o son inválidas." 
        });
    }

    // Si tiene la llave correcta, lo dejamos pasar a la ruta que solicitó
    next();
});

// ============================================================================
// NÚCLEO DE DATOS: SUPABASE Y PERRO RASTREADOR
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

        // 🔍 RADAR DE SUCURSAL: Buscamos a qué sede pertenece el cliente antes de registrar el pago
        let sucursalCliente = 'PRINCIPAL'; // Valor por defecto en caso de emergencia
        const { data: dataCli } = await supabase
            .from('clientes')
            .select('sucursal')
            .eq('documento_cliente', payload.documento_cliente)
            .single();
        
        if (dataCli && dataCli.sucursal) {
            sucursalCliente = dataCli.sucursal;
        }
        
        payload.sucursal = sucursalCliente; // Le pegamos la etiqueta final al pago

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
    // 🛡️ ESCUADRÓN PRINCIPAL
    { id: 1, url: 'https://obrero-5-3-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false, rwServiceId: 'a340646a-9c71-4a89-8bae-6c59ff9864ae', rwEnvId: ENV_PROD_ID, rwProjectName: NOMBRE_PROYECTO, sucursal: 'PRINCIPAL' },
    { id: 2, url: 'https://obrero-6-4-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false, rwServiceId: '9be77316-1d28-4d0b-ad80-621c4edd13a3', rwEnvId: ENV_PROD_ID, rwProjectName: NOMBRE_PROYECTO, sucursal: 'PRINCIPAL' },
    { id: 3, url: 'https://obrero-7-5-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false, rwServiceId: '7adc6cb6-ffe7-42a2-9ee9-22171b5266b6', rwEnvId: ENV_PROD_ID, rwProjectName: NOMBRE_PROYECTO, sucursal: 'PRINCIPAL' },
    
    // ⚔️ ESCUADRÓN TOCUYITO (Añade aquí sus IDs y URLs reales cuando los crees)
    { id: 4, url: 'https://obrero-1-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false, rwServiceId: '2d95e6e3-414d-4c63-a8aa-0397e82b8336', rwEnvId: ENV_PROD_ID, rwProjectName: NOMBRE_PROYECTO, sucursal: 'TOCUYITO' },
    { id: 5, url: 'https://obrero-3-1-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false, rwServiceId: 'fcd7a396-2606-41ad-aaec-fdcf78caf5bd', rwEnvId: ENV_PROD_ID, rwProjectName: NOMBRE_PROYECTO, sucursal: 'TOCUYITO' },
    { id: 6, url: 'https://obrero-2-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false, rwServiceId: '4c16d6ce-8b1d-4d1b-a1b5-a886e11ce6ea', rwEnvId: ENV_PROD_ID, rwProjectName: NOMBRE_PROYECTO, sucursal: 'TOCUYITO' }
];

const MAX_LOGS = 15000;
let HISTORIAL = [];

// --- 🛸 ESCUADRÓN FANTASMA (CRONOS) ---
const ENV_CRONOS_ID = 'bdf0e55e-76e7-48ae-9af3-4864fadfd05b';

const ESCUADRON_CRONOS = [
    { nombre: 'SERVICIOS-PRINCIPAL', url: 'https://servicios-production-9681.up.railway.app', fallos: 0, ignorarHasta: 0 },
    { nombre: 'FACTURAS-PRINCIPAL', url: 'https://facturas-production-2ab1.up.railway.app', fallos: 0, ignorarHasta: 0 },
    { nombre: 'SERVICIOS-TOCUYITO', url: 'https://servicios-tocuyito-production.up.railway.app', fallos: 0, ignorarHasta: 0 },
    { nombre: 'FACTURAS-TOCUYITO', url: 'https://facturas-tocuyito-production.up.railway.app', fallos: 0, ignorarHasta: 0 }
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

const esperarTurno = (requestId, etiqueta, esBackground = false, sucursal = 'PRINCIPAL') => {
    return new Promise((resolve, reject) => {
        // ... (mismo código interno del if y setTimeout) ...
        if (!esBackground) {
            timeoutCola = setTimeout(() => {
                const index = COLA_DE_ESPERA.findIndex(c => c.resolve === resolve);
                if (index !== -1) COLA_DE_ESPERA.splice(index, 1);
                reject(new Error("TIMEOUT_COLA"));
            }, TIMEOUT_SALA_ESPERA);
        }
        // Le pegamos la sucursal al ticket para que el Motor sepa a dónde va
        COLA_DE_ESPERA.push({ resolve, timeoutCola, requestId, etiqueta, sucursal });
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

// --- NUEVO: RECEPTOR DE TELEMETRÍA EXTERNA ---
app.post('/api/tactico/log-externo', (req, res) => {
    const { reqId, tipo, mensaje, idOrigen, duracion } = req.body;
    agregarLog(reqId || 'EXT', tipo, mensaje, idOrigen || 'UNK', duracion);
    res.status(200).send("Reporte recibido");
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

// --- EL DISPARO DE RESURRECCION PARA CRONOS (KILL SWITCH DIRECTO) ---
async function reiniciarCronosDesdeRailway(cronos) {
    agregarLog('SYS', 'ALERTA', `☠️ Enviando orden de KILL directo al contenedor de CRONOS ${cronos.nombre}...`);

    try {
        // Disparo táctico directo a la ruta secreta del bot
        await axios.get(`${cronos.url}/kill`, { timeout: 5000 });
        
        agregarLog('SYS', 'EXITO', `💥 CRONOS ${cronos.nombre} se ha auto-destruido. Railway lo levantará limpio. Entrando en gracia (4 min)...`);
        cronos.ignorarHasta = Date.now() + (4 * 60 * 1000); 
    } catch (error) {
        agregarLog('SYS', 'ERROR', `Fallo al ejecutar el kill a CRONOS ${cronos.nombre}: ${error.message}`);
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
        <link rel="stylesheet" href="https://cdnjs.cloudflare.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Fira+Code:wght@400;500&display=swap" rel="stylesheet">
        
        <script>
            tailwind.config = {
                theme: {
                    extend: {
                        fontFamily: {
                            sans: ['"Inter"', 'sans-serif'],
                            mono: ['"Fira Code"', 'monospace']
                        }
                    }
                }
            }
        </script>
        <style>
            body { background-color: #f8fafc; color: #334155; overflow-x: hidden; }
            ::-webkit-scrollbar { width: 6px; height: 6px; }
            ::-webkit-scrollbar-track { background: #f1f5f9; rounded: 4px; }
            ::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 4px; }
            ::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
            
            /* TABS CSS - Minimalista corporativo */
            .tab-btn { transition: all 0.2s ease; font-weight: 500; }
            .tab-active { color: #2563eb; border-bottom: 2px solid #2563eb; }
            .tab-inactive { color: #64748b; border-bottom: 2px solid transparent; }
            .tab-inactive:hover { color: #334155; border-color: #cbd5e1; }
            
            .micro-data { font-size: 0.65rem; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase; color: #64748b; }
            .card-shadow { box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06); }
        </style>
    </head>
    <body class="p-4 md:p-8 min-h-screen relative flex flex-col font-sans">
        
        <div class="max-w-[1400px] mx-auto w-full relative z-10 flex-grow flex flex-col">
            
            <header class="bg-white card-shadow border border-slate-200 rounded-xl p-5 md:p-6 mb-6 flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                <div class="flex items-center gap-4">
                    <div class="w-12 h-12 flex items-center justify-center rounded-lg bg-blue-50 border border-blue-100 text-blue-600">
                        <i class="fa-solid fa-network-wired text-xl"></i>
                    </div>
                    <div>
                        <h1 class="text-2xl font-bold text-slate-800 tracking-tight">CX-NEXUS <span class="font-normal text-slate-400">| Command Center</span></h1>
                        <div class="flex gap-3 text-xs font-medium text-slate-500 mt-1">
                            <span>SYS_ID: OP-77X</span> 
                            <span class="text-slate-300">•</span> 
                            <span>NODE: RAILWAY_PRD</span>
                        </div>
                    </div>
                </div>

                <div class="flex flex-col items-start md:items-end gap-2 w-full md:w-auto bg-slate-50 p-3 rounded-lg border border-slate-100">
                    <div class="flex items-center gap-3 text-sm font-semibold">
                        <div id="badge-cola" class="flex items-center gap-2 border border-slate-200 bg-white px-3 py-1 rounded-md transition-all duration-300 shadow-sm">
                            <i class="fa-solid fa-server text-slate-400" id="icon-cola"></i>
                            <span id="txt-cola" class="text-slate-600">COLA: 0</span>
                        </div>
                        <div class="flex items-center gap-2 border border-emerald-200 bg-emerald-50 px-3 py-1 rounded-md shadow-sm">
                            <span class="relative flex h-2 w-2">
                                <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                                <span class="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
                            </span>
                            <span class="text-emerald-700 text-xs">UPLINK ESTABLE</span>
                        </div>
                    </div>
                    <div class="flex gap-3 text-[10px] font-mono text-slate-500 mt-1 w-full justify-end">
                        <span>LATENCIA: <span class="text-emerald-600 font-semibold">12ms</span></span>
                        <span id="txt-actualizacion">SYNC: 00:00:00</span>
                    </div>
                </div>
            </header>
            
            <div class="mb-2">
                <h2 class="text-sm font-bold text-slate-700 uppercase tracking-wider mb-3 flex items-center gap-2">
                    <i class="fa-solid fa-robot text-blue-500"></i> Escuadrón Activo
                </h2>
                <div id="grid-obreros" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4 mb-8"></div>
            </div>

            <div class="flex gap-8 mb-4 px-2 border-b border-slate-200">
                <button id="btn-tab-telemetry" class="tab-btn tab-active pb-3 flex items-center gap-2 text-sm" onclick="switchTab('telemetry')">
                    <i class="fa-solid fa-satellite-dish"></i> Registro de Telemetría
                </button>
                <button id="btn-tab-ledger" class="tab-btn tab-inactive pb-3 flex items-center gap-2 text-sm" onclick="switchTab('ledger')">
                    <i class="fa-solid fa-vault"></i> Libro de Pagos (Ledger)
                </button>
            </div>

            <div class="bg-white card-shadow border border-slate-200 rounded-xl p-5 flex-grow flex flex-col relative overflow-hidden h-[500px]">
                
                <div id="view-telemetry" class="w-full h-full flex flex-col">
                    <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-4 gap-4">
                        <div class="micro-data text-slate-400">
                            BUFFER: 1000 EVENTOS
                        </div>
                        <div class="flex gap-3 w-full sm:w-auto">
                            <div class="relative w-full sm:w-72">
                                <div class="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none">
                                    <i class="fa-solid fa-magnifying-glass text-slate-400"></i>
                                </div>
                                <input type="text" id="input-busqueda-tel" placeholder="Buscar ID, mensaje..." class="bg-slate-50 border border-slate-200 text-slate-700 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full pl-9 p-2 outline-none transition-colors">
                            </div>
                            <select id="filtro-tipo-tel" class="bg-slate-50 border border-slate-200 text-slate-700 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full sm:w-40 p-2 outline-none font-medium cursor-pointer">
                                <option value="ALL">Todos los eventos</option>
                                <option value="ERROR">Errores</option>
                                <option value="EXITO">Éxito</option>
                                <option value="COLA">En Cola</option>
                                <option value="INFO">Información</option>
                            </select>
                        </div>
                    </div>
                    <div id="terminal-logs" class="overflow-y-auto flex-grow space-y-2 pr-2 mb-3 bg-slate-50/50 rounded-lg p-2 border border-slate-100"></div>
                    
                    <div class="flex justify-between items-center border-t border-slate-200 pt-3">
                        <span id="txt-resultados-tel" class="text-slate-500 text-xs font-semibold">COINCIDENCIAS: 0</span>
                        <div class="flex items-center gap-1">
                            <button id="btn-prev-tel" class="w-8 h-8 rounded-md bg-white border border-slate-200 text-slate-600 hover:bg-slate-50 hover:text-blue-600 disabled:opacity-30 transition-all"><i class="fa-solid fa-chevron-left"></i></button>
                            <span id="txt-paginacion-tel" class="px-4 text-xs font-bold text-slate-700">1 / 1</span>
                            <button id="btn-next-tel" class="w-8 h-8 rounded-md bg-white border border-slate-200 text-slate-600 hover:bg-slate-50 hover:text-blue-600 disabled:opacity-30 transition-all"><i class="fa-solid fa-chevron-right"></i></button>
                        </div>
                    </div>
                </div>

                <div id="view-ledger" class="w-full h-full flex flex-col hidden">
                    <div class="grid grid-cols-2 md:grid-cols-5 gap-4 mb-5 pb-5 border-b border-slate-100">
                        <div class="bg-slate-50 border border-slate-200 rounded-lg p-3">
                            <div class="micro-data mb-1 text-slate-500">TOTAL PAGOS</div>
                            <div class="text-2xl font-bold text-slate-800" id="stat-total">00</div>
                        </div>
                        <div class="bg-blue-50 border border-blue-100 rounded-lg p-3">
                            <div class="micro-data mb-1 text-blue-600">PAGOS VIDANET</div>
                            <div class="text-2xl font-bold text-blue-700" id="stat-vidanet">00</div>
                        </div>
                        <div class="bg-indigo-50 border border-indigo-100 rounded-lg p-3">
                            <div class="micro-data mb-1 text-indigo-600">PAGOS ICAROSOFT</div>
                            <div class="text-2xl font-bold text-indigo-700" id="stat-icaro">00</div>
                        </div>
                        <div class="bg-red-50 border border-red-100 rounded-lg p-3">
                            <div class="micro-data mb-1 text-red-600">RECHAZOS (APP)</div>
                            <div class="text-2xl font-bold text-red-700" id="stat-rechazos-vidanet">00</div>
                        </div>
                        <div class="bg-slate-50 border border-slate-200 rounded-lg p-3">
                            <div class="micro-data mb-1 text-slate-500">TIEMPO MEDIO</div>
                            <div class="text-2xl font-bold text-emerald-600" id="stat-avg">0.0s</div>
                        </div>
                    </div>

                    <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-4 gap-4">
                        <span class="font-bold text-slate-700 text-sm flex items-center gap-2"><i class="fa-solid fa-list-check text-slate-400"></i> Historial Reciente</span>
                        <div class="flex gap-3 w-full sm:w-auto">
                            <div class="relative w-full sm:w-72">
                                <div class="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none">
                                    <i class="fa-solid fa-magnifying-glass text-slate-400"></i>
                                </div>
                                <input type="text" id="input-busqueda-led" placeholder="Buscar cédula, ID..." class="bg-slate-50 border border-slate-200 text-slate-700 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full pl-9 p-2 outline-none">
                            </div>
                            <select id="filtro-tipo-led" class="bg-slate-50 border border-slate-200 text-slate-700 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full sm:w-40 p-2 outline-none font-medium cursor-pointer">
                                <option value="ALL">Ambos Sistemas</option>
                                <option value="VIDANET">Solo Vidanet</option>
                                <option value="ICAROSOFT">Solo Icarosoft</option>
                            </select>
                        </div>
                    </div>

                    <div id="ledger-logs" class="overflow-y-auto flex-grow space-y-2 pr-2 mb-3"></div>

                    <div class="flex justify-between items-center border-t border-slate-200 pt-3">
                        <span id="txt-resultados-led" class="text-slate-500 text-xs font-semibold">COINCIDENCIAS: 0</span>
                        <div class="flex items-center gap-1">
                            <button id="btn-prev-led" class="w-8 h-8 rounded-md bg-white border border-slate-200 text-slate-600 hover:bg-slate-50 hover:text-blue-600 disabled:opacity-30 transition-all"><i class="fa-solid fa-chevron-left"></i></button>
                            <span id="txt-paginacion-led" class="px-4 text-xs font-bold text-slate-700">1 / 1</span>
                            <button id="btn-next-led" class="w-8 h-8 rounded-md bg-white border border-slate-200 text-slate-600 hover:bg-slate-50 hover:text-blue-600 disabled:opacity-30 transition-all"><i class="fa-solid fa-chevron-right"></i></button>
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
                    btnTel.className = "tab-btn tab-active pb-3 flex items-center gap-2 text-sm";
                    btnLed.className = "tab-btn tab-inactive pb-3 flex items-center gap-2 text-sm";
                } else {
                    btnTel.className = "tab-btn tab-inactive pb-3 flex items-center gap-2 text-sm";
                    btnLed.className = "tab-btn tab-active pb-3 flex items-center gap-2 text-sm";
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
                    const meses = ['ENE','FEB','MAR','ABR','MAY','JUN','JUL','AGO','SEP','OCT','NOV','DIC'];
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
                    
                    // UI Cola - Tema claro
                    txtCola.innerText = 'COLA: ' + data.encolados.toString().padStart(2, '0');
                    if (data.encolados > 0) {
                        badgeCola.className = 'flex items-center gap-2 border border-amber-200 bg-amber-50 px-3 py-1 rounded-md transition-all duration-300 shadow-sm';
                        iconCola.className = 'fa-solid fa-network-wired text-amber-500 animate-pulse';
                        txtCola.className = 'text-amber-700 font-bold text-xs';
                    } else {
                        badgeCola.className = 'flex items-center gap-2 border border-slate-200 bg-white px-3 py-1 rounded-md transition-all duration-300 shadow-sm';
                        iconCola.className = 'fa-solid fa-server text-slate-400';
                        txtCola.className = 'text-slate-600 font-medium text-xs';
                    }
                    
                    const ahora = new Date();
                    txtActualizacion.innerText = \`SYNC: \${ahora.toLocaleTimeString('en-US', {hour12: false})}\`;
                    txtActualizacion.classList.replace('text-red-500', 'text-slate-500');
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
                    
                    const bordeEstado = isVivo ? 'border-slate-200 shadow-sm bg-white' : 'border-red-200 bg-red-50/50 shadow-sm';
                    let statusIcon = isVivo ? \`<i class="fa-solid fa-circle-check text-emerald-500 text-xs"></i> <span class="text-emerald-700 font-medium">Operativo</span>\` : \`<i class="fa-solid fa-triangle-exclamation text-red-500 text-xs animate-pulse"></i> <span class="text-red-700 font-semibold">Cuarentena</span>\`;
                    
                    let opsBadges = '';
                    if (isCocinando) opsBadges += \`<div class="border border-amber-200 bg-amber-50 text-amber-700 px-2 py-0.5 rounded text-[10px] font-semibold"><i class="fa-solid fa-fire animate-pulse mr-1"></i>\${segCoccion}s</div>\`;
                    if (o.buscandoServicios) opsBadges += \`<div class="border border-sky-200 bg-sky-50 text-sky-700 px-2 py-0.5 rounded text-[10px] font-semibold"><i class="fa-solid fa-satellite-dish animate-pulse mr-1"></i>SRV</div>\`;

                    const porcentajeCarga = Math.min((o.carga / 10) * 100, 100);
                    let colorBarra = o.carga > 5 ? 'bg-red-500' : (o.carga > 2 ? 'bg-amber-400' : 'bg-blue-500');

                    // Badge dinámico para identificar si es PRINCIPAL o TOCUYITO visualmente
                    let tagSucursal = (o.sucursal === 'TOCUYITO') 
                        ? \`<span class="bg-amber-100 text-amber-800 border border-amber-200 px-1.5 py-0.5 rounded text-[9px] font-bold">TOCUYITO</span>\`
                        : \`<span class="bg-slate-100 text-slate-600 border border-slate-200 px-1.5 py-0.5 rounded text-[9px] font-bold">PRINCIPAL</span>\`;

                    htmlTemp += \`
                    <div class="p-4 border \${bordeEstado} rounded-xl transition-colors flex flex-col justify-between min-h-[160px]">
                        <div class="flex justify-between items-start mb-3">
                            <div>
                                <h2 class="text-lg font-bold text-slate-800 tracking-tight flex items-center gap-2">WK_0\${o.id} \${tagSucursal}</h2>
                                <div class="text-[11px] mt-1 flex items-center gap-1">\${statusIcon}</div>
                            </div>
                            <div class="text-right bg-slate-50 p-2 rounded border border-slate-100">
                                <div class="text-xl font-bold leading-none \${o.fallos > 0 ? 'text-red-500' : 'text-slate-700'}">\${o.fallos.toString().padStart(2, '0')}</div>
                                <div class="text-[9px] font-bold text-slate-400 mt-1 uppercase tracking-wider">Errores</div>
                            </div>
                        </div>
                        
                        <div class="flex gap-2 mb-3 h-5">\${opsBadges}</div>
                        
                        <div class="mb-4 bg-slate-50 p-2 rounded border border-slate-100">
                            <div class="flex justify-between text-[10px] font-bold text-slate-500 mb-1.5 uppercase tracking-wide">
                                <span>Carga Actual</span><span class="text-slate-700">\${o.carga} REQ</span>
                            </div>
                            <div class="w-full bg-slate-200 rounded-full h-1.5 overflow-hidden">
                                <div class="\${colorBarra} h-full transition-all duration-500" style="width: \${porcentajeCarga}%"></div>
                            </div>
                        </div>
                        
                        <div class="grid grid-cols-2 gap-2 mt-auto text-xs font-semibold">
                            <button onclick="ejecutarOrden(\${o.id})" class="bg-white text-slate-600 border border-slate-200 hover:bg-red-50 hover:text-red-600 hover:border-red-200 rounded-lg py-1.5 transition-colors group shadow-sm flex items-center justify-center gap-1.5">
                                <i class="fa-solid fa-rotate-right group-hover:animate-spin"></i> Reiniciar
                            </button>
                            <button onclick="revivir(\${o.id})" class="bg-white text-slate-600 border border-slate-200 hover:bg-emerald-50 hover:text-emerald-700 hover:border-emerald-200 rounded-lg py-1.5 transition-colors shadow-sm flex items-center justify-center gap-1.5">
                                <i class="fa-solid fa-bolt"></i> Forzar ON
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
                document.getElementById('txt-resultados-tel').innerText = \`COINCIDENCIAS: \${logsFiltrados.length.toString().padStart(4, '0')}\`;
                document.getElementById('btn-prev-tel').disabled = pagTel === 1;
                document.getElementById('btn-next-tel').disabled = pagTel === totalPaginas;

                let htmlTemp = '';
                if(logsFiltrados.length === 0) {
                    terminal.innerHTML = '<div class="text-slate-400 italic mt-6 text-center font-medium text-sm">No se encontraron registros.</div>';
                    return;
                }

                logsFiltrados.slice((pagTel - 1) * LIMIT_TEL, pagTel * LIMIT_TEL).forEach(log => {
                    let colorBase = 'text-slate-600';
                    let iconClass = 'fa-solid fa-circle-info text-slate-400';
                    let bgFondo = '';

                    if (log.tipo === 'NUEVA') { iconClass = 'fa-solid fa-arrow-right-to-bracket text-blue-500'; }
                    if (log.tipo === 'EXITO') { iconClass = 'fa-solid fa-check-circle text-emerald-500'; colorBase = 'text-slate-700 font-medium'; bgFondo = 'bg-emerald-50/30'; }
                    if (log.tipo === 'ERROR') { iconClass = 'fa-solid fa-circle-xmark text-red-500'; colorBase = 'text-red-700 font-medium'; bgFondo = 'bg-red-50/50'; }
                    if (log.tipo === 'ALERTA') { iconClass = 'fa-solid fa-triangle-exclamation text-amber-500'; colorBase = 'text-amber-700 font-medium'; bgFondo = 'bg-amber-50/50'; }
                    if (log.tipo === 'COLA') { iconClass = 'fa-solid fa-layer-group text-indigo-500'; colorBase = 'text-indigo-700'; }
                    if (log.tipo === 'INFO') { iconClass = 'fa-solid fa-terminal text-slate-500'; }

                    const duracionStr = log.duracion ? \`<span class="text-slate-400 ml-2 font-mono text-[10px]">[\${log.duracion}ms]</span>\` : '';
                    const obreroTag = log.obreroId !== 'SYS' ? \`<span class="text-blue-600 ml-2 border border-blue-200 bg-blue-50 rounded px-1.5 font-bold text-[10px]">WK_\${log.obreroId.toString().padStart(2,'0')}</span>\` : '<span class="text-slate-500 ml-2 border border-slate-200 bg-white rounded px-1.5 font-bold text-[10px]">SYS</span>';

                    htmlTemp += \`
                    <div class="flex items-start gap-3 p-2 hover:bg-white transition-colors border-l-2 border-transparent hover:border-blue-400 rounded \${bgFondo}">
                        <div class="text-slate-400 w-24 shrink-0 mt-0.5 text-[11px] font-mono">\${formatearHora(log.tiempo)}</div>
                        <div class="w-5 shrink-0 text-center mt-0.5"><i class="\${iconClass}"></i></div>
                        <div class="text-slate-600 w-16 shrink-0 text-center mt-0.5 font-mono text-[10px] font-bold bg-white border border-slate-200 rounded py-0.5">\${log.reqId}</div>
                        <div class="\${colorBase} break-all flex-grow leading-snug text-[13px]">\${log.mensaje} \${obreroTag}\${duracionStr}</div>
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
                    if (p.sistema === 'VIDANET') {
                        cVidanet++;
                    } else if (p.sistema === 'ICAROSOFT') {
                        cIcaro++;
                    } else if (p.sistema && p.sistema.includes('VIDANET') && p.sistema.includes('RECHAZADO')) {
                        cRechazadosVid++;
                    }
                    if(p.duracion) sumTime += p.duracion;
                });
                
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
                document.getElementById('txt-resultados-led').innerText = \`COINCIDENCIAS: \${filtrados.length.toString().padStart(4, '0')}\`;
                document.getElementById('btn-prev-led').disabled = pagLed === 1;
                document.getElementById('btn-next-led').disabled = pagLed === totalPaginas;

                let htmlTemp = '';
                if(filtrados.length === 0) {
                    ledger.innerHTML = '<div class="text-slate-400 italic mt-8 text-center font-medium text-sm">No hay registros de pago en memoria.</div>';
                    return;
                }

                filtrados.slice((pagLed - 1) * LIMIT_LED, pagLed * LIMIT_LED).forEach(p => {
                    const esVidanet = p.sistema.includes('VIDANET');
                    const esRechazo = p.sistema.includes('RECHAZADO');
                    
                    let colorSis = esVidanet ? 'text-blue-700 bg-blue-50 border-blue-200' : 'text-indigo-700 bg-indigo-50 border-indigo-200';
                    let iconStatus = '<i class="fa-solid fa-check text-emerald-500"></i>';
                    let bgIcon = 'bg-emerald-50 border border-emerald-100 shadow-sm';

                    if (esRechazo) {
                        colorSis = 'text-red-700 bg-red-50 border-red-200';
                        iconStatus = '<i class="fa-solid fa-xmark text-red-500"></i>';
                        bgIcon = 'bg-red-50 border border-red-100 shadow-sm';
                    }

                    const segs = p.duracion ? (p.duracion / 1000).toFixed(1) + 's' : 'N/A';
                    
                    // 🎨 EL PINTOR TÁCTICO CORPORATIVO: Insignias de sucursal
                    const badgeSucursal = (p.sucursal === 'TOCUYITO') 
                        ? '<span class="bg-amber-100 text-amber-800 border border-amber-200 px-2 py-0.5 ml-2 rounded text-[10px] font-bold align-middle">TOCUYITO</span>' 
                        : '<span class="bg-emerald-100 text-emerald-800 border border-emerald-200 px-2 py-0.5 ml-2 rounded text-[10px] font-bold align-middle">PRINCIPAL</span>';

                    htmlTemp += \`
                    <div class="flex items-center justify-between border border-slate-200 bg-white rounded-lg p-3 hover:shadow-md hover:border-blue-300 transition-all mb-2">
                        <div class="flex items-center gap-4">
                            <div class="w-10 h-10 rounded-full \${bgIcon} flex items-center justify-center text-lg">
                                \${iconStatus}
                            </div>
                            <div>
                                <div class="text-slate-800 font-bold text-sm flex items-center">\${p.cliente} \${badgeSucursal}</div>
                                <div class="text-xs font-mono text-slate-500 mt-1">\${formatearHora(p.tiempo, true)} <span class="text-slate-300 mx-1">|</span> REQ: <span class="font-bold text-slate-600">\${p.reqId}</span></div>
                            </div>
                        </div>
                        <div class="text-right">
                            <div class="\${colorSis} border px-2 py-1 rounded text-[10px] font-bold inline-block">\${p.sistema}</div>
                            <div class="text-[10px] font-mono text-slate-400 mt-1.5 font-medium">TIEMPO: <span class="text-slate-600 font-bold">\${segs}</span></div>
                        </div>
                    </div>\`;
                });
                ledger.innerHTML = htmlTemp;
            }

            async function ejecutarOrden(id) {
                if(!confirm(\`ATENCIÓN: ¿Forzar reinicio del WORKER_\${id}? (Esto destruirá el contenedor actual)\`)) return;
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

        const arrFacturas = (facturas || []).map(f => {
            // 🎯 OPERACIÓN TÁCTICA: Convertimos los textos a números puros para la suma
            const valIva = parseFloat((f.iva || "0").toString().replace(/\./g, '').replace(',', '.')) || 0;
            const valSub = parseFloat((f.sub_total || "0").toString().replace(/\./g, '').replace(',', '.')) || 0;
            
            // Sumamos y formateamos de vuelta a formato venezolano (ej: "32,76")
            const sumaTotal = (valIva + valSub).toLocaleString('es-VE', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

            return {
                // --- CLÁSICO ---
                numero: f.nro_control || f.nro_notificacion || f.id_ventas || "00000",
                fecha: f.f_emision || "N/A",
                estado: f.status || "DESCONOCIDO",
                
                // 🎯 EL BLANCO FIJADO: Monto y Saldo ahora muestran la suma matemática exacta
                monto: sumaTotal,
                saldo: sumaTotal, 
                
                // --- NUEVO ---
                id_ventas: f.id_ventas || null,
                nro_fiscal: f.nro_fiscal || null,
                descripcion: f.descripcion || null,
                sub_total: f.sub_total || "0,00", 
                exento: f.exento || "0,00",
                base_imp: f.base_imp || "0,00",
                iva: f.iva || "0,00",
                total_fact_bsd: f.total_fact_bsd || "0,00",
                pago: f.pago || null,
                razon_social: f.razon_social || null,
                rif_fiscal: f.rif_fiscal || null
            };
        });

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
    let sucursalRequerida = "PRINCIPAL"; // Por defecto, asumimos Principal si no mandan nada

    if (req.method === 'POST') {
        if (req.path === '/pagar' && req.body && req.body.id) {
            idCliente = req.body.id;
            if (req.body.sucursal) sucursalRequerida = req.body.sucursal.toUpperCase();
        }
        else if (req.path === '/pagar-vidanet' && req.body && req.body.datos && req.body.datos.cedula) {
            idCliente = req.body.datos.cedula;
            if (req.body.sucursal) sucursalRequerida = req.body.sucursal.toUpperCase();
            else if (req.body.datos.sucursal) sucursalRequerida = req.body.datos.sucursal.toUpperCase();
        }
    } else if (req.method === 'GET') {
        if (req.query && req.query.id) idCliente = req.query.id;
        else if (req.query && req.query.cedula) idCliente = req.query.cedula;
        if (req.query && req.query.sucursal) sucursalRequerida = req.query.sucursal.toUpperCase();
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
            if (o.carga > 0) return false; 

            // 🎯 LA BARRERA DE JURISDICCIÓN: Solo elegimos obreros que pertenezcan a la sucursal pedida
            if (o.sucursal !== sucursalRequerida) return false;
            
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
                await esperarTurno(requestId, etiqueta, respuestaEnviada, sucursalRequerida); 
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

        // --- CORTACIRCUITOS DINÁMICO ---
        const limiteTiempo = 130000; // Todo lo que llega aquí es pesado (Pagos)

        try {
            obreroElegido.carga++;
            console.log(`  [>> REQ: ${requestId}] ${etiqueta}Intentando Obrero ${obreroElegido.id} (Intento ${intentos + 1}/3)`);

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
                        sistema: rMision.sistema, duracion: duracion, sucursal: sucursalRequerida
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
                            sistema: `${rMision.sistema} (RECHAZADO)`, duracion: duracion, sucursal: sucursalRequerida
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
                if (obreroElegido.activo) obreroElegido.cocinandoHasta = 0; // 💥 Matamos el candado de tiempo
            }

            if (COLA_DE_ESPERA.length > 0) {
                // 🎯 Despertamos al primero que vaya a la MISMA SUCURSAL
                let indexDespertar = 0;
                if (obreroElegido) {
                    const idx = COLA_DE_ESPERA.findIndex(c => c.sucursal === obreroElegido.sucursal);
                    if (idx !== -1) indexDespertar = idx;
                }
                
                const siguienteEnFila = COLA_DE_ESPERA.splice(indexDespertar, 1)[0];
                if (siguienteEnFila.timeoutCola) clearTimeout(siguienteEnFila.timeoutCola); 
                agregarLog(siguienteEnFila.requestId, 'INFO', `${siguienteEnFila.etiqueta}Saliendo de cola. Re-evaluando obreros de ${siguienteEnFila.sucursal}...`);
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
// 4.5. MOTOR DE COLA ACTIVA (VIGILANTE DE ALTA FRECUENCIA)
// ============================================================================
setInterval(() => {
    if (COLA_DE_ESPERA.length === 0) return; // Si no hay cola, no hacemos nada

    const ahora = Date.now();
    // Buscamos obreros que estén 100% libres y hayan terminado CUALQUIER cuenta regresiva
    const obrerosLibres = OBREROS.filter(o => o.activo && o.carga === 0 && ahora >= o.cocinandoHasta);

    for (let obrero of obrerosLibres) {
        if (COLA_DE_ESPERA.length === 0) break;
        
        // ¿Hay alguien en la cola esperando por la sucursal de este obrero?
        const idx = COLA_DE_ESPERA.findIndex(c => c.sucursal === obrero.sucursal);
        if (idx !== -1) {
            const estancado = COLA_DE_ESPERA.splice(idx, 1)[0];
            if (estancado.timeoutCola) clearTimeout(estancado.timeoutCola);
            
            agregarLog(estancado.requestId, 'INFO', `MOTOR DE COLA: Obrero ${obrero.id} detectado libre en las sombras. Despertando solicitud...`, obrero.id);
            
            // Lo marcamos ocupado artificialmente por medio segundo para que este loop no asigne a 2 personas al mismo obrero a la vez
            obrero.carga++; 
            setTimeout(() => { obrero.carga--; }, 500); 
            
            estancado.resolve(); // ¡DESPIERTA!
        }
    }
}, 2000); // Escanea la cola cada 2 segundos exactos

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
