const express = require('express');
const axios = require('axios');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// 🛡️ ESCUADRÓN DE OBREROS (Armería con variables de estado)
const OBREROS = [
    { id: 1, url: 'https://obrero-1-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false },
    { id: 2, url: 'https://obrero-2-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false },
    { id: 3, url: 'https://obrero-3-1-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false },
    { id: 4, url: 'https://obrero-4-2-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false },
    { id: 5, url: 'https://obrero-5-3-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false },
    { id: 6, url: 'https://obrero-6-4-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false },
    { id: 7, url: 'https://obrero-7-5-production.up.railway.app', carga: 0, fallos: 0, activo: true, cocinandoHasta: 0, buscandoServicios: false }
];

// RADAR DE MONITOREO (Visualización en tiempo real)
app.get('/status', (req, res) => {
    let html = `<h1 style="font-family: sans-serif;">📡 Torre de Control - Estado del Escuadrón</h1><table border="1" cellpadding="10" style="font-family: monospace; text-align: left; border-collapse: collapse;">`;
    html += `<tr style="background: #eee;"><th>ID</th><th>Estado</th><th>Carga Actual</th><th>Fallos Consecutivos</th><th>URL</th></tr>`;
    const ahora = Date.now();
    
    OBREROS.forEach(o => {
        let infoExtra = '';
        if (o.cocinandoHasta > ahora) {
            const segRestantes = Math.ceil((o.cocinandoHasta - ahora) / 1000);
            infoExtra += ` <br><b style="color: #d97706;">[🍳 Cocinando: ${segRestantes}s]</b>`;
        }
        if (o.buscandoServicios) {
            infoExtra += ` <br><b style="color: #2563eb;">[🔍 Ocupado: Servicios]</b>`;
        }
        
        const estado = o.activo ? `<span style="color: green;">🟢 ACTIVO</span>${infoExtra}` : '<span style="color: red;">🔴 CUARENTENA</span>';
        html += `<tr><td>${o.id}</td><td>${estado}</td><td>${o.carga} peticiones</td><td>${o.fallos}</td><td>${o.url}</td></tr>`;
    });
    html += `</table><p>Actualiza la página para ver cambios en tiempo real.</p>`;
    res.send(html);
});

// --- HELPER PARA LOGS ELEGANTES ---
const formatoLog = (titulo, objeto) => {
    try {
        const str = JSON.stringify(objeto, null, 2);
        if (str.length > 500) {
            return `\n    └─ ${titulo}: { ... [Objeto Grande] Claves: ${Object.keys(objeto).join(', ')} ... }`;
        }
        return `\n    └─ ${titulo}:\n${str.split('\n').map(l => `        ${l}`).join('\n')}`;
    } catch (e) {
        return `\n    └─ ${titulo}: [No se pudo parsear el objeto]`;
    }
};

// EL CEREBRO: Intercepta todas las rutas
app.all('*', async (req, res) => {
    if (req.originalUrl === '/favicon.ico') return res.status(204).end();

    let intentos = 0;
    let exito = false;
    let errorFinal = null;
    const inicioReloj = Date.now();

    // La lista negra temporal para esta petición específica
    let obrerosDescartados = []; 

    // LOG DE ENTRADA
    const requestId = Math.random().toString(36).substring(2, 7).toUpperCase();
    console.log(`\n======================================================`);
    console.log(`📥 [REQ: ${requestId}] NUEVA SOLICITUD: ${req.method} ${req.originalUrl}`);
    
    if (req.method !== 'GET' && Object.keys(req.body).length > 0) {
        const resumenBody = { ...req.body };
        if (resumenBody.datos && resumenBody.datos.rutaImagen) {
             resumenBody.datos.rutaImagen = "[IMAGEN OMITIDA EN LOG]";
        }
        console.log(formatoLog("Body Recibido", resumenBody));
    }
    console.log(`======================================================`);

    while (intentos < 3 && !exito) {
        // --- 1. FILTRO AVANZADO DE DISPONIBILIDAD ---
        const ahora = Date.now();
        const obrerosDisponibles = OBREROS.filter(o => {
            // Descartados por fallar previamente en esta misma request o en cuarentena
            if (!o.activo || obrerosDescartados.includes(o.id)) return false;
            
            // REGLA A: "Let Him Cook" (Si está en el periodo de gracia, lo ignoramos)
            if (ahora < o.cocinandoHasta) return false;
            
            // REGLA B: "Anti-Entrelazamiento" (Si pide servicios y ya está ocupado en eso)
            if (req.path === '/buscar-servicios' && o.buscandoServicios) return false;
            
            return true;
        });
        
        if (obrerosDisponibles.length === 0) {
            console.error(`\n[🔥 REQ: ${requestId}] ERROR CRÍTICO: No hay obreros disponibles (todos caídos, saturados o cocinando).`);
            return res.status(503).json({ success: false, message: "CRÍTICO: Escuadrón colapsado o todos los obreros están en operaciones exclusivas." });
        }

        // Desempate (Ruleta Rusa entre los menos cargados)
        const menorCarga = Math.min(...obrerosDisponibles.map(o => o.carga));
        const empatados = obrerosDisponibles.filter(o => o.carga === menorCarga);
        const obreroElegido = empatados[Math.floor(Math.random() * empatados.length)];

        try {
            obreroElegido.carga++;
            
            // --- 2. CANDADO DE SERVICIOS PRE-PETICIÓN ---
            if (req.path === '/buscar-servicios') {
                obreroElegido.buscandoServicios = true;
                console.log(`  [🔒] CANDADO: Obrero ${obreroElegido.id} bloqueado para múltiples /buscar-servicios.`);
            }

            console.log(`\n  [>>] REDIRECCIONANDO [Intento ${intentos + 1}/3] -> Obrero ${obreroElegido.id}`);

            const respuesta = await axios({
                method: req.method,
                url: `${obreroElegido.url}${req.originalUrl}`,
                data: req.method !== 'GET' ? req.body : undefined,
                headers: { 'Content-Type': 'application/json' },
                timeout: 120000 
            });

            // LOG DE SALIDA
            const duracion = Date.now() - inicioReloj;
            console.log(`  [<<] ✅ ÉXITO Obrero ${obreroElegido.id} (Tardó: ${duracion}ms)`);
            
            const mensajeAviso = respuesta.data.success ? "Operación Completada" : "Respuesta Recibida";
            console.log(`       Status: ${respuesta.status} | ${mensajeAviso}`);
            if(respuesta.data) console.log(formatoLog("Data Respuesta", respuesta.data));
            console.log(`------------------------------------------------------`);

            // --- 3. TIEMPOS DE COCCIÓN (LET HIM COOK) ---
            if (req.path === '/pagar') {
                obreroElegido.cocinandoHasta = Date.now() + 35000; // 35 segundos
                console.log(`  [🍳] LET HIM COOK: Obrero ${obreroElegido.id} respondió el POST /pagar. Ignorado por 35s.`);
            } else if (req.path === '/pagar-vidanet') {
                obreroElegido.cocinandoHasta = Date.now() + 15000; // 15 segundos
                console.log(`  [🍳] LET HIM COOK: Obrero ${obreroElegido.id} respondió el POST /pagar-vidanet. Ignorado por 15s.`);
            }

            obreroElegido.fallos = 0;
            res.status(respuesta.status).json(respuesta.data);
            exito = true;

        } catch (error) {
            const statusError = error.response ? error.response.status : 500; 
            let mensajeErrorLog = error.message;
            if(error.response && error.response.data) {
                 mensajeErrorLog = typeof error.response.data === 'object' ? JSON.stringify(error.response.data) : error.response.data;
            }

            // --- EXCEPCIÓN TÁCTICA PARA CONSULTAS (Sin castigo) ---
            if (req.path === '/consultar-deudas-vidanet') {
                console.log(`\n  [⚠️] INFO VIDANET: Error de consulta en Obrero ${obreroElegido.id}.`);
                console.log(`       Motivo: ${mensajeErrorLog.substring(0, 150)}`);
                const dataRespuesta = error.response && error.response.data ? error.response.data : { success: false, error: error.message };
                return res.status(statusError).json(dataRespuesta);
            }

            // Lógica normal de fallo
            intentos++;
            obreroElegido.fallos++;
            obrerosDescartados.push(obreroElegido.id); // Lo tachamos para el próximo intento de este bucle

            console.error(`\n  [❌] FALLO Obrero ${obreroElegido.id} (Intento ${intentos}/3)`);
            console.error(`       Status: ${statusError}`);
            console.error(`       Motivo: ${mensajeErrorLog.substring(0, 150)}${mensajeErrorLog.length > 150 ? '...' : ''}`);

            // --- PROTOCOLO DE AUTODESTRUCCIÓN ---
            if (obreroElegido.fallos >= 2) {
                console.log(`\n  [🚨] CIRCUIT BREAKER: Obrero ${obreroElegido.id} entra en CUARENTENA (2 fallos).`);
                obreroElegido.activo = false;
                console.log(`  [🔫] Enviando orden de AUTODESTRUCCIÓN al Obrero ${obreroElegido.id}...`);
                
                axios.post(`${obreroElegido.url}/orden-66`, {}, { headers: { 'x-comandante-secret': 'IcaroSoft_Destruccion_Inminente_2026' }, timeout: 5000 }).catch(() => {});

                setTimeout(() => {
                    obreroElegido.activo = true;
                    obreroElegido.fallos = 0;
                    console.log(`\n  [♻️] RESURRECCIÓN: Fin de cuarentena (30s). Obrero ${obreroElegido.id} vuelve al servicio activo.`);
                }, 30000); 
            }
            errorFinal = mensajeErrorLog;

        } finally {
            // Esto siempre se ejecuta al salir del try/catch (pase lo que pase)
            if (obreroElegido) {
                obreroElegido.carga--;
                
                // --- 4. LIBERACIÓN DEL CANDADO DE SERVICIOS ---
                if (req.path === '/buscar-servicios') {
                    obreroElegido.buscandoServicios = false;
                    console.log(`  [🔓] CANDADO LIBERADO: Obrero ${obreroElegido.id} terminó su operación de servicios.`);
                }
            }
        }
    }

    // Si salimos del while y no hubo éxito, quemamos la nave
    if (!exito) {
        console.error(`\n[💀 REQ: ${requestId}] SOLICITUD FALLIDA tras 3 intentos con distintos obreros.`);
        console.log(`======================================================\n`);
        res.status(500).json({ 
            success: false, 
            message: "El escuadrón está inestable. Reintentos agotados.",
            detalle: errorFinal
        });
    }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`\n======================================`);
    console.log(`🚀 COMANDANTE V3 (TÁCTICO)`);
    console.log(`📡 Puerto: ${PORT}`);
    console.log(`🤖 Obreros: ${OBREROS.length}`);
    console.log(`======================================\n`);
});
