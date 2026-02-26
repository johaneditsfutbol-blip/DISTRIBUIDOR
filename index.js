const express = require('express');
const axios = require('axios');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// 🛡️ ESCUADRÓN DE OBREROS
const OBREROS = [
    { id: 1, url: 'https://obrero-1-production.up.railway.app', carga: 0, fallos: 0, activo: true },
    { id: 2, url: 'https://obrero-2-production.up.railway.app', carga: 0, fallos: 0, activo: true },
    { id: 3, url: 'https://obrero-3-1-production.up.railway.app', carga: 0, fallos: 0, activo: true },
    { id: 4, url: 'https://obrero-4-2-production.up.railway.app', carga: 0, fallos: 0, activo: true },
    { id: 5, url: 'https://obrero-5-3-production.up.railway.app', carga: 0, fallos: 0, activo: true },
    { id: 6, url: 'https://obrero-6-4-production.up.railway.app', carga: 0, fallos: 0, activo: true },
    { id: 7, url: 'https://obrero-7-5-production.up.railway.app', carga: 0, fallos: 0, activo: true }
];

// RADAR DE MONITOREO
app.get('/status', (req, res) => {
    let html = `<h1 style="font-family: sans-serif;">📡 Torre de Control - Estado del Escuadrón</h1><table border="1" cellpadding="10" style="font-family: monospace; text-align: left; border-collapse: collapse;">`;
    html += `<tr style="background: #eee;"><th>ID</th><th>Estado</th><th>Carga Actual</th><th>Fallos Consecutivos</th><th>URL</th></tr>`;
    OBREROS.forEach(o => {
        const estado = o.activo ? '<span style="color: green;">🟢 ACTIVO</span>' : '<span style="color: red;">🔴 CUARENTENA</span>';
        html += `<tr><td>${o.id}</td><td>${estado}</td><td>${o.carga} peticiones</td><td>${o.fallos}</td><td>${o.url}</td></tr>`;
    });
    html += `</table><p>Actualiza la página para ver cambios en tiempo real.</p>`;
    res.send(html);
});

// --- HELPER PARA LOGS ELEGANTES ---
const formatoLog = (titulo, objeto) => {
    try {
        const str = JSON.stringify(objeto, null, 2);
        // Si es muy largo, no lo truncamos a lo bestia, mostramos las llaves principales
        if (str.length > 500) {
            return `\n    └─ ${titulo}: { ... [Objeto Grande] Claves: ${Object.keys(objeto).join(', ')} ... }`;
        }
        // Formato limpio con tabulaciones
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

    // La lista negra temporal para esta petición
    let obrerosDescartados = []; 

    // LOG DE ENTRADA (Refinado)
    const requestId = Math.random().toString(36).substring(2, 7).toUpperCase();
    console.log(`\n======================================================`);
    console.log(`📥 [REQ: ${requestId}] NUEVA SOLICITUD: ${req.method} ${req.originalUrl}`);
    
    if (req.method !== 'GET' && Object.keys(req.body).length > 0) {
        // Mostramos un resumen del body, no todo el chorizo
        const resumenBody = { ...req.body };
        // Si pasas fotos en base64 u otros datos masivos, escóndelos en el log
        if (resumenBody.datos && resumenBody.datos.rutaImagen) {
             resumenBody.datos.rutaImagen = "[IMAGEN OMITIDA EN LOG]";
        }
        console.log(formatoLog("Body Recibido", resumenBody));
    }
    console.log(`======================================================`);

    while (intentos < 3 && !exito) {
        // Filtramos activos Y que no hayan fallado en esta misma petición
        const obrerosDisponibles = OBREROS.filter(o => o.activo && !obrerosDescartados.includes(o.id));
        
        if (obrerosDisponibles.length === 0) {
            console.error(`\n[🔥 REQ: ${requestId}] ERROR CRÍTICO: No hay obreros disponibles o todos fallaron.`);
            return res.status(503).json({ success: false, message: "CRÍTICO: Todos los obreros están caídos o fallaron esta petición." });
        }

        // La Ruleta Rusa para desempatar (Cura de la obsesión)
        const menorCarga = Math.min(...obrerosDisponibles.map(o => o.carga));
        const empatados = obrerosDisponibles.filter(o => o.carga === menorCarga);
        const obreroElegido = empatados[Math.floor(Math.random() * empatados.length)];

        try {
            obreroElegido.carga++;
            console.log(`\n  [>>] REDIRECCIONANDO [Intento ${intentos + 1}/3] -> Obrero ${obreroElegido.id}`);

            const respuesta = await axios({
                method: req.method,
                url: `${obreroElegido.url}${req.originalUrl}`,
                data: req.method !== 'GET' ? req.body : undefined,
                headers: { 'Content-Type': 'application/json' },
                timeout: 120000 
            });

            // LOG DE SALIDA (Refinado)
            const duracion = Date.now() - inicioReloj;
            console.log(`  [<<] ✅ ÉXITO Obrero ${obreroElegido.id} (Tardó: ${duracion}ms)`);
            
            // Verificamos si la respuesta indica éxito (ej: success: true)
            const mensajeAviso = respuesta.data.success ? "Operación Completada" : "Respuesta Recibida (Validar Data)";
            console.log(`       Status: ${respuesta.status} | ${mensajeAviso}`);
            
            // Logeamos la respuesta de forma elegante
            if(respuesta.data) {
                console.log(formatoLog("Data Respuesta", respuesta.data));
            }
            
            console.log(`------------------------------------------------------`);

            obreroElegido.fallos = 0;
            res.status(respuesta.status).json(respuesta.data);
            exito = true;

} catch (error) {
            // Pre-procesamos los datos del error para usarlos en ambas lógicas
            const statusError = error.response ? error.response.status : 500; // Por defecto 500 si es timeout/red
            let mensajeErrorLog = error.message;
            if(error.response && error.response.data) {
                 mensajeErrorLog = typeof error.response.data === 'object' ? JSON.stringify(error.response.data) : error.response.data;
            }

            // ====================================================================
            // 🔥 NUEVO: EXCEPCIÓN TÁCTICA PARA CONSULTA VIDANET 🔥
            // Usamos req.path para ignorar parámetros GET (?letra=V&cedula=...)
            // ====================================================================
            if (req.path === '/consultar-deudas-vidanet') {
                console.log(`\n  [⚠️] INFO VIDANET: Error de consulta en Obrero ${obreroElegido.id}.`);
                console.log(`       Motivo: ${mensajeErrorLog.substring(0, 150)}`);
                console.log(`       Acción: Retornando error al cliente. Sin castigos. Sin reintentos.`);
                
                const dataRespuesta = error.response && error.response.data 
                    ? error.response.data 
                    : { success: false, error: error.message };
                
                // Hacemos RETURN. Esto corta el ciclo "while" de inmediato,
                // devuelve la respuesta al cliente, PERO el bloque "finally" 
                // se sigue ejecutando para restar la carga del obrero.
                return res.status(statusError).json(dataRespuesta);
            }
            // ====================================================================


            // --- LÓGICA NORMAL DE CASTIGO PARA EL RESTO DE RUTAS ---
            intentos++;
            obreroElegido.fallos++;
            
            // El Castigo (Se va a la lista negra temporal de esta solicitud)
            obrerosDescartados.push(obreroElegido.id);

            // LOG DE FALLO 
            console.error(`\n  [❌] FALLO Obrero ${obreroElegido.id} (Intento ${intentos}/3)`);
            console.error(`       Status: ${statusError}`);
            console.error(`       Motivo: ${mensajeErrorLog.substring(0, 150)}${mensajeErrorLog.length > 150 ? '...' : ''}`);

            // --- PROTOCOLO DE AUTODESTRUCCIÓN (TOLERANCIA CERO: 2 FALLOS) ---
            if (obreroElegido.fallos >= 2) {
                console.log(`\n  [🚨] CIRCUIT BREAKER: Obrero ${obreroElegido.id} entra en CUARENTENA (Alcanzó ${obreroElegido.fallos} fallos).`);
                obreroElegido.activo = false;

                console.log(`  [🔫] Enviando orden de AUTODESTRUCCIÓN al Obrero ${obreroElegido.id}...`);
                
                axios.post(`${obreroElegido.url}/orden-66`, {}, {
                    headers: { 'x-comandante-secret': 'IcaroSoft_Destruccion_Inminente_2026' }, 
                    timeout: 5000 
                }).catch(() => {
                    console.log(`  [🤷] El Obrero ${obreroElegido.id} no pudo ni recibir la orden (posiblemente colapso total).`);
                });

                // Esperamos 30 segundos y lo revivimos 
                setTimeout(() => {
                    obreroElegido.activo = true;
                    obreroElegido.fallos = 0;
                    console.log(`\n  [♻️] RESURRECCIÓN: Fin de cuarentena (30s). Obrero ${obreroElegido.id} vuelve al servicio activo.`);
                }, 30000); 
            }
            errorFinal = mensajeErrorLog;

        } finally {
            obreroElegido.carga--;
        }
    }

    if (!exito) {
        console.error(`\n[💀 REQ: ${requestId}] SOLICITUD FALLIDA tras 3 intentos con distintos obreros.`);
        console.log(`======================================================\n`);
        res.status(500).json({ 
            success: false, 
            message: "Icarosoft está inestable. Reintentos agotados.",
            detalle: errorFinal
        });
    }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`\n======================================`);
    console.log(`🚀 COMANDANTE V2.5 (INTOLERANCIA)`);
    console.log(`📡 Puerto: ${PORT}`);
    console.log(`🤖 Obreros: ${OBREROS.length}`);
    console.log(`======================================\n`);
});
