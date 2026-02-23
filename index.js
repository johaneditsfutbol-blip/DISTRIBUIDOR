const express = require('express');
const axios = require('axios');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// 🛡️ TU ESCUADRÓN DE OBREROS (Asegúrate de que las URLs sean las correctas)
const OBREROS = [
    { id: 1, url: 'https://obrero-1-production.up.railway.app', carga: 0, fallos: 0, activo: true },
    { id: 2, url: 'https://obrero-2-production.up.railway.app', carga: 0, fallos: 0, activo: true },
    { id: 2, url: 'https://obrero-3-1-production.up.railway.app', carga: 0, fallos: 0, activo: true },
    { id: 2, url: 'https://obrero-4-2-production.up.railway.app', carga: 0, fallos: 0, activo: true },
    { id: 2, url: 'https://obrero-5-3-production.up.railway.app', carga: 0, fallos: 0, activo: true },
    { id: 2, url: 'https://obrero-6-4-production.up.railway.app', carga: 0, fallos: 0, activo: true },
    { id: 2, url: 'https://obrero-7-5-production.up.railway.app', carga: 0, fallos: 0, activo: true }
];

// 📊 RADAR DE MONITOREO
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

// 🧠 EL CEREBRO: Intercepta todas las rutas
app.all('*', async (req, res) => {
    if (req.originalUrl === '/favicon.ico') return res.status(204).end();

    let intentos = 0;
    let exito = false;
    let errorFinal = null;
    const inicioReloj = Date.now();

    // 🕵️ LOG DE ENTRADA (Lo que viene de la App)
    console.log(`\n--- 📥 NUEVA SOLICITUD ---`);
    console.log(`Método: ${req.method} | Ruta: ${req.originalUrl}`);
    if (req.method !== 'GET' && Object.keys(req.body).length > 0) {
        console.log(`Body App: ${JSON.stringify(req.body)}`);
    }

    while (intentos < 3 && !exito) {
        const obrerosActivos = OBREROS.filter(o => o.activo);
        
        if (obrerosActivos.length === 0) {
            console.error(`[🔥] ERROR CRÍTICO: No hay obreros disponibles.`);
            return res.status(503).json({ success: false, message: "CRÍTICO: Todos los obreros están caídos." });
        }

        // Elegir al que tenga MENOR CARGA
        const obreroElegido = obrerosActivos.reduce((prev, curr) => (prev.carga < curr.carga ? prev : curr));

        try {
            obreroElegido.carga++;
            console.log(`[>>] REDIRECCIONANDO -> Obrero ${obreroElegido.id} (Carga actual: ${obreroElegido.carga})`);

            const respuesta = await axios({
                method: req.method,
                url: `${obreroElegido.url}${req.originalUrl}`,
                data: req.method !== 'GET' ? req.body : undefined,
                headers: { 'Content-Type': 'application/json' },
                timeout: 120000 
            });

            // 🕵️ LOG DE SALIDA (Lo que devuelve el Obrero)
            const duracion = Date.now() - inicioReloj;
            const dataString = JSON.stringify(respuesta.data);
            const preview = dataString.length > 250 ? dataString.substring(0, 250) + "... [Truncado]" : dataString;

            console.log(`[<<] ✅ ÉXITO Obrero ${obreroElegido.id} en ${duracion}ms`);
            console.log(`     Status: ${respuesta.status}`);
            console.log(`     Respuesta: ${preview}`);
            console.log(`--------------------------`);

            obreroElegido.fallos = 0;
            res.status(respuesta.status).json(respuesta.data);
            exito = true;

        } catch (error) {
            intentos++;
            obreroElegido.fallos++;
            
            const errorDetalle = error.response ? JSON.stringify(error.response.data) : error.message;
            console.error(`[❌] FALLO Obrero ${obreroElegido.id} (Intento ${intentos}/3)`);
            console.error(`     Error: ${errorDetalle}`);

            if (obreroElegido.fallos >= 3) {
                console.log(`[🚨] CIRCUIT BREAKER: Obrero ${obreroElegido.id} entra en CUARENTENA.`);
                obreroElegido.activo = false;
                setTimeout(() => {
                    obreroElegido.activo = true;
                    obreroElegido.fallos = 0;
                    console.log(`[♻️] RESURRECCIÓN: Obrero ${obreroElegido.id} vuelve al servicio.`);
                }, 300000);
            }
            errorFinal = errorDetalle;
        } finally {
            obreroElegido.carga--;
        }
    }

    if (!exito) {
        console.error(`[💀] SOLICITUD FALLIDA tras 3 intentos.`);
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
    console.log(`🚀 COMANDANTE V2`);
    console.log(`📡 Puerto: ${PORT}`);
    console.log(`🤖 Obreros: ${OBREROS.length}`);
    console.log(`======================================\n`);
});
