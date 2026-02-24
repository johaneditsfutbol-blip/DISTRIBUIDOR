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

// EL CEREBRO: Intercepta todas las rutas
app.all('*', async (req, res) => {
    if (req.originalUrl === '/favicon.ico') return res.status(204).end();

    let intentos = 0;
    let exito = false;
    let errorFinal = null;
    const inicioReloj = Date.now();

    // NUEVO CAMBIO 1: La lista negra temporal para esta petición
    let obrerosDescartados = []; 

    // LOG DE ENTRADA
    console.log(`\n--- 📥 NUEVA SOLICITUD ---`);
    console.log(`Método: ${req.method} | Ruta: ${req.originalUrl}`);
    if (req.method !== 'GET' && Object.keys(req.body).length > 0) {
        console.log(`Body App: ${JSON.stringify(req.body)}`);
    }

    while (intentos < 3 && !exito) {
        // NUEVO CAMBIO 2: Filtramos activos Y que no hayan fallado en esta misma petición
        const obrerosDisponibles = OBREROS.filter(o => o.activo && !obrerosDescartados.includes(o.id));
        
        if (obrerosDisponibles.length === 0) {
            console.error(`[🔥] ERROR CRÍTICO: No hay obreros disponibles o todos fallaron.`);
            return res.status(503).json({ success: false, message: "CRÍTICO: Todos los obreros están caídos o fallaron esta petición." });
        }

        // NUEVO CAMBIO 3: La Ruleta Rusa para desempatar (Cura de la obsesión)
        const menorCarga = Math.min(...obrerosDisponibles.map(o => o.carga));
        const empatados = obrerosDisponibles.filter(o => o.carga === menorCarga);
        const obreroElegido = empatados[Math.floor(Math.random() * empatados.length)];

        try {
            obreroElegido.carga++;
            console.log(`[>>] REDIRECCIONANDO -> Obrero ${obreroElegido.id} (Carga actual: ${obreroElegido.carga} | Intento ${intentos + 1}/3)`);

            const respuesta = await axios({
                method: req.method,
                url: `${obreroElegido.url}${req.originalUrl}`,
                data: req.method !== 'GET' ? req.body : undefined,
                headers: { 'Content-Type': 'application/json' },
                timeout: 120000 
            });

            // LOG DE SALIDA
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
            
            // NUEVO CAMBIO 4: El Castigo (Se va a la lista negra temporal)
            obrerosDescartados.push(obreroElegido.id);

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
