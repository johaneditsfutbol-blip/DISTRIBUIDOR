const express = require('express');
const axios = require('axios');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// 🛡️ TU ESCUADRÓN DE OBREROS (Actualiza estas URLs cuando Railway te las dé)
const OBREROS = [
    { id: 1, url: 'https://obrero-1-production.up.railway.app', carga: 0, fallos: 0, activo: true },
    { id: 2, url: 'https://obrero-2-production.up.railway.app', carga: 0, fallos: 0, activo: true }
];

// 📊 RADAR DE MONITOREO (Entra a /status desde tu navegador para ver esto)
app.get('/status', (req, res) => {
    let html = `<h1>📡 Torre de Control - Estado del Escuadrón</h1><table border="1" cellpadding="10" style="font-family: monospace; text-align: left;">`;
    html += `<tr><th>ID</th><th>Estado</th><th>Carga Actual</th><th>Fallos Consecutivos</th><th>URL</th></tr>`;
    OBREROS.forEach(o => {
        const estado = o.activo ? '🟢 ACTIVO' : '🔴 CUARENTENA';
        html += `<tr><td>${o.id}</td><td>${estado}</td><td>${o.carga} peticiones</td><td>${o.fallos}</td><td>${o.url}</td></tr>`;
    });
    html += `</table><p>Actualiza la página para ver cambios en tiempo real.</p>`;
    res.send(html);
});

// 🧠 EL CEREBRO: Intercepta todas las demás rutas
app.all('*', async (req, res) => {
    // Si la ruta es favicon, la ignoramos para no generar carga basura
    if (req.originalUrl === '/favicon.ico') return res.status(204).end();

    let intentos = 0;
    let exito = false;
    let errorFinal = null;

    while (intentos < 3 && !exito) {
        // 1. Filtrar obreros activos
        const obrerosActivos = OBREROS.filter(o => o.activo);
        
        if (obrerosActivos.length === 0) {
            return res.status(503).json({ success: false, message: "CRÍTICO: Todos los obreros están caídos o en cuarentena." });
        }

        // 2. Elegir al que tenga MENOR CARGA
        const obreroElegido = obrerosActivos.reduce((prev, curr) => (prev.carga < curr.carga ? prev : curr));

        try {
            // AUMENTAR CARGA
            obreroElegido.carga++;
            console.log(`[>>] Enviando ${req.method} ${req.originalUrl} al Obrero ${obreroElegido.id} (Carga: ${obreroElegido.carga})`);

            // 3. Reenviar la petición exactamente como vino
            const respuesta = await axios({
                method: req.method,
                url: `${obreroElegido.url}${req.originalUrl}`,
                data: req.method !== 'GET' ? req.body : undefined,
                headers: { 'Content-Type': 'application/json' },
                timeout: 120000 // 2 minutos máximo de espera
            });

            // 4. Si llegamos aquí, fue un éxito rotundo
            obreroElegido.fallos = 0; // Se resetean sus pecados
            res.status(respuesta.status).json(respuesta.data);
            exito = true;

        } catch (error) {
            intentos++;
            obreroElegido.fallos++;
            errorFinal = error.response ? error.response.data : error.message;
            
            console.error(`[❌] Fallo Obrero ${obreroElegido.id} (Intento ${intentos}/3). Razón: ${error.message}`);

            // 5. MODO PÁNICO: Si falla 3 veces seguidas, lo mandamos a dormir
            if (obreroElegido.fallos >= 3) {
                console.log(`[🚨] CIRCUIT BREAKER: Obrero ${obreroElegido.id} puesto en CUARENTENA por 5 minutos.`);
                obreroElegido.activo = false;
                
                // Programar su resurrección automática
                setTimeout(() => {
                    obreroElegido.activo = true;
                    obreroElegido.fallos = 0;
                    console.log(`[♻️] RESURRECCIÓN: Obrero ${obreroElegido.id} vuelve al campo de batalla.`);
                }, 300000); // 5 minutos (300,000 ms)
            }
        } finally {
            // BAJAR CARGA (Pase lo que pase, el obrero se desocupó)
            obreroElegido.carga--;
        }
    }

    // Si después de 3 intentos con distintos obreros no se logró:
    if (!exito) {
        res.status(500).json({ 
            success: false, 
            message: "Icarosoft está inestable. Reintentos agotados.", 
            detalle: errorFinal 
        });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`\n======================================`);
    console.log(`🚀 COMANDANTE INICIADO EN PUERTO ${PORT}`);
    console.log(`======================================\n`);
});
