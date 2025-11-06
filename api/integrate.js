// api/integrate.js

const { parse } = require('csv-parse/sync');

const DATA_SOURCES = {
  Temperatura_Observaciones:
    "https://catalogodatos.gub.uy/dataset/accd0e24-76be-4101-904b-81bb7d41ee88/resource/f800fc53-556b-4d1c-8bd6-28b41f9cf146/download/inumet_temperatura_del_aire.csv",
  Humedad_Observaciones:
    "https://catalogodatos.gub.uy/dataset/5f4f50ac-2d11-4863-8ef2-b500d5f3aa90/resource/97ee0df8-3407-433f-b9f7-6e5a2d95ad25/download/inumet_humedad_relativa.csv",
  Precipitacion_Acumulada:
    "https://catalogodatos.gub.uy/dataset/fd896b11-4c04-4807-bae4-5373d65beea2/resource/ca987721-6052-4bb8-8596-2a5ad9630639/download/inumet_precipitacion_acumulada_horaria.csv",
  Produccion_Manzanas:
    "https://docs.google.com/spreadsheets/d/1AzJs_mNWoFXHN81HO0iT2u-WoZmoMz1K/export?format=csv",
  Ubicacion_Estaciones_Scraping:
    "https://www.inumet.gub.uy/tiempo/estaciones-meteorologicas-automaticas"
};

const delay = (ms) => new Promise((res) => setTimeout(res, ms));

async function loadAndNormalizeSource(sourceName, url) {
  console.log(`Loading source: ${sourceName} from ${url}`);

  const response = await fetch(url);
  if (!response.ok) {
    console.warn(
      `HTTP ${response.status} loading ${sourceName} (${url})`
    );
    return [];
  }

  const text = await response.text();

  if (/<html/i.test(text)) {
    console.warn(`HTML received instead of CSV for ${sourceName}`);
    return [];
  }

  let records;
  try {
    records = parse(text, {
      delimiter: ";",
      columns: true,
      skip_empty_lines: true
    });
    if (records.length && Object.keys(records[0]).length === 1) {
      throw new Error("Only one column with ';' → retry with ','");
    }
  } catch {
    records = parse(text, {
      delimiter: ",",
      columns: true,
      skip_empty_lines: true
    });
  }

  const normalized = records.map((row) => {
    const newRow = {};
    for (const key in row) {
      const newKey = key
        .toLowerCase()
        .trim()
        .replace(/[^a-z0-9_]/gi, "_");
      newRow[newKey] = row[key];
    }
    newRow.origen_fuente = sourceName;
    return newRow;
  });

  console.log(`Loaded ${normalized.length} rows for ${sourceName}`);
  return normalized;
}

async function scrapeStationLocations(sourceName, url) {
  console.log(`Scraping ${sourceName} from ${url}`);

  const response = await fetch(url);
  if (!response.ok) {
    console.warn(
      `HTTP ${response.status} scraping ${sourceName} (${url})`
    );
    return [];
  }

  const html = await response.text();
  const match = html.match(/var estaciones\s*=\s*(\{.*?\});/s);

  if (!match) {
    console.warn("Could not find 'var estaciones = ...' in HTML");
    return [];
  }

  let data;
  try {
    data = JSON.parse(match[1]);
  } catch (e) {
    console.warn("Failed to parse estaciones JSON", e);
    return [];
  }

  if (!data.estaciones || !Array.isArray(data.estaciones)) {
    console.warn("Key 'estaciones' missing or not an array");
    return [];
  }

  const arr = data.estaciones.map((obj) => {
    const newObj = {};
    for (const key in obj) {
      const newKey = key
        .toLowerCase()
        .trim()
        .replace(/[^a-z0-9_]/gi, "_");
      newObj[newKey] = obj[key];
    }
    newObj.origen_fuente = sourceName;
    return newObj;
  });

  console.log(`Scraped ${arr.length} stations for ${sourceName}`);
  return arr;
}

// Node-style Vercel function: /api/integrate
module.exports = async (req, res) => {
  if (req.method && req.method !== "GET") {
    res.statusCode = 405;
    res.setHeader("Allow", "GET");
    return res.end("Method Not Allowed");
  }

  const integrated = {};

  try {
    for (const [sourceName, url] of Object.entries(DATA_SOURCES)) {
      let dataRows = [];
      if (sourceName.includes("Scraping")) {
        dataRows = await scrapeStationLocations(sourceName, url);
      } else {
        dataRows = await loadAndNormalizeSource(sourceName, url);
      }

      if (dataRows.length > 0) {
        integrated[sourceName] = dataRows;
      } else {
        console.warn(`Source empty or error: ${sourceName}`);
      }

      // simple throttling to be amable with data providers
      await delay(1000);
    }

    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json");
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.end(JSON.stringify(integrated));
  } catch (err) {
    console.error("Integration error", err);
    res.statusCode = 500;
    res.setHeader("Content-Type", "application/json");
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Headers", "*");
    res.end(
      JSON.stringify({
        error: "Error al integrar fuentes",
        detail: String(err && err.message ? err.message : err)
      })
    );
  }
};
