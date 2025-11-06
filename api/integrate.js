// file: netlify/functions/integrate.js (for example)
import fetch from 'node-fetch';
import { parse } from 'csv-parse/sync';   // synchronous parse for simplicity

const DATA_SOURCES = {
  Temperatura_Observaciones: "https://…/inumet_temperatura_del_aire.csv",
  Humedad_Observaciones: "https://…/inumet_humedad_relativa.csv",
  Precipitacion_Acumulada: "https://…/inumet_precipitacion_acumulada_horaria.csv",
  Produccion_Manzanas: "https://docs.google.com/spreadsheets/…/export?format=csv",
  Ubicacion_Estaciones_Scraping: "https://www.inumet.gub.uy/tiempo/estaciones-meteorologicas-automaticas"
};

const delay = (ms) => new Promise(res => setTimeout(res, ms));

const loadAndNormalizeSource = async (sourceName, url) => {
  console.log(`Loading source: ${sourceName} from ${url}`);
  const response = await fetch(url, { timeout: 60000 });
  if (!response.ok) {
    console.warn(`Warning: HTTP status ${response.status} for source ${sourceName}`);
    return [];
  }
  const text = await response.text();

  if (/<html/i.test(text)) {
    console.warn(`Server returned HTML rather than CSV for ${sourceName}`);
    return [];
  }

  // Try parsing with semicolon, then comma
  let records;
  try {
    records = parse(text, { delimiter: ';', columns: true, skip_empty_lines: true });
    if (records.length && Object.keys(records[0]).length === 1) {
      throw new Error("only one column with ; delim → retrying with comma");
    }
  } catch (err) {
    records = parse(text, { delimiter: ',', columns: true, skip_empty_lines: true });
  }

  // Normalize column names and add source name
  const normalized = records.map(row => {
    const newRow = {};
    for (const key in row) {
      const newKey = key
        .toLowerCase()
        .trim()
        .replace(/[^a‐z0‐9_]/gi, '_');
      newRow[newKey] = row[key];
    }
    newRow.origin_source = sourceName;
    return newRow;
  });

  console.log(`Loaded ${normalized.length} rows for ${sourceName}`);
  return normalized;
};

const scrapeStationLocations = async (sourceName, url) => {
  console.log(`Scraping ${sourceName} from ${url}`);
  const response = await fetch(url, { timeout: 60000 });
  if (!response.ok) {
    console.warn(`Warning: HTTP status ${response.status} for scraping source ${sourceName}`);
    return [];
  }
  const html = await response.text();

  const match = html.match(/var estaciones\s*=\s*(\{.*?\});/s);
  if (!match) {
    console.warn("Could not find var estaciones = … in HTML");
    return [];
  }
  const data = JSON.parse(match[1]);
  if (!data.estaciones) {
    console.warn("Key 'estaciones' not found in scraped data");
    return [];
  }

  const arr = data.estaciones.map(obj => {
    const newObj = {};
    for (const key in obj) {
      const newKey = key.toLowerCase().trim().replace(/[^a‐z0‐9_]/gi, '_');
      newObj[newKey] = obj[key];
    }
    newObj.origin_source = sourceName;
    return newObj;
  });

  console.log(`Scraped ${arr.length} stations for ${sourceName}`);
  return arr;
};

export async function handler(event, context) {
  const integrated = {};

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

    // delay a bit between requests to reduce strain / avoid throttling
    await delay(1000);
  }

  return {
    statusCode: 200,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*"
    },
    body: JSON.stringify(integrated)
  };
}