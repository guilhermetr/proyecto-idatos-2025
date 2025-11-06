import pandas as pd
import requests
import os
from io import StringIO
import time
import json
import re

# ---------------------------------------------------------------------
# 1. Definición de Fuentes (URLs reales y verificables)
# ---------------------------------------------------------------------

DATA_SOURCES = {
    # Observaciones Horarias/Diarias de INUMET
    "Temperatura_Observaciones": "https://catalogodatos.gub.uy/dataset/accd0e24-76be-4101-904b-81bb7d41ee88/resource/f800fc53-556b-4d1c-8bd6-28b41f9cf146/download/inumet_temperatura_del_aire.csv",
    "Humedad_Observaciones": "https://catalogodatos.gub.uy/dataset/5f4f50ac-2d11-4863-8ef2-b500d5f3aa90/resource/97ee0df8-3407-433f-b9f7-6e5a2d95ad25/download/inumet_humedad_relativa.csv",
    "Precipitacion_Acumulada": "https://catalogodatos.gub.uy/dataset/fd896b11-4c04-4807-bae4-5373d65beea2/resource/ca987721-6052-4bb8-8596-2a5ad9630639/download/inumet_precipitacion_acumulada_horaria.csv",

    # Google Sheets → debe usar export=csv
    "Produccion_Manzanas": "https://docs.google.com/spreadsheets/d/1AzJs_mNWoFXHN81HO0iT2u-WoZmoMz1K/export?format=csv",

    # Fuente geográfica (Scraping)
    "Ubicacion_Estaciones_Scraping": "https://www.inumet.gub.uy/tiempo/estaciones-meteorologicas-automaticas",
}

# ---------------------------------------------------------------------
# 2. Capa de Acceso (Wrappers)
# ---------------------------------------------------------------------

def load_and_normalize_source(source_name, url):
    """
    Wrapper genérico:
    - Descarga CSV desde URL o ruta local.
    - Detecta HTML no esperado.
    - Ajusta separador dinámico.
    - Normaliza columnas.
    """
    print(f"\n--- Cargando Fuente: {source_name} ---")
    print(f"URL: {url}")
    df = pd.DataFrame()

    try:
        if os.path.exists(url):
            print("  Detectado archivo local.")
            df = pd.read_csv(url, sep=',', on_bad_lines='skip', encoding='utf-8')
        else:
            response = requests.get(url, timeout=60)
            print(f" Status: {response.status_code}, Content-Type: {response.headers.get('Content-Type', 'N/A')}")
            response.raise_for_status()

            # Comprobación de HTML
            if "<html" in response.text.lower():
                print(" ERROR: El servidor devolvió HTML, no un CSV válido.")
                print("   Posiblemente el enlace no es directo a un archivo de datos.")
                print("   Primeros 200 caracteres:")
                print(response.text[:200])
                return pd.DataFrame()

            # Intentar leer con ';' y luego con ','
            content = StringIO(response.text)
            try:
                df = pd.read_csv(content, sep=';', on_bad_lines='skip', encoding='utf-8')
                if df.shape[1] == 1:
                    raise ValueError("Solo una columna detectada → reintentando con ','")
            except Exception:
                content.seek(0)
                df = pd.read_csv(content, sep=',', on_bad_lines='skip', encoding='utf-8')

        # Normalización
        df.columns = (
            df.columns.str.lower()
            .str.replace(' ', '_')
            .str.replace('[^a-zA-Z0-9_]', '', regex=True)
            .str.strip()
        )

        df['origen_fuente'] = source_name
        print(f" Éxito: Datos cargados. Filas: {len(df)}, Columnas: {len(df.columns)}")
        return df

    except requests.exceptions.HTTPError as e:
        print(f" ERROR HTTP al cargar {source_name}: {e}")
    except requests.exceptions.Timeout:
        print(f" ERROR DE TIEMPO DE ESPERA al cargar {source_name}")
    except Exception as e:
        print(f"ERROR GENERAL al cargar {source_name}: {e}")

    return pd.DataFrame()

# ---------------------------------------------------------------------
# 3. Scraping de estaciones INUMET
# ---------------------------------------------------------------------

def scrape_station_locations(source_name, url):
    """
    Extrae la lista de estaciones meteorológicas desde la web de INUMET.
    """
    print(f"\n--- Extrayendo por Scraping: {source_name} ---")
    print(f"URL: {url}")
    try:
        response = requests.get(url, timeout=60)
        print(f"Status: {response.status_code}")
        response.raise_for_status()
        html_content = response.text

        json_match = re.search(r'var estaciones\s*=\s*(.*?);', html_content, re.DOTALL)
        if not json_match:
            print("No se encontró 'var estaciones =' en el HTML. La estructura pudo cambiar.")
            print("   Primeros 300 caracteres del HTML:")
            print(html_content[:300])
            return pd.DataFrame()

        json_data_str = json_match.group(1).strip()
        estaciones_data = json.loads(json_data_str)

        if 'estaciones' not in estaciones_data:
            print("Clave 'estaciones' no encontrada en el JSON extraído.")
            return pd.DataFrame()

        df_stations = pd.DataFrame(estaciones_data['estaciones'])
        df_stations.columns = (
            df_stations.columns.str.lower()
            .str.replace(' ', '_')
            .str.replace('[^a-zA-Z0-9_]', '', regex=True)
            .str.strip()
        )

        id_col = next((c for c in df_stations.columns if 'estacion' in c or 'nombre' in c or 'displayname' in c), None)
        lat_col = next((c for c in df_stations.columns if 'lat' in c), None)
        lon_col = next((c for c in df_stations.columns if 'lon' in c), None)

        if id_col:
            df_stations.rename(columns={id_col: 'estacion_id'}, inplace=True)
            df_stations['estacion_id'] = df_stations['estacion_id'].astype(str).str.strip().str.lower()
            if lat_col and lon_col:
                df_stations.rename(columns={lat_col: 'latitud', lon_col: 'longitud'}, inplace=True)
                for col in ['latitud', 'longitud']:
                    df_stations[col] = pd.to_numeric(df_stations[col], errors='coerce')

            df_stations['origen_fuente'] = source_name
            print(f"{len(df_stations)} estaciones extraídas correctamente.")
            return df_stations
        else:
            print("No se encontró columna identificadora de estación.")
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"ERROR DE CONEXIÓN: {e}")
    except json.JSONDecodeError as e:
        print(f"ERROR JSON: {e}")
    except Exception as e:
        print(f"ERROR GENERAL DE SCRAPING: {e}")

    return pd.DataFrame()

# ---------------------------------------------------------------------
# 4. Mediador / Integración
# ---------------------------------------------------------------------

def run_integration_wrappers():
    integrated_data = {}

    print("\n===============================================================")
    print("INICIO DE LA INTEGRACIÓN DE FUENTES")
    print("===============================================================")

    for source_name, url in DATA_SOURCES.items():
        if "Scraping" in source_name:
            df_source = scrape_station_locations(source_name, url)
        else:
            df_source = load_and_normalize_source(source_name, url)

        if not df_source.empty:
            integrated_data[source_name] = df_source
        else:
            print(f"Fuente vacía o con error: {source_name}")

        time.sleep(1)

    print("\n===============================================================")
    print("FASE DE CARGA COMPLETADA.")
    print(f"Fuentes con datos cargadas: {list(integrated_data.keys())}")
    print("===============================================================")

    for name, df in integrated_data.items():
        print(f"\n[{name}] Primeras filas:")
        print(df.head(3))
        print("---------------------------------------------------------------")

    return integrated_data


if __name__ == "__main__":
    run_integration_wrappers()
