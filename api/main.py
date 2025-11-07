from http.server import BaseHTTPRequestHandler
from io import StringIO
from datetime import datetime
import pandas as pd
import requests
import os
import time
import json
import re

# ---------------------------------------------------------------------
# 1. Definición de Fuentes y mapeos
# ---------------------------------------------------------------------

DATA_SOURCES = {
    # INUMET observaciones horarias/diarias
    "INUMET_temperatura": "https://catalogodatos.gub.uy/dataset/accd0e24-76be-4101-904b-81bb7d41ee88/resource/f800fc53-556b-4d1c-8bd6-28b41f9cf146/download/inumet_temperatura_del_aire.csv",
    "INUMET_humedad": "https://catalogodatos.gub.uy/dataset/5f4f50ac-2d11-4863-8ef2-b500d5f3aa90/resource/97ee0df8-3407-433f-b9f7-6e5a2d95ad25/download/inumet_humedad_relativa.csv",
    "INUMET_precipitaciones": "https://catalogodatos.gub.uy/dataset/fd896b11-4c04-4807-bae4-5373d65beea2/resource/ca987721-6052-4bb8-8596-2a5ad9630639/download/inumet_precipitacion_acumulada_horaria.csv",
    # INUMET estaciones meteorológicas (scraping)
    "INUMET_estaciones": "https://www.inumet.gub.uy/tiempo/estaciones-meteorologicas-automaticas",

    # MEF precios (placeholder, ajustar si cambia)
    "MEF_precios": "https://docs.google.com/spreadsheets/d/1AzJs_mNWoFXHN81HO0iT2u-WoZmoMz1K/export?format=csv",

    # UAM producción (usa mismo sheet en este ejemplo; ajustar a URL real)
    "UAM_produccion": "https://docs.google.com/spreadsheets/d/1AzJs_mNWoFXHN81HO0iT2u-WoZmoMz1K/export?format=csv",
}

# Permitir override por env si lo necesitás
OVERRIDE_JSON = os.getenv("DATA_SOURCES_JSON")
if OVERRIDE_JSON:
    DATA_SOURCES.update(json.loads(OVERRIDE_JSON))

TAXONOMY_MAP = {
    'Manzana Roja': 'Manzana',
    'Red Delicious': 'Manzana',
    'Manzana Red Deliciosa': 'Manzana',
    'Granny Smith': 'Manzana',
    'Manzana Granny Smith': 'Manzana',
    'Fuji': 'Manzana',
    'Manzana Fuji': 'Manzana',
    'Otras rojas': 'Manzana',
    'Otras verdes': 'Manzana'
}

DEPARTAMENTOS_URUGUAY = [
    'Artigas', 'Canelones', 'Cerro Largo', 'Colonia', 'Durazno',
    'Flores', 'Florida', 'Lavalleja', 'Maldonado', 'Montevideo',
    'Paysandú', 'Río Negro', 'Rivera', 'Rocha', 'Salto',
    'San José', 'Soriano', 'Tacuarembó', 'Treinta y Tres'
]

MESES_MAP_ABR = {
    'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may': '05', 'jun': '06',
    'jul': '07', 'ago': '08', 'sept': '09', 'oct': '10', 'nov': '11', 'dic': '12'
}

# ---------------------------------------------------------------------
# 2. Wrappers
# ---------------------------------------------------------------------

def wrapper_fuentes(source_name: str, url: str) -> pd.DataFrame:
    """
    Wrapper general:
    - descarga CSV vía HTTP,
    - detecta separador (; o ,),
    - normaliza nombres de columnas,
    - agrega origen_fuente.
    """
    print(f"[wrapper_fuentes] Cargando {source_name}")
    df = pd.DataFrame()

    try:
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()
        content = StringIO(resp.text)

        try:
            df = pd.read_csv(content, sep=';', on_bad_lines='skip', encoding='utf-8')
            # Si vino una sola columna gigante, reintentar con ','
            if df.shape[1] <= 1:
                content.seek(0)
                df = pd.read_csv(content, sep=',', on_bad_lines='skip', encoding='utf-8')
        except Exception:
            content.seek(0)
            df = pd.read_csv(content, sep=',', on_bad_lines='skip', encoding='utf-8')

        # Eliminar filas completamente vacías
        df = df.dropna(how='all')

        # Normalizar nombres
        df.columns = (
            df.columns
            .str.lower()
            .str.replace(' ', '_')
            .str.replace('[^a-zA-Z0-9_]', '', regex=True)
            .str.strip()
        )

        df['origen_fuente'] = source_name
        print(f"[wrapper_fuentes] OK {source_name}: {len(df)} filas")
        return df

    except requests.exceptions.Timeout:
        print(f"[wrapper_fuentes] Timeout {source_name}")
    except requests.exceptions.HTTPError as e:
        print(f"[wrapper_fuentes] HTTPError {source_name}: {e}")
    except Exception as e:
        print(f"[wrapper_fuentes] Error general {source_name}: {e}")

    return pd.DataFrame()


def wrapper_web_scraping_estaciones(source_name: str, url: str) -> pd.DataFrame:
    """
    Scraping estaciones INUMET:
    - extrae JSON embebido en 'var estaciones = ...;'
    - filtra a 19 departamentos
    - devuelve estacion_id + departamento
    """
    print(f"[wrapper_web_scraping_estaciones] Cargando {source_name}")
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        html = resp.text

        m = re.search(r'var estaciones = (.*?);', html, re.DOTALL)
        if not m:
            print("[wrapper_web_scraping_estaciones] No se encontró JSON estaciones")
            return pd.DataFrame()

        estaciones_data = json.loads(m.group(1))
        if 'estaciones' not in estaciones_data:
            print("[wrapper_web_scraping_estaciones] Clave 'estaciones' no encontrada")
            return pd.DataFrame()

        df_st = pd.DataFrame(estaciones_data['estaciones'])
        df_st.columns = (
            df_st.columns
            .str.lower()
            .str.replace(' ', '_')
            .str.replace('[^a-zA-Z0-9_]', '', regex=True)
            .str.strip()
        )

        # Filtrado geográfico
        initial = len(df_st)
        df_st = df_st[df_st['departamento'].isin(DEPARTAMENTOS_URUGUAY)]
        print(f"[wrapper_web_scraping_estaciones] Filtrado {initial - len(df_st)} filas fuera de departamentos objetivo")

        df = pd.DataFrame()
        # 'nombreestacion' viene del JSON real de INUMET
        df['estacion_id'] = df_st['nombreestacion']
        df['departamento'] = df_st['departamento']
        df['origen_fuente'] = source_name

        print(f"[wrapper_web_scraping_estaciones] OK {source_name}: {len(df)} estaciones")
        return df

    except Exception as e:
        print(f"[wrapper_web_scraping_estaciones] Error: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------------------
# 3. Mediador GAV
# ---------------------------------------------------------------------

def mediador() -> pd.DataFrame:
    """
    Orquesta:
    - carga fuentes con wrappers,
    - integra INUMET (clima) horario -> mensual por Departamento,
    - integra producción UAM (Manzana) a nivel mensual,
    - devuelve vista global mensual.
    """
    integrated_data = {}
    print("[mediador] Inicio carga de fuentes")

    # 1) Carga de fuentes
    for source_name, url in DATA_SOURCES.items():
        if "INUMET_estaciones" in source_name:
            df = wrapper_web_scraping_estaciones(source_name, url)
        else:
            df = wrapper_fuentes(source_name, url)

        if not df.empty:
            integrated_data[source_name] = df

        time.sleep(0.2)

    if not integrated_data:
        print("[mediador] Sin datos integrados")
        return pd.DataFrame()

    # 2) INUMET: normalizar y mapear columnas como en el script original

    df_temp = integrated_data.get("INUMET_temperatura", pd.DataFrame()).copy()
    df_hum = integrated_data.get("INUMET_humedad", pd.DataFrame()).copy()
    df_precip = integrated_data.get("INUMET_precipitaciones", pd.DataFrame()).copy()
    df_est = integrated_data.get("INUMET_estaciones", pd.DataFrame()).copy()

    # Renombres clave (solo si existen)
    if not df_temp.empty and "temp_aire" in df_temp.columns:
        df_temp = df_temp.rename(columns={"temp_aire": "temperatura_c"})
    if not df_hum.empty and "hum_relativa" in df_hum.columns:
        df_hum = df_hum.rename(columns={"hum_relativa": "humedad_pje"})
    if not df_precip.empty and "precip_horario" in df_precip.columns:
        df_precip = df_precip.rename(columns={"precip_horario": "precipitacion_mm"})

    # Si ninguna serie climática está disponible, abortar
    if df_temp.empty and df_hum.empty and df_precip.empty:
        print("[mediador] No se pudo cargar ninguna serie climática de INUMET.")
        return pd.DataFrame()

    # Parseo de fechas
    for df_src in (df_temp, df_hum, df_precip):
        if "fecha" in df_src.columns:
            df_src["fecha"] = pd.to_datetime(df_src["fecha"], errors="coerce")

    # Construir base para merges: usamos la primera fuente no vacía
    if not df_temp.empty:
        df_clima = df_temp[["fecha", "estacion_id", "temperatura_c"]].copy()
    elif not df_hum.empty:
        df_clima = df_hum[["fecha", "estacion_id"]].copy()
    else:
        df_clima = df_precip[["fecha", "estacion_id"]].copy()

    # Merge con humedad si existe mapeada
    if not df_hum.empty and "humedad_pje" in df_hum.columns:
        df_clima = df_clima.merge(
            df_hum[["fecha", "estacion_id", "humedad_pje"]],
            on=["fecha", "estacion_id"],
            how="outer"
        )

    # Merge con precipitación si existe mapeada
    if not df_precip.empty and "precipitacion_mm" in df_precip.columns:
        df_clima = df_clima.merge(
            df_precip[["fecha", "estacion_id", "precipitacion_mm"]],
            on=["fecha", "estacion_id"],
            how="outer"
        )

    # Merge con estaciones (departamento); si falla, usamos estacion_id como fallback
    if not df_est.empty and "estacion_id" in df_est.columns:
        df_clima = df_clima.merge(
            df_est[["estacion_id", "departamento"]],
            on="estacion_id",
            how="left"
        )
    else:
        df_clima["departamento"] = pd.NA

    if df_clima["departamento"].notna().sum() == 0:
        print("[mediador] Advertencia: sin match estacion_id-estaciones; usando estacion_id como Departamento.")
        df_clima["departamento"] = df_clima["estacion_id"]

    # Limpiar filas sin fecha o sin departamento
    df_clima = df_clima.dropna(subset=["fecha", "departamento"])
    if df_clima.empty:
        print("[mediador] Clima integrado vacío después de limpieza.")
        return pd.DataFrame()

    # Asegurar columnas numéricas aunque alguna fuente falte
    for col in ["temperatura_c", "humedad_pje", "precipitacion_mm"]:
        if col not in df_clima.columns:
            df_clima[col] = pd.NA

    # Clave temporal mensual
    df_clima["Mes_año"] = df_clima["fecha"].dt.to_period("M")

    # Agregación mensual
    df_clima_mensual = df_clima.groupby(
        ["Mes_año", "departamento"],
        as_index=False
    ).agg(
        precip_total_mm=("precipitacion_mm", "sum"),
        temp_media_c=("temperatura_c", "mean"),
        hum_media_pje=("humedad_pje", "mean"),
    )

    if df_clima_mensual.empty:
        print("[mediador] Agregación mensual de clima vacía.")
        return pd.DataFrame()

    df_clima_mensual[["precip_total_mm", "temp_media_c", "hum_media_pje"]] = (
        df_clima_mensual[["precip_total_mm", "temp_media_c", "hum_media_pje"]].round(1)
    )

    df_clima_mensual = df_clima_mensual.rename(columns={
        "departamento": "Departamento",
        "precip_total_mm": "Precip_Total_mm",
        "temp_media_c": "Temp_Media_C",
        "hum_media_pje": "Hum_Media_Pje",
    })

    # Origen combinado solo con fuentes efectivamente cargadas
    origenes_clima = [
        k for k in ["INUMET_temperatura", "INUMET_humedad", "INUMET_precipitaciones"]
        if k in integrated_data and not integrated_data[k].empty
    ]
    df_clima_mensual["origen_fuente"] = ", ".join(origenes_clima) or "INUMET"

    # 3) UAM: producción mensual (idéntico a tu versión actual)

    df_produccion_mensual = pd.DataFrame()
    if "UAM_produccion" in integrated_data:
        df_produccion = integrated_data["UAM_produccion"].copy()

        if "especie" in df_produccion.columns:
            df_filtrado = df_produccion[df_produccion["especie"] == "Manzana"]
        else:
            df_filtrado = df_produccion

        columnas_a_eliminar = ["grupo", "variedad", "especie", "unidad", "origen_fuente"]
        columnas_meses = [c for c in df_filtrado.columns if c not in columnas_a_eliminar]

        df_produccion_bruto = df_filtrado[columnas_meses].copy()
        df_produccion_bruto["dummy"] = 1

        df_produccion_final = df_produccion_bruto.melt(
            id_vars=["dummy"],
            var_name="Mes_Bruto",
            value_name="Produccion_kg"
        ).drop(columns=["dummy"])

        df_produccion_final["Produccion_kg"] = (
            df_produccion_final["Produccion_kg"]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace("-", "0", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df_produccion_final["Produccion_kg"] = pd.to_numeric(
            df_produccion_final["Produccion_kg"], errors="coerce"
        )
        df_produccion_final = df_produccion_final.dropna(subset=["Produccion_kg"])

        df_produccion_final["Mes_Num"] = (
            df_produccion_final["Mes_Bruto"].str[:3].str.lower().map(MESES_MAP_ABR)
        )
        df_produccion_final["Año_Num"] = df_produccion_final["Mes_Bruto"].str[3:].apply(
            lambda x: f"20{x}" if isinstance(x, str) and x.isdigit() else None
        )
        df_produccion_final = df_produccion_final.dropna(subset=["Mes_Num", "Año_Num"])

        df_produccion_final["Fecha_Str"] = (
            df_produccion_final["Mes_Num"] + "-" + df_produccion_final["Año_Num"]
        )
        df_produccion_final["Mes_año"] = pd.to_datetime(
            df_produccion_final["Fecha_Str"],
            format="%m-%Y",
            errors="coerce"
        ).dt.to_period("M")

        df_produccion_final = df_produccion_final.dropna(subset=["Mes_año"])

        df_produccion_mensual = df_produccion_final.groupby(
            ["Mes_año"], as_index=False
        ).agg(
            Produccion_kg=("Produccion_kg", "sum")
        )

    # 4) Fusión Global

    df_global = df_clima_mensual.copy()

    if not df_produccion_mensual.empty:
        df_global = df_global.merge(
            df_produccion_mensual,
            on="Mes_año",
            how="left"
        )
    else:
        df_global["Produccion_kg"] = pd.NA

    esquema_cols = [
        "Mes_año", "Departamento",
        "Precip_Total_mm", "Temp_Media_C", "Hum_Media_Pje",
        "Produccion_kg"
    ]
    df_global = df_global[esquema_cols]

    df_global["Mes_año"] = df_global["Mes_año"].astype(str)

    print(f"[mediador] Filas en vista_global_final: {len(df_global)}")
    return df_global

# ---------------------------------------------------------------------
# 4. Vercel Python Function handler
# ---------------------------------------------------------------------

class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        try:
            df = mediador()
            if df.empty:
                body = json.dumps({"data": [], "warning": "No se pudo construir la vista global"}, ensure_ascii=False)
                self.send_response(200)
            else:
                records = df.to_dict(orient="records")

                # normalizar NaN → None, Period/Timestamp → str, etc.
                cleaned = []
                for row in records:
                    clean_row = {}
                    for k, v in row.items():
                        if isinstance(v, float) and (pd.isna(v) or v != v):
                            clean_row[k] = None
                        elif isinstance(v, (pd.Timestamp, pd.Period)):
                            clean_row[k] = str(v)
                        else:
                            clean_row[k] = v
                    cleaned.append(clean_row)
                    
                body = json.dumps({"data": records}, ensure_ascii=False)

                self.send_response(200)

            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))

        except Exception as e:
            err = {"error": str(e)}
            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(err).encode("utf-8"))
