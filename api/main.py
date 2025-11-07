from http.server import BaseHTTPRequestHandler
from io import StringIO
from datetime import datetime
import pandas as pd
import requests
import os
import time
import json
import re
import math
import numpy as np
import traceback

# -----------------------------------------------------------------------------
# 1. Definición de Fuentes y mapeos
# -----------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
MEF_PRECIOS_LOCAL = os.path.join(PROJECT_ROOT, "filtered_precios.csv")

DATA_SOURCES = {
    # INUMET observaciones horarias/diarias
    "INUMET_temperatura": "https://catalogodatos.gub.uy/dataset/accd0e24-76be-4101-904b-81bb7d41ee88/resource/f800fc53-556b-4d1c-8bd6-28b41f9cf146/download/inumet_temperatura_del_aire.csv",
    "INUMET_humedad": "https://catalogodatos.gub.uy/dataset/5f4f50ac-2d11-4863-8ef2-b500d5f3aa90/resource/97ee0df8-3407-433f-b9f7-6e5a2d95ad25/download/inumet_humedad_relativa.csv",
    "INUMET_precipitaciones": "https://catalogodatos.gub.uy/dataset/fd896b11-4c04-4807-bae4-5373d65beea2/resource/ca987721-6052-4bb8-8596-2a5ad9630639/download/inumet_precipitacion_acumulada_horaria.csv",
    # INUMET estaciones meteorológicas (scraping)
    "INUMET_estaciones": "https://www.inumet.gub.uy/tiempo/estaciones-meteorologicas-automaticas",

    # MEF precios consumo + establecimientos
    "MEF_precios": MEF_PRECIOS_LOCAL,
    "MEF_establecimientos": "https://catalogodatos.gub.uy/dataset/0c9edcfa-e10e-4068-b967-f1730107bddb/resource/7a007bdf-4c75-44a9-8a8f-f8f75e65648e/download/establecimiento.csv",

    # UAM producción (Google Sheets -> CSV)
    "UAM_produccion": "https://docs.google.com/spreadsheets/d/1AzJs_mNWoFXHN81HO0iT2u-WoZmoMz1K/export?format=csv",
}

# Permitir override por variable de entorno si se quiere apuntar a otras URLs/paths
OVERRIDE_JSON = os.getenv("DATA_SOURCES_JSON")
if OVERRIDE_JSON:
    try:
        DATA_SOURCES.update(json.loads(OVERRIDE_JSON))
    except Exception:
        print("[WARN] DATA_SOURCES_JSON inválido, ignorando override")

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

# -----------------------------------------------------------------------------
# 2. Wrappers de acceso
# -----------------------------------------------------------------------------

def wrapper_fuentes(source_name: str, url: str) -> pd.DataFrame:
    """
    Wrapper general:
    - Soporta archivo local o URL remota.
    - Detecta separador (; o ,).
    - Normaliza nombres de columnas.
    - Agrega columna origen_fuente.
    """
    print(f"[wrapper_fuentes] Cargando {source_name}")
    df = pd.DataFrame()

    try:
        if os.path.exists(url):
            # Archivo local dentro del repo (ej: filtered_precios.csv)
            try:
                df = pd.read_csv(url, sep=";", on_bad_lines="skip", encoding="utf-8")
                if df.shape[1] <= 1:
                    df = pd.read_csv(url, sep=",", on_bad_lines="skip", encoding="utf-8")
            except Exception:
                df = pd.read_csv(url, sep=",", on_bad_lines="skip", encoding="utf-8")
        else:
            # URL remota
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            content = StringIO(resp.text)
            try:
                df = pd.read_csv(content, sep=";", on_bad_lines="skip", encoding="utf-8")
                if df.shape[1] <= 1:
                    content.seek(0)
                    df = pd.read_csv(content, sep=",", on_bad_lines="skip", encoding="utf-8")
            except Exception:
                content.seek(0)
                df = pd.read_csv(content, sep=",", on_bad_lines="skip", encoding="utf-8")

        df = df.dropna(how="all")

        df.columns = (
            df.columns
            .str.lower()
            .str.replace(' ', '_')
            .str.replace('[^a-zA-Z0-9_]', '', regex=True)
            .str.strip()
        )

        df["origen_fuente"] = source_name
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
        if "estaciones" not in estaciones_data:
            print("[wrapper_web_scraping_estaciones] Clave 'estaciones' no encontrada")
            return pd.DataFrame()

        df_st = pd.DataFrame(estaciones_data["estaciones"])
        df_st.columns = (
            df_st.columns
            .str.lower()
            .str.replace(' ', '_')
            .str.replace('[^a-zA-Z0-9_]', '', regex=True)
            .str.strip()
        )

        before = len(df_st)
        df_st = df_st[df_st["departamento"].isin(DEPARTAMENTOS_URUGUAY)]
        print(f"[wrapper_web_scraping_estaciones] Filtrado {before - len(df_st)} filas fuera de Uruguay")

        df = pd.DataFrame()
        df["estacion_id"] = df_st["nombreestacion"]
        df["departamento"] = df_st["departamento"]
        df["origen_fuente"] = source_name

        print(f"[wrapper_web_scraping_estaciones] OK {source_name}: {len(df)} estaciones")
        return df

    except Exception as e:
        print(f"[wrapper_web_scraping_estaciones] Error: {e}")
        return pd.DataFrame()

# -----------------------------------------------------------------------------
# 3. Mediador GAV
# -----------------------------------------------------------------------------

def construir_clima_mensual(integrated_data: dict) -> pd.DataFrame:
    df_temp = integrated_data.get("INUMET_temperatura", pd.DataFrame()).copy()
    df_hum = integrated_data.get("INUMET_humedad", pd.DataFrame()).copy()
    df_precip = integrated_data.get("INUMET_precipitaciones", pd.DataFrame()).copy()
    df_est = integrated_data.get("INUMET_estaciones", pd.DataFrame()).copy()

    # Renombres suaves
    if not df_temp.empty and "temp_aire" in df_temp.columns:
        df_temp = df_temp.rename(columns={"temp_aire": "temperatura_c"})
    if not df_hum.empty and "hum_relativa" in df_hum.columns:
        df_hum = df_hum.rename(columns={"hum_relativa": "humedad_pje"})
    if not df_precip.empty and "precip_horario" in df_precip.columns:
        df_precip = df_precip.rename(columns={"precip_horario": "precipitacion_mm"})

    if df_temp.empty and df_hum.empty and df_precip.empty:
        print("[mediador] No se pudo cargar ninguna serie climática de INUMET.")
        return pd.DataFrame()

    # Parseo fechas
    for df_src in (df_temp, df_hum, df_precip):
        if not df_src.empty and "fecha" in df_src.columns:
            df_src["fecha"] = pd.to_datetime(df_src["fecha"], errors="coerce")

    # Base clima
    if not df_temp.empty:
        df_clima = df_temp[["fecha", "estacion_id", "temperatura_c"]].copy()
    elif not df_hum.empty:
        df_clima = df_hum[["fecha", "estacion_id"]].copy()
    else:
        df_clima = df_precip[["fecha", "estacion_id"]].copy()

    # Merge humedad
    if not df_hum.empty and "humedad_pje" in df_hum.columns:
        df_clima = df_clima.merge(
            df_hum[["fecha", "estacion_id", "humedad_pje"]],
            on=["fecha", "estacion_id"],
            how="outer"
        )

    # Merge precip
    if not df_precip.empty and "precipitacion_mm" in df_precip.columns:
        df_clima = df_clima.merge(
            df_precip[["fecha", "estacion_id", "precipitacion_mm"]],
            on=["fecha", "estacion_id"],
            how="outer"
        )

    # Merge estaciones
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

    df_clima = df_clima.dropna(subset=["fecha", "departamento"])
    if df_clima.empty:
        print("[mediador] Clima integrado vacío después de limpieza.")
        return pd.DataFrame()

    # Asegurar cols numéricas
    for col in ["temperatura_c", "humedad_pje", "precipitacion_mm"]:
        if col not in df_clima.columns:
            df_clima[col] = pd.NA

    # Mes_año mensual
    df_clima["Mes_año"] = df_clima["fecha"].dt.to_period("M")

    # Agregación mensual por Departamento
    df_clima_mensual = df_clima.groupby(
        ["Mes_año", "departamento"],
        as_index=False
    ).agg(
        Precip_Total_mm=("precipitacion_mm", "sum"),
        Temp_Media_C=("temperatura_c", "mean"),
        Hum_Media_Pje=("humedad_pje", "mean"),
    )

    if df_clima_mensual.empty:
        print("[mediador] Agregación mensual de clima vacía.")
        return pd.DataFrame()

    df_clima_mensual[["Precip_Total_mm", "Temp_Media_C", "Hum_Media_Pje"]] = (
        df_clima_mensual[["Precip_Total_mm", "Temp_Media_C", "Hum_Media_Pje"]].round(1)
    )

    df_clima_mensual = df_clima_mensual.rename(columns={"departamento": "Departamento"})

    # Origen combinado
    origenes_clima = [
        k for k in ["INUMET_temperatura", "INUMET_humedad", "INUMET_precipitaciones"]
        if k in integrated_data and not integrated_data[k].empty
    ]
    df_clima_mensual["origen_fuente"] = ", ".join(origenes_clima) or "INUMET"

    return df_clima_mensual


def construir_produccion_mensual(integrated_data: dict) -> pd.DataFrame:
    df_prod = integrated_data.get("UAM_produccion", pd.DataFrame()).copy()
    if df_prod.empty:
        return pd.DataFrame()

    if "especie" in df_prod.columns:
        df_prod = df_prod[df_prod["especie"] == "Manzana"]

    columnas_a_eliminar = ["grupo", "variedad", "especie", "unidad", "origen_fuente"]
    df_bruto = df_prod.drop(columns=columnas_a_eliminar, errors="ignore")

    df_long = df_bruto.melt(
        var_name="Mes_Bruto",
        value_name="Produccion_kg"
    )

    df_long["Produccion_kg"] = (
        df_long["Produccion_kg"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace("-", "0", regex=False)
        .str.replace(",", ".", regex=False)
    )

    df_long["Produccion_kg"] = pd.to_numeric(df_long["Produccion_kg"], errors="coerce")
    df_long = df_long.dropna(subset=["Produccion_kg"])

    df_long["Mes_Num"] = df_long["Mes_Bruto"].str[:3].str.lower().map(MESES_MAP_ABR)
    df_long["Año_Num"] = df_long["Mes_Bruto"].str[3:].apply(
        lambda x: f"20{x}" if isinstance(x, str) and x.isdigit() else None
    )
    df_long = df_long.dropna(subset=["Mes_Num", "Año_Num"])

    df_long["Fecha_Str"] = df_long["Mes_Num"] + "-" + df_long["Año_Num"]
    df_long["Mes_año"] = pd.to_datetime(
        df_long["Fecha_Str"], format="%m-%Y", errors="coerce"
    ).dt.to_period("M")
    df_long = df_long.dropna(subset=["Mes_año"])

    df_prod_m = df_long.groupby("Mes_año", as_index=False).agg(
        Produccion_kg=("Produccion_kg", "sum")
    )

    return df_prod_m


def construir_precios_mensual(integrated_data: dict) -> pd.DataFrame:
    df_precios = integrated_data.get("MEF_precios", pd.DataFrame()).copy()
    df_est = integrated_data.get("MEF_establecimientos", pd.DataFrame()).copy()

    if df_precios.empty or df_est.empty:
        print("[mediador] Precios: faltan MEF_precios o MEF_establecimientos")
        return pd.DataFrame()

    # Merge precios + establecimientos
    merged = pd.merge(
        df_precios,
        df_est,
        left_on="establecimiento",
        right_on="idestablecimientos",
        how="inner"
    )

    if "fecha" not in merged.columns or "precio" not in merged.columns:
        print("[mediador] Precios: columnas 'fecha' o 'precio' no encontradas tras merge")
        return pd.DataFrame()

    merged["fecha"] = pd.to_datetime(merged["fecha"], errors="coerce", format="mixed")
    merged = merged.dropna(subset=["fecha"])
    merged["Mes_año"] = merged["fecha"].dt.to_period("M")

    # group by depto + Mes_año
    if "iddepto" not in merged.columns and "depto" not in merged.columns:
        print("[mediador] Precios: columnas depto no encontradas")
        return pd.DataFrame()

    grouped = (
        merged.groupby(["iddepto", "depto", "Mes_año"], as_index=False)
        .agg({"precio": "mean"})
    )

    grouped["Precio_kg"] = grouped["precio"].round(2)
    grouped["Departamento"] = grouped["depto"]

    df_precios_m = grouped[["Mes_año", "Departamento", "Precio_kg"]]
    print(f"[mediador] Precios MEF_mensual: {len(df_precios_m)} filas")
    return df_precios_m


def mediador() -> pd.DataFrame:
    """
    Orquesta:
    - Carga fuentes con wrappers.
    - Integra clima (INUMET) horario -> mensual por Departamento.
    - Integra producción UAM (Manzana) mensual (sin departamento).
    - Integra precios MEF mensuales por Departamento.
    - Devuelve vista global mensual.
    """
    integrated_data = {}
    print("[mediador] Inicio carga de fuentes")

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

    # Construir subcomponentes
    df_clima_m = construir_clima_mensual(integrated_data)
    df_prod_m = construir_produccion_mensual(integrated_data)
    df_prec_m = construir_precios_mensual(integrated_data)

    if df_clima_m.empty and df_prod_m.empty and df_prec_m.empty:
        print("[mediador] Todas las vistas parciales están vacías.")
        return pd.DataFrame()

    # Fusión global
    if not df_clima_m.empty:
        df_global = df_clima_m.copy()
    elif not df_prec_m.empty:
        df_global = df_prec_m.copy()
    else:
        df_global = df_prod_m.copy()

    # Merge producción (solo por Mes_año, se replica por depto)
    if not df_prod_m.empty:
        df_global = df_global.merge(
            df_prod_m,
            on="Mes_año",
            how="left"
        )

    # Merge precios
    if not df_prec_m.empty:
        join_keys = ["Mes_año", "Departamento"] if "Departamento" in df_global.columns else ["Mes_año"]
        df_global = df_global.merge(
            df_prec_m,
            on=join_keys,
            how="left"
        )

    # Asegurar columnas del esquema
    for col in ["Precip_Total_mm", "Temp_Media_C", "Hum_Media_Pje",
                "Produccion_kg", "Precio_kg", "Departamento"]:
        if col not in df_global.columns:
            df_global[col] = pd.NA

    # Limpiar filas sin Mes_año o sin Departamento
    if "Departamento" in df_global.columns:
        df_global = df_global.dropna(subset=["Mes_año", "Departamento"])
    else:
        df_global = df_global.dropna(subset=["Mes_año"])

    if df_global.empty:
        print("[mediador] Vista global vacía después de limpieza.")
        return pd.DataFrame()

    # Convertir Mes_año a string (YYYY-MM) para el frontend
    df_global["Mes_año"] = df_global["Mes_año"].astype(str)

    # Recalcular origen_fuente aproximado por fila
    def origen_row(row):
        fuentes = []
        if not pd.isna(row.get("Precip_Total_mm")) or not pd.isna(row.get("Temp_Media_C")) or not pd.isna(row.get("Hum_Media_Pje")):
            fuentes.append("INUMET")
        if not pd.isna(row.get("Produccion_kg")):
            fuentes.append("UAM")
        if not pd.isna(row.get("Precio_kg")):
            fuentes.append("MEF")
        return ", ".join(fuentes) if fuentes else None

    df_global["origen_fuente"] = df_global.apply(origen_row, axis=1)

    # Orden columnas para el frontend
    cols = [
        "Mes_año", "Departamento",
        "Precip_Total_mm", "Temp_Media_C", "Hum_Media_Pje",
        "Produccion_kg", "Precio_kg",
        "origen_fuente"
    ]
    df_global = df_global[cols]

    df_global = df_global.sort_values(["Mes_año", "Departamento"]).reset_index(drop=True)

    print(f"[mediador] Filas en vista_global_final: {len(df_global)}")
    return df_global

# -----------------------------------------------------------------------------
# 4. Cache simple en memoria (dentro del runtime de la función)
# -----------------------------------------------------------------------------

CACHE = {
    "timestamp": None,
    "data": None,
}

CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "3600"))  # 1h por defecto


def get_cached_vista_global() -> pd.DataFrame:
    now = time.time()
    if CACHE["data"] is not None and CACHE["timestamp"] is not None:
        if now - CACHE["timestamp"] < CACHE_TTL_SECONDS:
            print("[cache] usando resultado en memoria")
            return CACHE["data"]

    print("[cache] recalculando mediador()")
    df = mediador()
    CACHE["data"] = df
    CACHE["timestamp"] = now
    return df

# -----------------------------------------------------------------------------
# 5. Utilidad: limpieza para JSON
# -----------------------------------------------------------------------------

def to_jsonable_records(df: pd.DataFrame):
    records = df.to_dict(orient="records")
    out = []

    for rec in records:
        clean = {}
        for k, v in rec.items():
            # Period ya los convertimos a str antes, pero por las dudas
            if isinstance(v, pd.Period):
                clean[k] = str(v)
            elif isinstance(v, pd.Timestamp):
                clean[k] = v.isoformat()
            elif isinstance(v, (float, np.floating)):
                if math.isnan(v) or math.isinf(v):
                    clean[k] = None
                else:
                    clean[k] = float(v)
            else:
                try:
                    if pd.isna(v):
                        clean[k] = None
                    else:
                        clean[k] = v
                except TypeError:
                    clean[k] = v
        out.append(clean)
    return out

# -----------------------------------------------------------------------------
# 6. Vercel Python Function handler
# -----------------------------------------------------------------------------

class handler(BaseHTTPRequestHandler):
    def _set_cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")

    def do_OPTIONS(self):
        self.send_response(204)
        self._set_cors()
        self.end_headers()

    def do_GET(self):
        try:
            df = get_cached_vista_global()

            if isinstance(df, pd.DataFrame) and not df.empty:
                records = to_jsonable_records(df)
                body = json.dumps({"data": records}, ensure_ascii=False)
                self.send_response(200)
            else:
                body = json.dumps(
                    {
                        "data": [],
                        "warning": "No se pudo construir la vista global"
                    },
                    ensure_ascii=False
                )
                self.send_response(200)

            self.send_header("Content-Type", "application/json; charset=utf-8")
            self._set_cors()
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))

        except Exception as e:
            print("[handler] Error:", e)
            traceback.print_exc()
            err_body = json.dumps({"error": str(e)}, ensure_ascii=False)
            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self._set_cors()
            self.end_headers()
            self.wfile.write(err_body.encode("utf-8"))