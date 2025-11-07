# /api/main.py

import os
import json
import time
import re
from io import StringIO
from datetime import datetime
from http.server import BaseHTTPRequestHandler

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# 0. Paths (Vercel-friendly)
# ---------------------------------------------------------------------------

# En Vercel, el cwd es el root del proyecto.
# Los CSV están en /api, así que construimos rutas absolutas desde este archivo.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def csv_path(filename: str) -> str:
    return os.path.join(BASE_DIR, filename)

# ---------------------------------------------------------------------------
# 1. Definición de Fuentes y mapeos
# ---------------------------------------------------------------------------

DATA_SOURCES = {
    # INUMET observaciones Horarias/Diarias (archivos locales dentro de /api)
    "INUMET_temperatura": csv_path("inumet_temperatura_del_aire.csv"),
    "INUMET_humedad": csv_path("inumet_humedad_relativa.csv"),
    "INUMET_precipitaciones": csv_path("inumet_precipitacion_acumulada_horaria.csv"),

    # INUMET estaciones meteorológicas (scraping)
    "INUMET_estaciones": "https://www.inumet.gub.uy/tiempo/estaciones-meteorologicas-automaticas",

    # MEF
    "MEF_precios": csv_path("filtered_precios.csv"),
    "MEF_establecimientos": csv_path("establecimiento.csv"),

    # UAM
    "UAM_produccion": csv_path("volumen_de_ingresos_frutas.csv"),
}

# Resolución Semántica: Mapeo de taxonomía (Manzana fina -> Manzana global)
TAXONOMY_MAP = {
    "Manzana Roja": "Manzana",
    "Red Delicious": "Manzana",
    "Manzana Red Deliciosa": "Manzana",
    "Granny Smith": "Manzana",
    "Manzana Granny Smith": "Manzana",
    "Fuji": "Manzana",
    "Manzana Fuji": "Manzana",
    "Otras rojas": "Manzana",
    "Otras verdes": "Manzana",
}

# Departamentos de Uruguay
DEPARTAMENTOS_URUGUAY = [
    "Artigas", "Canelones", "Cerro Largo", "Colonia", "Durazno",
    "Flores", "Florida", "Lavalleja", "Maldonado", "Montevideo",
    "Paysandú", "Río Negro", "Rivera", "Rocha", "Salto",
    "San José", "Soriano", "Tacuarembó", "Treinta y Tres",
]

# Meses abreviados -> número
MESES_MAP_ABR = {
    "ene": "01", "feb": "02", "mar": "03", "abr": "04", "may": "05", "jun": "06",
    "jul": "07", "ago": "08", "sept": "09", "oct": "10", "nov": "11", "dic": "12",
}

# ---------------------------------------------------------------------------
# 2. Capa de Acceso (Wrappers)
# ---------------------------------------------------------------------------

def wrapper_fuentes(source_name, url):
    """
    Wrapper general
    - Soporta múltiples encodings comunes (utf-8, latin1, cp1252, etc.).
    - Prueba automáticamente ';' y ',' como separadores.
    - Normaliza columnas y agrega 'origen_fuente'.
    """
    print(f"--- Cargando Fuente: {source_name} ---")

    # Encodings y separadores a probar (en este orden)
    ENCODINGS = ["utf-8", "utf-8-sig", "latin1", "iso-8859-1", "cp1252"]
    SEPARATORS = [";", ","]

    def try_read_from_local(path):
        last_exc = None
        for enc in ENCODINGS:
            for sep in SEPARATORS:
                try:
                    df = pd.read_csv(
                        path,
                        sep=sep,
                        on_bad_lines="skip",
                        encoding=enc
                    )
                    # Evitar falsos positivos de una sola columna gigante
                    if df.shape[1] > 1:
                        print(f"{source_name}: leído OK local con encoding={enc}, sep='{sep}'")
                        return df
                except Exception as e:
                    last_exc = e
                    continue
        if last_exc:
            raise last_exc
        raise ValueError(f"No se pudo leer {source_name} como CSV local con los encodings probados.")

    def try_read_from_url(the_url):
        resp = requests.get(the_url, timeout=30)
        resp.raise_for_status()
        text = resp.text

        last_exc = None
        for enc in ENCODINGS:
            for sep in SEPARATORS:
                try:
                    content = StringIO(text)
                    df = pd.read_csv(
                        content,
                        sep=sep,
                        on_bad_lines="skip",
                        encoding=enc
                    )
                    if df.shape[1] > 1:
                        print(f"{source_name}: leído OK URL con encoding={enc}, sep='{sep}'")
                        return df
                except Exception as e:
                    last_exc = e
                    continue
        if last_exc:
            raise last_exc
        raise ValueError(f"No se pudo leer {source_name} desde URL con los encodings probados.")

    try:
        # 1) Local si existe el path
        if os.path.exists(url):
            df = try_read_from_local(url)
        else:
            # 2) Si no existe como archivo, interpretamos como URL
            df = try_read_from_url(url)

        # Limpieza básica
        df = df.dropna(how="all")

        df.columns = (
            df.columns.str.lower()
            .str.replace(" ", "_")
            .str.replace("[^a-zA-Z0-9_]", "", regex=True)
            .str.strip()
        )

        df["origen_fuente"] = source_name
        print(f"Éxito: {source_name} Filas: {len(df)} Columnas: {list(df.columns)}")
        return df

    except Exception as e:
        print(f"ERROR GENERAL al cargar {source_name}: {e}")
        return pd.DataFrame()


def wrapper_web_scraping_estaciones(source_name, url):
    """
    Scraping estaciones INUMET -> DataFrame normalizado.
    """
    print(f"--- Extrayendo por Scraping: {source_name} ---")
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        html_content = response.text

        json_match = re.search(r"var estaciones = (.*?);", html_content, re.DOTALL)
        if not json_match:
            print("ERROR: no se encontró 'var estaciones = ...;' en el HTML.")
            return pd.DataFrame()

        json_data_str = json_match.group(1).strip()
        estaciones_data = json.loads(json_data_str)

        if "estaciones" not in estaciones_data:
            print("Error: estructura JSON inesperada para estaciones.")
            return pd.DataFrame()

        df_stations = pd.DataFrame(estaciones_data["estaciones"])
        df_stations.columns = (
            df_stations.columns.str.lower()
            .str.replace(" ", "_")
            .str.replace("[^a-zA-Z0-9_]", "", regex=True)
            .str.strip()
        )

        df_stations = df_stations[df_stations["departamento"].isin(DEPARTAMENTOS_URUGUAY)]

        df = pd.DataFrame()
        df["estacion_id"] = df_stations["nombreestacion"]
        df["departamento"] = df_stations["departamento"]
        df["origen_fuente"] = source_name

        print(f"Éxito: Tabla con {len(df)} estaciones cargada.")
        return df

    except requests.exceptions.RequestException as e:
        print(f"ERROR DE CONEXIÓN al hacer scraping: {e}")
    except json.JSONDecodeError as e:
        print(f"ERROR DE PARSEO JSON: {e}")
    except Exception as e:
        print(f"ERROR GENERAL de scraping: {e}")

    return pd.DataFrame()

# ---------------------------------------------------------------------------
# 3. Mediador (Transformación e Integración GAV)
# ---------------------------------------------------------------------------

def mediador():
    integrated_data = {}

    print("\n---------------- INICIO WRAPPERS ----------------")

    for source_name, url in DATA_SOURCES.items():
        if "INUMET_estaciones" in source_name:
            df_source = wrapper_web_scraping_estaciones(source_name, url)
        else:
            df_source = wrapper_fuentes(source_name, url)

        if not df_source.empty:
            integrated_data[source_name] = df_source

        # Pausa corta defensiva
        time.sleep(0.1)

    print("\n---------------- INICIO INTEGRACIÓN ----------------")

    if not integrated_data:
        print("Advertencia: No se pudo cargar ninguna fuente.")
        return pd.DataFrame()

    # 3.2 Clima (INUMET)
    print("--- Clima (INUMET)")
    df_temp = integrated_data["INUMET_temperatura"].rename(
        columns={"temp_aire": "temperatura_c"}
    )
    df_hum = integrated_data["INUMET_humedad"].rename(
        columns={"hum_relativa": "humedad_pje"}
    )
    df_precip = integrated_data["INUMET_precipitaciones"].rename(
        columns={"precip_horario": "precipitacion_mm"}
    )
    df_estaciones = integrated_data["INUMET_estaciones"]

    df_temp["fecha"] = pd.to_datetime(df_temp["fecha"], errors="coerce")
    df_hum["fecha"] = pd.to_datetime(df_hum["fecha"], errors="coerce")
    df_precip["fecha"] = pd.to_datetime(df_precip["fecha"], errors="coerce")

    origen_temp = df_temp["origen_fuente"].iloc[0]
    origen_hum = df_hum["origen_fuente"].iloc[0]
    origen_precip = df_precip["origen_fuente"].iloc[0]
    origen_fuente_clima_combinado = ", ".join([origen_precip, origen_temp, origen_hum])

    df_clima_horario = df_temp.merge(
        df_hum[["fecha", "estacion_id", "humedad_pje"]],
        on=["fecha", "estacion_id"],
        how="outer",
    )
    df_clima_horario = df_clima_horario.merge(
        df_precip[["fecha", "estacion_id", "precipitacion_mm"]],
        on=["fecha", "estacion_id"],
        how="outer",
    )

    df_clima_horario = df_clima_horario.merge(
        df_estaciones[["estacion_id", "departamento"]],
        on="estacion_id",
        how="inner",
    )

    df_clima_horario["Mes_año"] = df_clima_horario["fecha"].dt.to_period("M")

    df_clima_mensual = (
        df_clima_horario.groupby(["Mes_año", "departamento"])
        .agg(
            precip_total_mm=("precipitacion_mm", "sum"),
            temp_media_c=("temperatura_c", "mean"),
            hum_media_pje=("humedad_pje", "mean"),
        )
        .reset_index()
    )

    df_clima_mensual[
        ["precip_total_mm", "temp_media_c", "hum_media_pje"]
    ] = df_clima_mensual[
        ["precip_total_mm", "temp_media_c", "hum_media_pje"]
    ].round(1)

    df_clima_mensual = df_clima_mensual.rename(
        columns={
            "precip_total_mm": "Precip_Total_mm",
            "temp_media_c": "Temp_Media_C",
            "hum_media_pje": "Hum_Media_Pje",
            "departamento": "Departamento",
        }
    )
    df_clima_mensual["origen_fuente"] = origen_fuente_clima_combinado

    print("Clima listo")

    # 3.3 Producción (UAM)
    print("--- Producción (UAM)")

    df_produccion = integrated_data["UAM_produccion"]
    df_filtrado_produccion = df_produccion[df_produccion["especie"] == "Manzana"]

    COLUMNAS_A_ELIMINAR = ["grupo", "variedad", "especie", "unidad", "origen_fuente"]
    df_produccion_bruto_clean = df_filtrado_produccion.drop(
        columns=COLUMNAS_A_ELIMINAR, errors="ignore"
    )

    df_produccion_final = df_produccion_bruto_clean.melt(
        var_name="Mes_Bruto",
        value_name="Produccion_kg",
    )

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
        df_produccion_final["Mes_Bruto"].str[:3].map(MESES_MAP_ABR)
    )
    df_produccion_final["Año_Num"] = df_produccion_final["Mes_Bruto"].str[3:].apply(
        lambda x: f"20{x}"
    )
    df_produccion_final["Fecha_Str"] = (
        df_produccion_final["Mes_Num"] + "-" + df_produccion_final["Año_Num"]
    )
    df_produccion_final["Mes_año"] = pd.to_datetime(
        df_produccion_final["Fecha_Str"], format="%m-%Y"
    ).dt.to_period("M")

    df_produccion_final = df_produccion_final.drop(
        columns=["Mes_Bruto", "Mes_Num", "Año_Num", "Fecha_Str"]
    )

    df_produccion_mensual = (
        df_produccion_final.groupby(["Mes_año"])
        .agg(Produccion_kg=("Produccion_kg", "sum"))
        .reset_index()
    )

    print("Producción lista")

    # 3.4 Precios (MEF)
    print("--- Precios (MEF)")

    df_precios = integrated_data["MEF_precios"]
    df_establecimientos = integrated_data["MEF_establecimientos"]

    merged = pd.merge(
        df_precios,
        df_establecimientos,
        left_on="establecimiento",
        right_on="idestablecimientos",
        how="inner",
    )

    merged["fecha"] = pd.to_datetime(merged["fecha"], format="mixed")
    merged = merged.dropna(subset=["fecha"])
    merged["Mes_año"] = merged["fecha"].dt.to_period("M")

    grouped = (
        merged.groupby(["iddepto", "depto", "Mes_año"], as_index=False)
        .agg({"precio": "mean"})
    )

    grouped["Precio_kg"] = grouped["precio"].round(2)
    grouped["Departamento"] = grouped["depto"]

    df_precios_mensual = grouped[["Mes_año", "Departamento", "Precio_kg"]]

    print("Precios listos")

    # 3.5 Fusión final
    df_global = df_clima_mensual.merge(
        df_produccion_mensual,
        on="Mes_año",
        how="outer",
    )

    df_global = df_global.merge(
        df_precios_mensual,
        on=["Mes_año", "Departamento"],
        how="outer",
    )

    ESQUEMA_GLOBAL_FINAL = [
        "Mes_año",
        "Departamento",
        "Precip_Total_mm",
        "Temp_Media_C",
        "Hum_Media_Pje",
        "Produccion_kg",
        "Precio_kg",
        # "origen_fuente",
    ]

    vista_global_final = df_global[ESQUEMA_GLOBAL_FINAL]

    print("\n================ VISTA GLOBAL UNIFICADA ==================\n")
    print(f"Filas totales integradas (solo Manzana): {len(vista_global_final)}")

    return vista_global_final

# ---------------------------------------------------------------------------
# 4. Helper JSON
# ---------------------------------------------------------------------------

def dataframe_to_json_records(df: pd.DataFrame):
    if df is None or df.empty:
        return []

    if "Mes_año" in df.columns:
        df = df.copy()
        df["Mes_año"] = df["Mes_año"].astype(str)

    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")

# ---------------------------------------------------------------------------
# 5. HTTP handler para Vercel (/api/main)
# ---------------------------------------------------------------------------

class handler(BaseHTTPRequestHandler):
    """
    Vercel ejecuta esta clase para las requests a /api/main.
    """

    def _set_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _send_json(self, status_code: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self._set_cors_headers()
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self._set_cors_headers()
        self.end_headers()

    def do_GET(self):
        try:
            df_vista_global = mediador()
            data = dataframe_to_json_records(df_vista_global)
            self._send_json(200, {"data": data})
        except Exception as e:
            print(f"ERROR en /api/main: {e}")
            self._send_json(500, {"error": "Error interno en el mediador"})
