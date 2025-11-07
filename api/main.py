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
# 1) Fuentes y mapeos
# ---------------------------------------------------------------------

DATA_SOURCES = {
    "INUMET_temperatura": "https://catalogodatos.gub.uy/dataset/accd0e24-76be-4101-904b-81bb7d41ee88/resource/f800fc53-556b-4d1c-8bd6-28b41f9cf146/download/inumet_temperatura_del_aire.csv",
    "INUMET_humedad": "https://catalogodatos.gub.uy/dataset/5f4f50ac-2d11-4863-8ef2-b500d5f3aa90/resource/97ee0df8-3407-433f-b9f7-6e5a2d95ad25/download/inumet_humedad_relativa.csv",
    "INUMET_precipitaciones": "https://catalogodatos.gub.uy/dataset/fd896b11-4c04-4807-bae4-5373d65beea2/resource/ca987721-6052-4bb8-8596-2a5ad9630639/download/inumet_precipitacion_acumulada_horaria.csv",
    "INUMET_estaciones": "https://www.inumet.gub.uy/tiempo/estaciones-meteorologicas-automaticas",
    "MEF_precios": "https://docs.google.com/spreadsheets/d/1AzJs_mNWoFXHN81HO0iT2u-WoZmoMz1K/export?format=csv",
    "UAM_produccion": "https://docs.google.com/spreadsheets/d/1AzJs_mNWoFXHN81HO0iT2u-WoZmoMz1K/export?format=csv",
}

OVERRIDE_JSON = os.getenv("DATA_SOURCES_JSON")
if OVERRIDE_JSON:
    DATA_SOURCES.update(json.loads(OVERRIDE_JSON))

DEPARTAMENTOS_URUGUAY = [
    'Artigas','Canelones','Cerro Largo','Colonia','Durazno','Flores','Florida','Lavalleja',
    'Maldonado','Montevideo','Paysandú','Río Negro','Rivera','Rocha','Salto','San José',
    'Soriano','Tacuarembó','Treinta y Tres'
]

MESES_MAP_ABR = {
    'ene':'01','feb':'02','mar':'03','abr':'04','may':'05','jun':'06',
    'jul':'07','ago':'08','sept':'09','oct':'10','nov':'11','dic':'12'
}

# ---------------------------------------------------------------------
# 2) Wrappers
# ---------------------------------------------------------------------

def wrapper_fuentes(source_name: str, url: str) -> pd.DataFrame:
    print(f"[wrapper_fuentes] Cargando {source_name}")
    try:
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()
        content = StringIO(resp.text)

        try:
            df = pd.read_csv(content, sep=';', on_bad_lines='skip', encoding='utf-8')
            if df.shape[1] <= 1:
                content.seek(0)
                df = pd.read_csv(content, sep=',', on_bad_lines='skip', encoding='utf-8')
        except Exception:
            content.seek(0)
            df = pd.read_csv(content, sep=',', on_bad_lines='skip', encoding='utf-8')

        df = df.dropna(how='all')
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
        initial = len(df_st)
        df_st = df_st[df_st['departamento'].isin(DEPARTAMENTOS_URUGUAY)]
        print(f"[wrapper_web_scraping_estaciones] Filtrado {initial - len(df_st)} filas fuera de departamentos objetivo")

        df = pd.DataFrame()
        df['estacion_id'] = df_st['nombreestacion']   # del JSON real de INUMET
        df['departamento'] = df_st['departamento']
        df['origen_fuente'] = source_name
        print(f"[wrapper_web_scraping_estaciones] OK {source_name}: {len(df)} estaciones")
        return df
    except Exception as e:
        print(f"[wrapper_web_scraping_estaciones] Error: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------------------
# 3) Mediador GAV
# ---------------------------------------------------------------------

def mediador() -> pd.DataFrame:
    integrated = {}
    print("[mediador] Inicio carga de fuentes")

    for source_name, url in DATA_SOURCES.items():
        if "INUMET_estaciones" in source_name:
            df = wrapper_web_scraping_estaciones(source_name, url)
        else:
            df = wrapper_fuentes(source_name, url)
        if not df.empty:
            integrated[source_name] = df
        time.sleep(0.2)

    if not integrated:
        print("[mediador] Sin datos integrados")
        return pd.DataFrame()

    df_temp = integrated.get("INUMET_temperatura", pd.DataFrame()).copy()
    df_hum  = integrated.get("INUMET_humedad", pd.DataFrame()).copy()
    df_prec = integrated.get("INUMET_precipitaciones", pd.DataFrame()).copy()
    df_est  = integrated.get("INUMET_estaciones", pd.DataFrame()).copy()

    if not df_temp.empty and "temp_aire" in df_temp.columns:
        df_temp = df_temp.rename(columns={"temp_aire": "temperatura_c"})
    if not df_hum.empty and "hum_relativa" in df_hum.columns:
        df_hum = df_hum.rename(columns={"hum_relativa": "humedad_pje"})
    if not df_prec.empty and "precip_horario" in df_prec.columns:
        df_prec = df_prec.rename(columns={"precip_horario": "precipitacion_mm"})

    if df_temp.empty and df_hum.empty and df_prec.empty:
        print("[mediador] No se pudo cargar ninguna serie climática de INUMET.")
        return pd.DataFrame()

    for df_src in (df_temp, df_hum, df_prec):
        if "fecha" in df_src.columns:
            df_src["fecha"] = pd.to_datetime(df_src["fecha"], errors="coerce")

    if not df_temp.empty:
        df_clima = df_temp[["fecha","estacion_id","temperatura_c"]].copy()
    elif not df_hum.empty:
        df_clima = df_hum[["fecha","estacion_id"]].copy()
    else:
        df_clima = df_prec[["fecha","estacion_id"]].copy()

    if not df_hum.empty and "humedad_pje" in df_hum.columns:
        df_clima = df_clima.merge(
            df_hum[["fecha","estacion_id","humedad_pje"]],
            on=["fecha","estacion_id"], how="outer"
        )
    if not df_prec.empty and "precipitacion_mm" in df_prec.columns:
        df_clima = df_clima.merge(
            df_prec[["fecha","estacion_id","precipitacion_mm"]],
            on=["fecha","estacion_id"], how="outer"
        )

    if not df_est.empty and "estacion_id" in df_est.columns:
        df_clima = df_clima.merge(
            df_est[["estacion_id","departamento"]],
            on="estacion_id", how="left"
        )
    else:
        df_clima["departamento"] = pd.NA

    df_clima = df_clima.dropna(subset=["fecha"])
    if df_clima["departamento"].notna().sum() == 0:
        print("[mediador] Advertencia: sin match estacion_id-estaciones; usando estacion_id como Departamento.")
        df_clima["departamento"] = df_clima["estacion_id"]

    df_clima = df_clima.dropna(subset=["departamento"])
    if df_clima.empty:
        print("[mediador] Clima integrado vacío después de limpieza.")
        return pd.DataFrame()

    for col in ["temperatura_c", "humedad_pje", "precipitacion_mm"]:
        if col not in df_clima.columns:
            df_clima[col] = pd.NA

    df_clima["Mes_año"] = df_clima["fecha"].dt.to_period("M")
    df_clima_m = df_clima.groupby(["Mes_año","departamento"], as_index=False).agg(
        precip_total_mm=("precipitacion_mm","sum"),
        temp_media_c=("temperatura_c","mean"),
        hum_media_pje=("humedad_pje","mean"),
    )

    if df_clima_m.empty:
        print("[mediador] Agregación mensual de clima vacía.")
        return pd.DataFrame()

    df_clima_m[["precip_total_mm","temp_media_c","hum_media_pje"]] = \
        df_clima_m[["precip_total_mm","temp_media_c","hum_media_pje"]].round(1)

    df_clima_m = df_clima_m.rename(columns={
        "departamento": "Departamento",
        "precip_total_mm": "Precip_Total_mm",
        "temp_media_c": "Temp_Media_C",
        "hum_media_pje": "Hum_Media_Pje",
    })

    origenes_clima = [
        k for k in ["INUMET_temperatura","INUMET_humedad","INUMET_precipitaciones"]
        if k in integrated and not integrated[k].empty
    ]
    df_clima_m["origen_fuente"] = ", ".join(origenes_clima) or "INUMET"

    # Producción mensual (simple)
    df_prod_m = pd.DataFrame()
    if "UAM_produccion" in integrated:
        df_prod = integrated["UAM_produccion"].copy()
        if "especie" in df_prod.columns:
            df_prod = df_prod[df_prod["especie"] == "Manzana"]

        columnas_a_eliminar = ["grupo","variedad","especie","unidad","origen_fuente"]
        columnas_meses = [c for c in df_prod.columns if c not in columnas_a_eliminar]
        df_bruto = df_prod[columnas_meses].copy()
        df_bruto["dummy"] = 1
        df_long = df_bruto.melt(id_vars=["dummy"], var_name="Mes_Bruto", value_name="Produccion_kg").drop(columns=["dummy"])

        df_long["Produccion_kg"] = (
            df_long["Produccion_kg"].astype(str)
                .str.replace(".","",regex=False)
                .str.replace(" ","",regex=False)
                .str.replace("-","0",regex=False)
                .str.replace(",",".",regex=False)
        )
        df_long["Produccion_kg"] = pd.to_numeric(df_long["Produccion_kg"], errors="coerce")
        df_long = df_long.dropna(subset=["Produccion_kg"])

        df_long["Mes_Num"] = df_long["Mes_Bruto"].str[:3].str.lower().map(MESES_MAP_ABR)
        df_long["Año_Num"] = df_long["Mes_Bruto"].str[3:].apply(
            lambda x: f"20{x}" if isinstance(x,str) and x.isdigit() else None
        )
        df_long = df_long.dropna(subset=["Mes_Num","Año_Num"])
        df_long["Fecha_Str"] = df_long["Mes_Num"] + "-" + df_long["Año_Num"]
        df_long["Mes_año"] = pd.to_datetime(df_long["Fecha_Str"], format="%m-%Y", errors="coerce").dt.to_period("M")
        df_long = df_long.dropna(subset=["Mes_año"])
        df_prod_m = df_long.groupby(["Mes_año"], as_index=False).agg(Produccion_kg=("Produccion_kg","sum"))

    df_global = df_clima_m.copy()
    if not df_prod_m.empty:
        df_global = df_global.merge(df_prod_m, on="Mes_año", how="left")
    else:
        df_global["Produccion_kg"] = pd.NA

    df_global = df_global[["Mes_año","Departamento","Precip_Total_mm","Temp_Media_C","Hum_Media_Pje","Produccion_kg"]]
    df_global["Mes_año"] = df_global["Mes_año"].astype(str)
    print(f"[mediador] Filas en vista_global_final: {len(df_global)}")
    return df_global

# ---------------------------------------------------------------------
# 4) Utilidades de caché y JSON seguro
# ---------------------------------------------------------------------

CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))
_cache_df = None
_cache_ts = 0.0

def get_df_cached() -> pd.DataFrame:
    global _cache_df, _cache_ts
    now = time.time()
    if _cache_df is not None and (now - _cache_ts) < CACHE_TTL:
        print("[cache] usando df cacheado")
        return _cache_df
    print("[cache] recalculando mediador()")
    df = mediador()
    _cache_df = df
    _cache_ts = now
    return df

def df_to_json_rows(df: pd.DataFrame):
    records = df.to_dict(orient="records")
    cleaned = []
    for row in records:
        out = {}
        for k, v in row.items():
            # pd.NA, NaN, NaT, None -> None
            try:
                if pd.isna(v):
                    out[k] = None
                    continue
            except Exception:
                pass
            # Tipos pandas -> str
            if isinstance(v, (pd.Timestamp, pd.Period, pd.Timedelta)):
                out[k] = str(v)
            else:
                out[k] = v
        cleaned.append(out)
    return cleaned

# ---------------------------------------------------------------------
# 5) Vercel Python Function handler
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
            df = get_df_cached()

            if df.empty:
                payload = {"data": [], "warning": "No se pudo construir la vista global"}
            else:
                rows = df_to_json_rows(df)
                payload = {"data": rows}

            body = json.dumps(payload, ensure_ascii=False, allow_nan=False)

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
            self.wfile.write(json.dumps(err, ensure_ascii=False).encode("utf-8"))
