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

def mediador() -> pd.DataFrame:
    """
    Función principal del Mediador. Orquesta la carga (Wrappers),
    resuelve las heterogeneidades semánticas y construye el Esquema Global Virtual.
    """
    # 3.1. FASE DE CARGA Y NORMALIZACIÓN (ORQUESTACIÓN DE WRAPPERS)
    # El diccionario 'integrated_data' simula la memoria del Mediador para el procesamiento GAV
    integrated_data = {}

    print("\n----------------------------------------------------------------------------------------------------")
    print("INICIO DE LA FASE DE EXTRACCIÓN/CARGA (WRAPPERS)")
    print("----------------------------------------------------------------------------------------------------")

    for source_name, url in DATA_SOURCES.items():
        # Llamada a la Capa de Acceso (Wrapper)
        if "INUMET_estaciones" in source_name:
            df_source = wrapper_web_scraping_estaciones(source_name, url)
        else:
            df_source = wrapper_fuentes(source_name, url)
        
        if not df_source.empty:
            integrated_data[source_name] = df_source

        # Pausa para evitar bloqueos del servidor
        time.sleep(1)

    print("\n----------------------------------------------------------------------------------------------------")
    print("INICIO DE LA CONSTRUCCIÓN DEL ESQUEMA GLOBAL (RESOLUCIÓN DE HETEROGENEIDADES E INTEGRACIÓN FINAL)")
    print("----------------------------------------------------------------------------------------------------")

    if not integrated_data:
        print("Advertencia: No se pudo cargar ninguna fuente.")
        return integrated_data # Return empty dictionary

    # 3.2. RESOLUCIÓN DE HETEROGENEIDADES CLIMÁTICAS (INUMET)
    
    print("--- Resolviendo heterogeneidades climáticas (INUMET)")
    
    # Unificación de Datos Climáticos Horarios (Por Enriquecimiento de Clave)
    df_temp = integrated_data['INUMET_temperatura'].rename(columns={'temp_aire': 'temperatura_c'})
    df_hum = integrated_data['INUMET_humedad'].rename(columns={'hum_relativa': 'humedad_pje'})
    df_precip = integrated_data['INUMET_precipitaciones'].rename(columns={'precip_horario': 'precipitacion_mm'})
    df_estaciones = integrated_data['INUMET_estaciones']
    
    # Pre-procesamiento de fechas y claves
    # Conversión de Fecha/Hora
    df_temp['fecha'] = pd.to_datetime(df_temp['fecha'], errors='coerce')
    df_hum['fecha'] = pd.to_datetime(df_hum['fecha'], errors='coerce')
    df_precip['fecha'] = pd.to_datetime(df_precip['fecha'], errors='coerce')

    # Fusión inicial de clima (Horario)
    origen_temp = df_temp['origen_fuente'].iloc[0] 
    origen_hum = df_hum['origen_fuente'].iloc[0]  
    origen_precip = df_precip['origen_fuente'].iloc[0]
    lista_origenes_clima = [origen_precip, origen_temp, origen_hum]
    origen_fuente_clima_combinado = ', '.join(lista_origenes_clima)

    df_clima_horario = df_temp.merge(df_hum[['fecha', 'estacion_id', 'humedad_pje']], 
                                     on=['fecha', 'estacion_id'], how='outer')
    df_clima_horario = df_clima_horario.merge(df_precip[['fecha', 'estacion_id', 'precipitacion_mm']],
                                              on=['fecha', 'estacion_id'], how='outer')
    
    # Resolución de Granularidad Espacial (Estación -> Departamento/Zona) 
    # Se añade el departamento a cada registro de clima (Enriquecimiento) y se descartan los registros sin ubicación estacion->departamento
    df_clima_horario = df_clima_horario.merge(df_estaciones[['estacion_id', 'departamento']], 
                                              left_on='estacion_id', right_on='estacion_id', how='inner')

    # Resolución de Granularidad Temporal (Horario -> Mensual) 
    # Se define la clave temporal (Mes_año) para el Esquema Global 
    df_clima_horario['Mes_año'] = df_clima_horario['fecha'].dt.to_period('M')#.dt.to_timestamp()
    
    # Agregación a nivel Mensual (Mes_año, Departamento)
    df_clima_mensual = df_clima_horario.groupby(['Mes_año', 'departamento']).agg(
        # Suma total para Precipitación
        precip_total_mm=('precipitacion_mm', 'sum'), 
        # Promedio para Temperatura y Humedad
        temp_media_c=('temperatura_c', 'mean'), 
        hum_media_pje=('humedad_pje', 'mean')
    ).reset_index()

    # APLICAR REDONDEO: Todas las cifras numéricas a un solo dígito decimal.
    df_clima_mensual[['precip_total_mm', 'temp_media_c', 'hum_media_pje']] = df_clima_mensual[['precip_total_mm', 'temp_media_c', 'hum_media_pje']].round(1)
    
    df_clima_mensual = df_clima_mensual.rename(columns={'precip_total_mm': 'Precip_Total_mm'})
    df_clima_mensual = df_clima_mensual.rename(columns={'temp_media_c': 'Temp_Media_C'})
    df_clima_mensual = df_clima_mensual.rename(columns={'hum_media_pje': 'Hum_Media_Pje'})
    df_clima_mensual = df_clima_mensual.rename(columns={'departamento': 'Departamento'})
    # Agregar data provenance
    df_clima_mensual['origen_fuente'] = origen_fuente_clima_combinado 
    
    print("Éxito")

    # 3.3. RESOLUCIÓN DE HETEROGENEIDADES DE PRODUCCIÓN (UAM)
    
    print("--- Resolviendo heterogeneidades de producción (UAM)")
    df_produccion = integrated_data['UAM_produccion']
    # df_filtrado_produccion ahora solo contiene las filas donde la columna 'especie' tiene exactamente el valor 'Manzana'.
    df_filtrado_produccion = df_produccion[df_produccion['especie'] == 'Manzana']

    # Columnas a eliminar 
    COLUMNAS_A_ELIMINAR = ['grupo', 'variedad', 'especie', 'unidad', 'origen_fuente']

    # Columnas que contienen los valores de los meses
    COLUMNAS_MESES = [col for col in df_filtrado_produccion.columns if col not in COLUMNAS_A_ELIMINAR]

    # Eliminar las columnas que no son necesarias para el Esquema Global
    df_produccion_bruto_clean = df_filtrado_produccion.drop(columns=COLUMNAS_A_ELIMINAR, errors='ignore')

    # Aplicar el Pivote Inverso (melt)
    df_produccion_final = df_produccion_bruto_clean.melt(
        # Nombre de la nueva columna que contendrá los encabezados de los meses (ej. 'ene07')
        var_name='Mes_Bruto',
        # Nombre de la nueva columna que contendrá los valores (producción)
        value_name='Produccion_kg'
    )

    # Limpieza de caracteres no numéricos y conversión
    # Eliminar espacios y puntos que actúan como separadores de miles
    df_produccion_final['Produccion_kg'] = (
        df_produccion_final['Produccion_kg']
        .astype(str) # Asegurar que es string para las operaciones
        .str.replace('.', '', regex=False) # Eliminar puntos (separador de miles)
        .str.replace(' ', '', regex=False) # Eliminar espacios (posibles separadores de miles o basura)
        .str.replace('-', '0', regex=False) # Reemplazar guiones (posibles nulos/faltantes) por cero
        .str.replace(',', '.', regex=False) # Si hay comas, tratarlas como separador decimal (aunque parece no ser el caso)
    )

    # Convertir la columna al tipo numérico float (los valores no válidos se harán NaN)
    df_produccion_final['Produccion_kg'] = pd.to_numeric(
        df_produccion_final['Produccion_kg'], 
        errors='coerce' # Convierte cualquier valor que no sea un número a NaN
    )

    # Eliminar filas donde la producción es NaN después de la conversión
    df_produccion_final = df_produccion_final.dropna(subset=['Produccion_kg'])

    # Transformar la columna 'Mes_Bruto' a formato 'fecha' (yyyy-mm-01)

    # Extracción y Mapeo del Mes (ej. 'ene07' -> '01')
    df_produccion_final['Mes_Num'] = (
        df_produccion_final['Mes_Bruto']
        .str[:3] # Toma los primeros 3 caracteres ('ene', 'feb', etc.)
        .map(MESES_MAP_ABR) # Mapea a '01', '02', etc.
    )

    # Extracción y Conversión del Año (ej. 'ene07' -> '07' -> '2007')
    df_produccion_final['Año_Num'] = (
        df_produccion_final['Mes_Bruto']
        .str[3:] # Toma los últimos 2 caracteres ('07', '25', etc.)
        .apply(lambda x: f"20{x}") # Añade '20' al inicio (ej. '07' -> '2007')
    )

    # Creación de la Cadena de Fecha Estándar (ej. '01-2007')
    df_produccion_final['Fecha_Str'] = df_produccion_final['Mes_Num'] + '-' + df_produccion_final['Año_Num']

    # Conversión Final a mes año
    df_produccion_final['Mes_año'] = pd.to_datetime(
        df_produccion_final['Fecha_Str'], 
        format='%m-%Y' # Indicamos que el formato de entrada es Mes-Año
    ).dt.to_period('M')

    # Limpieza final de las columnas temporales auxiliares
    df_produccion_final = df_produccion_final.drop(columns=['Mes_Bruto', 'Mes_Num', 'Año_Num', 'Fecha_Str'])

    # Agregación a nivel Mensual (Mes_año)
    df_produccion_mensual = df_produccion_final.groupby(['Mes_año']).agg(
        # Suma total de kilos
        Produccion_kg=('Produccion_kg', 'sum')
    ).reset_index()
    
    print("Éxito")
    # 3.4. RESOLUCIÓN DE HETEROGENEIDADES DE PRECIOS (MEF)

    print("--- Resolviendo heterogeneidades de precios (MEF)")
    df_precios = integrated_data['MEF_precios']
    df_establecimientos = integrated_data['MEF_establecimientos']

    # Merge datasets
    merged = pd.merge(
        df_precios,
        df_establecimientos,
        left_on="establecimiento",
        right_on="idestablecimientos",
        how="inner"
    )
    
    # Parse date and extract year-month
    merged['fecha'] = pd.to_datetime(merged['fecha'], format='mixed')
    merged = merged.dropna(subset=['fecha'])
    merged['Mes_año'] = merged['fecha'].dt.to_period('M')

    # Group by dept and year-month
    grouped = (
        merged.groupby(['iddepto', 'depto', 'Mes_año'], as_index=False)
        .agg({'precio': 'mean'})
    )

    grouped['Precio_kg'] = grouped['precio'].round(2)
    grouped['Departamento'] = grouped['depto']

    # Keep only relevant columns
    df_precios_mensual = grouped[['Mes_año', 'Departamento', 'Precio_kg']]
    print("Éxito")
    
    # 3.5. FUSIÓN DE INSTANCIAS (CONSTRUCCIÓN DEL ESQUEMA GLOBAL VIRTUAL)
    
    # La Clave Global es: (Mes_año, Departamento) 
    CLAVE_GLOBAL = ['Mes_año', 'Departamento']
    
    # Fusión 1: Clima + Producción (Outer Join para Cobertura y Huecos) 
    
    df_global = df_clima_mensual.merge(
        df_produccion_mensual, 
        on='Mes_año', 
        how='outer'
    )
    
    # Fusión 2: Resultado anterior + Precios (Outer Join)
   
    df_global = df_global.merge(
        df_precios_mensual, 
        on=['Mes_año', 'Departamento'], 
        how='outer'
    )

    # 3.6. RESULTADO FINAL (VISTA GLOBAL UNIFICADA)
    
    # Selecciona y ordena las columnas del Esquema Global Consolidado
    ESQUEMA_GLOBAL_FINAL = [
        'Mes_año', 'Departamento',
        'Precip_Total_mm', 'Temp_Media_C', 'Hum_Media_Pje',
        'Produccion_kg', 'Precio_kg'
        #'origen_fuente'
    ]
    
    vista_global_final = df_global[ESQUEMA_GLOBAL_FINAL]

    print("\n========================================================================")
    print("VISTA GLOBAL UNIFICADA CONSTRUIDA (ESQUEMA GLOBAL VIRTUAL)")
    print("========================================================================\n")
    print(f"Filas totales integradas (solo Manzana): {len(vista_global_final)}")
    
    print("\nEsquema Final:")
    return vista_global_final

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