# /api/main.py
import os
import math
import json
from http.server import BaseHTTPRequestHandler

import pandas as pd

# ---------------------------------------------------------------------------
# Paths (Vercel-friendly)
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def csv_path(filename: str) -> str:
    return os.path.join(BASE_DIR, filename)

DF_VISTA_GLOBAL_PATH = csv_path("df_vista_global.csv")

# ---------------------------------------------------------------------------
# Helpers carga CSV
# ---------------------------------------------------------------------------

def load_df_vista_global() -> pd.DataFrame:
    """
    Carga df_vista_global.csv desde /api y devuelve el DataFrame.
    Soporta ; o , y encodings comunes.
    No altera nombres de columnas.
    """
    if not os.path.exists(DF_VISTA_GLOBAL_PATH):
        raise FileNotFoundError(f"No se encontró {DF_VISTA_GLOBAL_PATH}")

    ENCODINGS = ["utf-8", "utf-8-sig", "latin1", "iso-8859-1", "cp1252"]
    SEPARATORS = [";", ","]

    last_exc = None
    for enc in ENCODINGS:
        for sep in SEPARATORS:
            try:
                df = pd.read_csv(
                    DF_VISTA_GLOBAL_PATH,
                    sep=sep,
                    encoding=enc,
                    on_bad_lines="skip"
                )
                # Evita falso positivo de una sola columna enorme
                if df.shape[1] > 1:
                    print(
                        f"df_vista_global leído OK con encoding={enc}, sep='{sep}', "
                        f"filas={len(df)}, cols={list(df.columns)}"
                    )
                    return df
            except Exception as e:
                last_exc = e
                continue

    if last_exc:
        raise last_exc
    raise ValueError("No se pudo leer df_vista_global.csv con los formatos probados.")

# ---------------------------------------------------------------------------
# Helpers JSON
# ---------------------------------------------------------------------------

def sanitize_for_json(obj):
    """
    Convierte NaN/Inf en None de forma recursiva.
    """
    if isinstance(obj, float):
        if math.isfinite(obj):
            return obj
        return None

    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        return [sanitize_for_json(v) for v in obj]

    return obj


def dataframe_to_json_records(df: pd.DataFrame):
    """
    Convierte el DataFrame en lista de dicts listo para JSON.

    - Asegura Mes_año como string si existe.
    - Reemplaza NaN por None.
    """
    if df is None or df.empty:
        return []

    df = df.copy()

    if "Mes_año" in df.columns:
        df["Mes_año"] = df["Mes_año"].astype(str)

    # NaN -> None
    df = df.where(pd.notnull(df), None)

    records = df.to_dict(orient="records")
    return sanitize_for_json(records)

# ---------------------------------------------------------------------------
# HTTP handler para Vercel (/api/main)
# ---------------------------------------------------------------------------

class handler(BaseHTTPRequestHandler):
    """
    Vercel usa esta clase para manejar /api/main.
    """

    def _set_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _send_json(self, status_code: int, payload: dict):
        safe_payload = sanitize_for_json(payload)
        body = json.dumps(
            safe_payload,
            ensure_ascii=False,
            allow_nan=False
        ).encode("utf-8")

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
            df = load_df_vista_global()
            data = dataframe_to_json_records(df)
            self._send_json(200, {"data": data})
        except FileNotFoundError as e:
            print(f"ERROR en /api/main: {e}")
            self._send_json(500, {"error": "No se encontró df_vista_global.csv en /api"})
        except Exception as e:
            print(f"ERROR en /api/main: {e}")
            self._send_json(500, {"error": "Error interno al leer df_vista_global"})
