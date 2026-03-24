"""
Escritura de salidas: CSVs en output/ (acumulativos) y una línea más en el histórico de métricas.

Los CSV de detalle no se pisan: cada corrida agrega filas con columna fecha_reporte.
Si un dataset viene vacío para esa semana, no se agrega nada (los totales siguen en locations_history).
"""

import os
import shutil
import time

import pandas as pd
from config import OUTPUT_FOLDER, HISTORY_FILE

# Primera columna en cada export de detalle: fecha del SAP de esa corrida (YYYYMMDD del nombre).
FECHA_REPORTE_COL = "fecha_reporte"


def _output_should_append(path, name):
    """
    True = el archivo ya existe con el formato nuevo (podemos hacer append).
    False = no hay archivo, está vacío, o era formato viejo (en ese caso archivamos y empezamos de cero).
    """
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return False
    try:
        hdr = pd.read_csv(path, nrows=0)
    except (pd.errors.EmptyDataError, OSError, UnicodeDecodeError):
        return False
    if FECHA_REPORTE_COL in hdr.columns:
        return True
    legacy = os.path.join(OUTPUT_FOLDER, f"{name}_antes_acumulativo.csv")
    if os.path.exists(legacy):
        legacy = os.path.join(
            OUTPUT_FOLDER,
            f"{name}_antes_acumulativo_{time.strftime('%Y%m%d_%H%M%S')}.csv",
        )
    shutil.move(path, legacy)
    print(
        f"Aviso: {name}.csv no tenía columna {FECHA_REPORTE_COL}. "
        f"Se archivó como {os.path.basename(legacy)}; las próximas filas usan el formato nuevo."
    )
    return False


def export_datasets(data, file_date):
    """
    Por cada nombre en data, agrega filas a output/<nombre>.csv.

    file_date es la fecha lógica de la semana (la del nombre del SAP). Así podés filtrar
    en Excel "qué había duplicado / faltante / etc. en tal fecha".

    Si encontrás un CSV viejo sin fecha_reporte, se renombra a *_antes_acumulativo.csv
    la primera vez que toca ese archivo, para no mezclar columnas.
    """
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    fecha_val = file_date.isoformat() if hasattr(file_date, "isoformat") else str(file_date)

    for name, df in data.items():
        if df is None or df.empty:
            continue

        path = os.path.join(OUTPUT_FOLDER, f"{name}.csv")
        out = df.copy()
        out.insert(0, FECHA_REPORTE_COL, fecha_val)

        append_mode = _output_should_append(path, name)
        out.to_csv(path, mode="a" if append_mode else "w", header=not append_mode, index=False)


def update_history(stats):
    """
    stats: una fila (o varias) con los contadores de la corrida.
    Si ya existe locations_history.csv, append sin repetir encabezado.
    """
    if os.path.exists(HISTORY_FILE):
        stats.to_csv(HISTORY_FILE, mode="a", header=False, index=False)
    else:
        stats.to_csv(HISTORY_FILE, index=False)
