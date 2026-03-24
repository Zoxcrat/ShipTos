"""
Escritura de salidas: CSVs en output/ y una línea más en el histórico de métricas.
"""

import os
import pandas as pd
from config import OUTPUT_FOLDER, HISTORY_FILE


def export_datasets(data):
    """
    data es un dict nombre → DataFrame. Cada uno sale a output/<nombre>.csv.
    Ojo: si procesás varias semanas en un solo run, el último pisa los CSVs de la anterior
    (el detalle queda en locations_history.csv por fecha).
    """
    for name, df in data.items():
        path = os.path.join(OUTPUT_FOLDER, f"{name}.csv")
        df.to_csv(path, index=False)


def update_history(stats):
    """
    stats: una fila (o varias) con los contadores de la corrida.
    Si ya existe locations_history.csv, append sin repetir encabezado.
    """
    if os.path.exists(HISTORY_FILE):
        stats.to_csv(HISTORY_FILE, mode="a", header=False, index=False)
    else:
        stats.to_csv(HISTORY_FILE, index=False)
