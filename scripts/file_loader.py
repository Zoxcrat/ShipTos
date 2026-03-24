"""
Encuentra los Excel de SAP y TMS, empareja por fecha en el nombre y carga tablas.

La fecha se saca del primer bloque YYYYMMDD que aparezca en la ruta (típico en los nombres de export).
"""

import pandas as pd
import glob
import os
import re
from collections import defaultdict
from datetime import datetime
from config import SAP_FOLDER, TMS_FOLDER


def get_latest_sap_file():
    """Útil si querés el SAP más nuevo sin armar la cola de semanas (p. ej. otros scripts)."""
    sap_files = glob.glob(os.path.join(SAP_FOLDER, "SAP - REPORTE DISTRIBUIDORES *.xlsx"))
    # ctime = cuándo apareció el archivo en esta carpeta (p. ej. al copiarlo), no el contenido lógico
    latest_file = max(sap_files, key=os.path.getctime)
    return latest_file


def get_latest_tms_file():
    """Igual que el SAP pero para la carpeta TMS."""
    tms_files = glob.glob(os.path.join(TMS_FOLDER, "TMS - UBICACIONES DE ENVIO *.xlsx"))
    latest_file = max(tms_files, key=os.path.getctime)
    return latest_file


def try_extract_date_from_filename(path):
    """
    Si el nombre trae algo como ...20260310..., devuelve esa fecha.
    Si no hay 8 dígitos seguidos, None (el archivo no entra al emparejamiento por semana).
    """
    date_match = re.search(r"\d{8}", path)
    if not date_match:
        return None
    return datetime.strptime(date_match.group(), "%Y%m%d").date()


def extract_date_from_filename(file):
    """Misma idea que try_*, pero falla en voz alta: load_data asume que el SAP tiene fecha en el nombre."""
    d = try_extract_date_from_filename(file)
    if d is None:
        raise ValueError(f"No se encontró fecha YYYYMMDD en el nombre: {file}")
    return d


def build_week_pairs():
    """
    Devuelve una lista ordenada por fecha: (fecha, ruta_sap, ruta_tms).

    Solo entran semanas donde existen los dos lados con la misma fecha en el nombre.
    Si subiste dos SAP de la misma semana, gana el que tenga ctime más reciente (el último que tocaste).
    """
    sap_files = glob.glob(os.path.join(SAP_FOLDER, "SAP - REPORTE DISTRIBUIDORES *.xlsx"))
    tms_files = glob.glob(os.path.join(TMS_FOLDER, "TMS - UBICACIONES DE ENVIO *.xlsx"))

    sap_by_date = defaultdict(list)
    tms_by_date = defaultdict(list)
    undated_sap = []
    undated_tms = []

    for p in sap_files:
        d = try_extract_date_from_filename(p)
        if d is None:
            undated_sap.append(p)
        else:
            sap_by_date[d].append(p)

    for p in tms_files:
        d = try_extract_date_from_filename(p)
        if d is None:
            undated_tms.append(p)
        else:
            tms_by_date[d].append(p)

    for p in undated_sap:
        print("Aviso: SAP sin fecha YYYYMMDD en el nombre, se omite:", p)
    for p in undated_tms:
        print("Aviso: TMS sin fecha YYYYMMDD en el nombre, se omite:", p)

    sap_dates = set(sap_by_date)
    tms_dates = set(tms_by_date)

    # Semanas incompletas: avisamos pero no las mezclamos con otra fecha.
    for d in sorted(sap_dates - tms_dates):
        print(
            f"Aviso: hay SAP para la fecha {d} pero no hay TMS con esa fecha; no se procesa esa semana."
        )
    for d in sorted(tms_dates - sap_dates):
        print(
            f"Aviso: hay TMS para la fecha {d} pero no hay SAP con esa fecha; no se procesa esa semana."
        )

    common = sorted(sap_dates & tms_dates)
    pairs = []
    for d in common:
        sap_path = max(sap_by_date[d], key=os.path.getctime)
        tms_path = max(tms_by_date[d], key=os.path.getctime)
        pairs.append((d, sap_path, tms_path))

    return pairs


def resolve_latest_inputs():
    """Par (SAP, TMS) más nuevo por fecha de archivo en disco; lo usa load_data si no le pasás rutas."""
    return get_latest_sap_file(), get_latest_tms_file()


def load_data(sap_file=None, tms_file=None):
    """
    Lee los dos Excel y devuelve (sap_df, tms_df, fecha_del_nombre_sap).

    Si no pasás rutas, agarra el último de cada carpeta (comportamiento clásico de una sola corrida).
    """
    if sap_file is None or tms_file is None:
        sap_file, tms_file = resolve_latest_inputs()

    print("Archivo SAP utilizado:", sap_file)
    print("Archivo TMS utilizado:", tms_file)

    sap = pd.read_excel(sap_file)
    tms = pd.read_excel(tms_file)

    file_date = extract_date_from_filename(sap_file)

    return sap, tms, file_date
