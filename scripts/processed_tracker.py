"""
Evita procesar dos veces el mismo par de archivos (mismo nombre + misma fecha de modificación).

Si reemplazás el Excel por uno nuevo con el mismo nombre, cambia el mtime y se vuelve a procesar.
"""

import json
import os

from config import PROCESSED_RUNS_FILE


def _fingerprint(sap_path, tms_path):
    """Lo mínimo para reconocer "esta corrida ya la hice" sin abrir los xlsx."""
    return {
        "sap_file": os.path.basename(sap_path),
        "tms_file": os.path.basename(tms_path),
        "sap_mtime": os.path.getmtime(sap_path),
        "tms_mtime": os.path.getmtime(tms_path),
    }


def _load_runs():
    """Lista de huellas guardadas; si el archivo no existe o está roto, arrancamos de cero."""
    if not os.path.exists(PROCESSED_RUNS_FILE):
        return []
    with open(PROCESSED_RUNS_FILE, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        return []
    return data


def is_already_processed(sap_path, tms_path):
    """True si este par exacto (incluido mtime) ya está en el JSON."""
    fp = _fingerprint(sap_path, tms_path)
    for run in _load_runs():
        if not isinstance(run, dict):
            continue
        if (
            run.get("sap_file") == fp["sap_file"]
            and run.get("tms_file") == fp["tms_file"]
            and float(run.get("sap_mtime", -1)) == fp["sap_mtime"]
            and float(run.get("tms_mtime", -1)) == fp["tms_mtime"]
        ):
            return True
    return False


def register_processed_run(sap_path, tms_path):
    """Llamar al terminar bien una semana: agrega una entrada y reescribe el JSON."""
    runs = _load_runs()
    fp = _fingerprint(sap_path, tms_path)
    runs.append(fp)
    os.makedirs(os.path.dirname(PROCESSED_RUNS_FILE), exist_ok=True)
    with open(PROCESSED_RUNS_FILE, "w", encoding="utf-8") as f:
        json.dump(runs, f, indent=2)


def pending_week_pairs(week_pairs):
    """
    week_pairs viene ordenado por fecha (vieja → nueva).
    Acá filtramos y dejamos solo los que todavía no están en processed_runs.
    """
    return [
        (d, s, t)
        for d, s, t in week_pairs
        if not is_already_processed(s, t)
    ]
