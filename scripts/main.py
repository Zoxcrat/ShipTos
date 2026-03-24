"""
Punto de entrada: arma la cola de semanas pendientes y las procesa en orden.

Flujo general:
  1) Encontrar todos los pares SAP+TMS por fecha en el nombre del archivo.
  2) Sacar los que ya están en processed_runs.json.
  3) Por cada semana que queda (vieja → nueva), correr el análisis y actualizar historial / snapshots.
"""

import sys

import pandas as pd
from file_loader import build_week_pairs, load_data
from processed_tracker import pending_week_pairs, register_processed_run
from scripts.processor import (
    normalize_ids,
    detect_duplicates,
    compare_systems,
    detect_bay_locations,
    count_tipo_cuenta,
    detect_new_locations,
    detect_removed_locations,
    save_current_ids,
    save_sap_snapshot,
)
from exporters import export_datasets, update_history
from config import HISTORY_IDS_FILE, LAST_SAP_SNAPSHOT_FILE


def run_one_week(sap_path, tms_path, file_date):
    """
    Todo el pipeline para una sola semana: lee Excel, cruza datos, escribe CSVs
    y deja listo el estado para la próxima (IDs, snapshot, registro de corrida).
    """
    sap, tms, loaded_date = load_data(sap_path, tms_path)

    # Por las dudas: la fecha del par debería coincidir con la del nombre del SAP.
    if loaded_date != file_date:
        print(
            f"Aviso: fecha del nombre SAP ({loaded_date}) distinta a la del par ({file_date}); "
            "se usa la extraída del archivo."
        )
        file_date = loaded_date

    sap, tms = normalize_ids(sap, tms)

    sold_to, ship_to = count_tipo_cuenta(sap)

    # Duplicados se reportan aparte; el resto del análisis usa un registro por ID.
    sap_duplicates, sap = detect_duplicates(sap)

    missing_in_tms, extra_in_tms, id_differences = compare_systems(sap, tms)

    new_location = detect_new_locations(sap, HISTORY_IDS_FILE)

    removed_location = detect_removed_locations(
        sap, HISTORY_IDS_FILE, LAST_SAP_SNAPSHOT_FILE
    )

    bay_locations = detect_bay_locations(tms)

    datasets = {
        "missing_in_tms": missing_in_tms,
        "bay_locations": bay_locations,
        "sap_duplicates": sap_duplicates,
        "new_locations": new_location,
        "removed_locations": removed_location,
    }

    export_datasets(datasets)

    stats = pd.DataFrame(
        [
            {
                "date": file_date,
                "sap_total": len(sap),
                "tms_total": len(tms),
                "missing_in_tms": len(missing_in_tms),
                "bay_locations": len(bay_locations),
                "sap_duplicates": len(sap_duplicates),
                "new_locations": len(new_location),
                "removed_locations": len(removed_location),
                "sold_to": sold_to,
                "ship_to": ship_to,
            }
        ]
    )

    update_history(stats)

    save_current_ids(sap, HISTORY_IDS_FILE)
    save_sap_snapshot(sap, LAST_SAP_SNAPSHOT_FILE)

    register_processed_run(sap_path, tms_path)


# --- arranque ---

week_pairs = build_week_pairs()

if not week_pairs:
    print(
        "No hay pares SAP+TMS con fecha YYYYMMDD en el nombre en las carpetas configuradas."
    )
    sys.exit(0)

to_process = pending_week_pairs(week_pairs)

if not to_process:
    print(
        "No hay semanas pendientes: todos los pares SAP/TMS actuales ya fueron procesados.\n"
        "Para reprocesar, ajustá history/processed_runs.json o subí archivos nuevos "
        "(otro nombre o fecha de modificación distinta)."
    )
    sys.exit(0)

print(
    f"Semanas pendientes: {len(to_process)} "
    f"({to_process[0][0]} … {to_process[-1][0]})\n"
)

for file_date, sap_path, tms_path in to_process:
    print(f"========== Semana {file_date} ==========")
    run_one_week(sap_path, tms_path, file_date)
    print(f"Semana {file_date} OK.\n")

print("Proceso finalizado correctamente.")
