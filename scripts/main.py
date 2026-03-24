import pandas as pd
from file_loader import load_data
from scripts.processor import normalize_ids, detect_duplicates, compare_systems, detect_bay_locations, count_tipo_cuenta, detect_new_locations,detect_removed_locations,save_current_ids
from exporters import export_datasets, update_history
from config import HISTORY_IDS_FILE

#Cargar datos de los archivos
sap, tms, file_date = load_data()

# Normalizar columnas de ID en archivos TMS y SAP
sap, tms = normalize_ids(sap, tms)

# Contar shiptos y soldtos en SAP
sold_to, ship_to = count_tipo_cuenta(sap)

#detectar duplicados en SAP
sap_duplicates, sap = detect_duplicates(sap)

# buscar faltantes en TMS
missing_in_tms, extra_in_tms, id_differences = compare_systems(sap, tms)

#detectar nuevas ubicaciones en SAP semanalmente comparando con el histórico de IDs
new_location = detect_new_locations(sap, HISTORY_IDS_FILE)

#comparar la inversa, cuales se dieron de baja, respecto a la semana anterior
removed_location = detect_removed_locations(sap, HISTORY_IDS_FILE)

#detectar ubicaciones BAY
bay_locations = detect_bay_locations(tms)

datasets = {
    "missing_in_tms": missing_in_tms,
    "bay_locations": bay_locations,
    "sap_duplicates": sap_duplicates,
    "new_locations": new_location,
    "removed_locations": removed_location,

}

#exportar archivos
export_datasets(datasets)

stats = pd.DataFrame([{
    "date": file_date,
    "sap_total": len(sap),
    "tms_total": len(tms),
    "missing_in_tms": len(missing_in_tms),
    "bay_locations": len(bay_locations),
    "sap_duplicates": len(sap_duplicates),
    "new_locations": len(new_location),
    "removed_locations": len(removed_location),
    "sold_to": sold_to,
    "ship_to": ship_to

}])

update_history(stats)

# guardar las ubicaciones actuales de SAP para la proxima comparacion
save_current_ids(sap, HISTORY_IDS_FILE)

print("Proceso finalizado correctamente")
