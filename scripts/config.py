import os

# Acá van las rutas fijas de tu máquina. Si movés el proyecto, tocá solo BASE_PATH.
BASE_PATH = r"C:\Users\EFNYZ\OneDrive - Bayer\Desktop\Analisis Ship to's"

# Excel de SAP y TMS que bajás cada semana (el script los busca por patrón de nombre).
SAP_FOLDER = os.path.join(BASE_PATH, "SAP")
TMS_FOLDER = os.path.join(BASE_PATH, "TMS")

OUTPUT_FOLDER = os.path.join(BASE_PATH, "output")
HISTORY_FOLDER = os.path.join(BASE_PATH, "history")

# Una fila por cada semana ya procesada: totales y contadores para ver evolución.
HISTORY_FILE = os.path.join(HISTORY_FOLDER, "locations_history.csv")

# Nombre de la columna de ID en cada export (viene así del reporte).
SAP_ID_COLUMN = "Consulta1[IDSAP]"
TMS_ID_COLUMN = "Location ID"

# Lista de IDs SAP de la última corrida; sirve para ver qué es "nuevo" o se fue.
HISTORY_IDS_FILE = os.path.join(HISTORY_FOLDER, "last_sap_ids.csv")

# Copia completa del SAP (sin duplicados) de la última vez: sin esto no podés armar
# el detalle de las bajas, porque esos IDs ya no están en el Excel nuevo.
LAST_SAP_SNAPSHOT_FILE = os.path.join(HISTORY_FOLDER, "last_sap_snapshot.csv")

# Qué pares SAP+TMS ya corrieron; así no duplicás filas en el historial si ejecutás dos veces.
PROCESSED_RUNS_FILE = os.path.join(HISTORY_FOLDER, "processed_runs.json")
