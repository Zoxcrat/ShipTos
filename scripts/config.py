import os

# Carpeta base del proyecto
BASE_PATH = r"C:\Users\EFNYZ\OneDrive - Bayer\Desktop\Analisis Ship to's"

# Carpetas de datos
SAP_FOLDER = os.path.join(BASE_PATH, "SAP")
TMS_FOLDER = os.path.join(BASE_PATH, "TMS")

OUTPUT_FOLDER = os.path.join(BASE_PATH, "output")
HISTORY_FOLDER = os.path.join(BASE_PATH, "history")

# Archivo histórico
HISTORY_FILE = os.path.join(HISTORY_FOLDER, "locations_history.csv")

# Columnas de ID en cada sistema
SAP_ID_COLUMN = "Consulta1[IDSAP]"
TMS_ID_COLUMN = "Location ID"

HISTORY_IDS_FILE = os.path.join(HISTORY_FOLDER, "last_sap_ids.csv")