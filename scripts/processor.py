import pandas as pd
from config import SAP_ID_COLUMN, TMS_ID_COLUMN
import os

#normalizar columnas de ID
def normalize_ids(sap, tms):

    sap = sap.rename(columns={SAP_ID_COLUMN: "ID"})
    tms = tms.rename(columns={TMS_ID_COLUMN: "ID"})

    sap["ID"] = sap["ID"].astype(str).str.strip()
    tms["ID"] = tms["ID"].astype(str).str.strip()

    return sap, tms


def detect_duplicates(sap):

    duplicates = sap[sap.duplicated(subset="ID", keep=False)]

    sap_clean = sap.drop_duplicates(subset="ID")

    return duplicates, sap_clean


def compare_systems(sap, tms):

    sap["exists_in_tms"] = sap["ID"].isin(tms["ID"])
    tms["exists_in_sap"] = tms["ID"].isin(sap["ID"])

    missing_in_tms = sap[~sap["exists_in_tms"]]
    extra_in_tms = tms[~tms["exists_in_sap"]]

    id_differences = pd.concat([
        missing_in_tms[["ID"]],
        extra_in_tms[["ID"]]
    ])

    return missing_in_tms, extra_in_tms, id_differences



def detect_bay_locations(tms):

    return tms[tms["ID"].str.startswith("BA", na=False)]


def count_tipo_cuenta(sap):

    counts = sap["Consulta1[TipoCuenta]"].value_counts()

    sold_to = counts.get("SoldTo", 0)
    ship_to = counts.get("ShipTo", 0)

    return sold_to, ship_to


def detect_new_locations(sap,history_ids_path):

    current_ids = set(sap["ID"])

    if not os.path.exists(history_ids_path):
        return sap.copy()
    
    previous_ids = pd.read_csv(history_ids_path)["ID"]
    previous_ids=set(previous_ids)

    new_ids = current_ids - previous_ids

    new_locations = sap[sap["ID"].isin(new_ids)]

    return new_locations

def detect_removed_locations(sap,history_ids_path):

    current_ids = set(sap["ID"])

    if not os.path.exists(history_ids_path):
        return pd.DataFrame(columns=sap.columns)
    
    previous_ids = pd.read_csv(history_ids_path)["ID"]
    previous_ids=set(previous_ids)

    removed_ids = previous_ids - current_ids

    removed_locations = sap[sap["ID"].isin(removed_ids)]

    return removed_locations


def save_current_ids(sap,history_ids_path):

    sap[["ID"]].drop_duplicates().to_csv(history_ids_path, index=False)