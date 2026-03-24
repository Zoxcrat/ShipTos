"""
Lógica de negocio sobre los DataFrames: normalizar IDs, cruces, duplicados, altas/bajas, etc.

Ojo con los tipos: los IDs siempre se trabajan como string para que el CSV del histórico
no nos juegue en contra (pandas suele leer números como int y después no matchean).
"""

import pandas as pd
from config import SAP_ID_COLUMN, TMS_ID_COLUMN
import os


def normalize_ids(sap, tms):
    """
    Renombra las columnas de ID a "ID" en ambos lados y las deja como texto limpio.
    Sin esto el cruce SAP/TMS es un dolor (nombres distintos, espacios, tipos raros).
    """
    sap = sap.rename(columns={SAP_ID_COLUMN: "ID"})
    tms = tms.rename(columns={TMS_ID_COLUMN: "ID"})

    sap["ID"] = sap["ID"].astype(str).str.strip()
    tms["ID"] = tms["ID"].astype(str).str.strip()

    return sap, tms


def detect_duplicates(sap):
    """
    Filas SAP que comparten ID (las dos aparecen en el reporte de duplicados).
    Devuelve también una versión de sap con un solo renglón por ID para el resto del pipeline.
    """
    duplicates = sap[sap.duplicated(subset="ID", keep=False)]
    sap_clean = sap.drop_duplicates(subset="ID")
    return duplicates, sap_clean


def compare_systems(sap, tms):
    """
    Marca existencia cruzada y arma:
    - missing_in_tms: está en SAP pero no en TMS (lo que más suele importar)
    - extra_in_tms: en TMS pero no en SAP
    - id_differences: concatenación de IDs de ambos lados (por si lo querés debuggear)
    """
    sap["exists_in_tms"] = sap["ID"].isin(tms["ID"])
    tms["exists_in_sap"] = tms["ID"].isin(sap["ID"])

    missing_in_tms = sap[~sap["exists_in_tms"]]
    extra_in_tms = tms[~tms["exists_in_sap"]]

    id_differences = pd.concat(
        [missing_in_tms[["ID"]], extra_in_tms[["ID"]]]
    )

    return missing_in_tms, extra_in_tms, id_differences


def detect_bay_locations(tms):
    """IDs que en TMS empiezan con BA (regla de negocio que ya venías usando)."""
    return tms[tms["ID"].str.startswith("BA", na=False)]


def count_tipo_cuenta(sap):
    """Cuenta SoldTo vs ShipTo según la columna del export SAP."""
    counts = sap["Consulta1[TipoCuenta]"].value_counts()
    sold_to = counts.get("SoldTo", 0)
    ship_to = counts.get("ShipTo", 0)
    return sold_to, ship_to


def detect_new_locations(sap, history_ids_path):
    """
    IDs que están en este SAP y no estaban en last_sap_ids.csv (corrida anterior).

    La primera vez que corrés no hay archivo: devolvemos vacío porque no son "altas",
    es la línea base. Después sí tiene sentido el delta semana a semana.
    """
    current_ids = set(sap["ID"])

    if not os.path.exists(history_ids_path):
        return pd.DataFrame(columns=sap.columns)

    previous_ids = set(
        pd.read_csv(history_ids_path)["ID"].astype(str).str.strip()
    )

    new_ids = current_ids - previous_ids
    return sap[sap["ID"].isin(new_ids)]


def detect_removed_locations(current_sap, history_ids_path, snapshot_path):
    """
    IDs que estaban la vez pasada y ahora no vienen en el SAP.

    No podés buscar esas filas en el Excel nuevo: ya no están. Por eso las sacamos del
    snapshot completo que guardamos al final de la corrida anterior (last_sap_snapshot.csv).
    """
    current_ids = set(current_sap["ID"])

    if not os.path.exists(history_ids_path):
        return pd.DataFrame(columns=current_sap.columns)

    previous_ids = set(
        pd.read_csv(history_ids_path)["ID"].astype(str).str.strip()
    )

    removed_ids = previous_ids - current_ids

    if not removed_ids or not os.path.exists(snapshot_path):
        return pd.DataFrame(columns=current_sap.columns)

    previous_sap = pd.read_csv(snapshot_path)
    if "ID" not in previous_sap.columns:
        return pd.DataFrame(columns=current_sap.columns)

    previous_sap = previous_sap.copy()
    previous_sap["ID"] = previous_sap["ID"].astype(str).str.strip()

    return previous_sap[previous_sap["ID"].isin(removed_ids)]


def save_current_ids(sap, history_ids_path):
    """Solo la columna ID, sin duplicados: es el "universo" SAP para la próxima comparación."""
    sap[["ID"]].drop_duplicates().to_csv(history_ids_path, index=False)


def save_sap_snapshot(sap, snapshot_path):
    """Todo el SAP ya limpio (post-duplicados): necesario para armar el CSV de bajas la semana que viene."""
    sap.to_csv(snapshot_path, index=False)
