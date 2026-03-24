import pandas as pd
import glob
import os
import re
from datetime import datetime
from config import SAP_FOLDER, TMS_FOLDER


def get_latest_sap_file():

    # Buscar archivos SAP con el formato esperado
    sap_files = glob.glob(os.path.join(SAP_FOLDER, "SAP - REPORTE DISTRIBUIDORES *.xlsx"))

    # Elegir el más reciente
    latest_file = max(sap_files, key=os.path.getctime)

    return latest_file



def get_latest_tms_file():

    tms_files = glob.glob(os.path.join(TMS_FOLDER, "TMS - UBICACIONES DE ENVIO *.xlsx"))

    latest_file = max(tms_files, key=os.path.getctime)

    return latest_file



def extract_date_from_filename(file):

    date_match = re.search(r'\d{8}', file)

    return datetime.strptime(date_match.group(), "%Y%m%d").date()



def load_data():

    sap_file = get_latest_sap_file()
    tms_file = get_latest_tms_file()

    print("Archivo SAP utilizado:", sap_file)
    print("Archivo TMS utilizado:", tms_file)

    sap = pd.read_excel(sap_file)
    tms = pd.read_excel(tms_file)

    file_date = extract_date_from_filename(sap_file)

    return sap, tms, file_date
