import os
import pandas as pd
from config import OUTPUT_FOLDER, HISTORY_FILE


def export_datasets(data):

    for name, df in data.items():

        path = os.path.join(OUTPUT_FOLDER, f"{name}.csv")

        df.to_csv(path, index=False)


def update_history(stats):

    if os.path.exists(HISTORY_FILE):

        stats.to_csv(HISTORY_FILE, mode="a", header=False, index=False)

    else:

        stats.to_csv(HISTORY_FILE, index=False)
