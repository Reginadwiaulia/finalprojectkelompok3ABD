############################ HELPER ############################
from datetime import datetime
import os

def find_latest_backup(table_name, backup_dir, prefix="fullbackup"):
    # Mencari file backup terakhir untuk tabel tertentu di direktori yang diberikan
    latest_file = None
    latest_time = None
    for file in os.listdir(backup_dir):
        if file.startswith(f"{prefix}_") and file.endswith(f"{table_name}.csv"):
            timestamp = datetime.strptime(f"{file.split('_')[1]}_{file.split('_')[2]}", "%Y%m%d_%H%M%S")
            if latest_time is None or timestamp > latest_time:
                latest_time = timestamp
                latest_file = os.path.join(backup_dir, file)
    return latest_file, latest_time
#################################################################