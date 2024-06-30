
from datetime import datetime
import os
from config import connect_db
import csv
from utils import find_latest_backup

conn = connect_db()
cursor = conn.cursor()

def full_backup():
    # Mendapatkan direktori saat ini dimana script Python dijalankan
    backup_file_directory = os.path.join(os.getcwd(), "backup")
    
    # Mendapatkan daftar semua tabel yang bukan tabel sistem
    cursor.execute("""
        SELECT tablename FROM pg_catalog.pg_tables
        WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';
    """)
    tables = cursor.fetchall()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Backup setiap tabel
    for table in tables:
        table_name = table[0]
        backup_file = os.path.join(backup_file_directory, f"fullbackup_{timestamp}_{table_name}.csv")

        # Mengambil semua data dari tabel saat ini
        cursor.execute(f"SELECT * FROM {table_name}")
        records = cursor.fetchall()

        # Membuat file CSV dan menulis data
        with open(backup_file, 'w', newline='') as file:
            writer = csv.writer(file)
            # Menulis header
            writer.writerow([i[0] for i in cursor.description])
            # Menulis data
            writer.writerows(records)

        print(f"Backup of {table_name} completed successfully. File saved as {backup_file}")

def diff_backup():
    backup_file_directory = os.path.join(os.getcwd(), "backup")

    # Mendapatkan daftar semua tabel yang bukan tabel sistem
    cursor.execute("""
        SELECT tablename FROM pg_catalog.pg_tables
        WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';
    """)
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        if table_name == 'transaction_log' or table_name == 'error_log':
            continue
        _, latest_backup_time = find_latest_backup(table_name, backup_file_directory)
        
        if latest_backup_time is not None:
            # Mengambil perubahan sejak full backup terakhir
            cursor.execute(f"SELECT * FROM {table_name} WHERE updated_at > %s", (latest_backup_time,))
            records = cursor.fetchall()

            if records:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                diff_backup_file = os.path.join(backup_file_directory, f"diffbackup_{timestamp}_{table_name}.csv")
                
                # Membuat file CSV dan menulis data
                with open(diff_backup_file, 'w', newline='') as file:
                    writer = csv.writer(file)
                    # Menulis header
                    writer.writerow([i[0] for i in cursor.description])
                    # Menulis data
                    writer.writerows(records)

                print(f"Diff backup of {table_name} completed successfully. File saved as {diff_backup_file}")
            else:
                print(f"No changes to backup for {table_name} since last full backup.")
        else:
            print(f"No full backup found for {table_name}, skipping diff backup.")

# full_backup()
# diff_backup()

# Menutup koneksi database
cursor.close()
conn.close()