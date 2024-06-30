import os
import csv
from utils import find_latest_backup
from config import connect_db
from psycopg2.extras import execute_values

# Koneksi ke database PostgreSQL
conn = connect_db()
cursor = conn.cursor()

def restore_full_backup(table_name, file_path=None):
    # Menentukan file backup jika tidak diberikan
    backup_file_directory = os.path.join(os.getcwd(), "backup")
    if file_path is None:
        file_path, _ = find_latest_backup(table_name, backup_file_directory)
        if file_path is None:
            print(f"No backup files found for table {table_name}.")
            return False
        else:
            print(f"Restoring from the latest backup file: {file_path}")

    # Menghapus data yang ada di tabel
    cursor.execute(f"DELETE FROM {table_name}")

    # Membaca data dari file CSV
    with open(file_path, 'r', newline='') as file:
        reader = csv.reader(file)
        header = next(reader)  # Skip header
        records = [tuple(row) for row in reader]

    # Memasukkan data ke dalam tabel
    insert_query = f"INSERT INTO {table_name} ({', '.join(header)}) VALUES %s"
    execute_values(cursor, insert_query, records)
    conn.commit()
    print(f"Data restored successfully from {file_path} into {table_name}")
    return True

def restore_diff_backup(table_name):
    # Restore dari full backup terlebih dahulu
    if not restore_full_backup(table_name):
        return

    backup_file_directory = os.path.join(os.getcwd(), "backup")
    # Cari dan terapkan diff backup
    diff_file, _ = find_latest_backup(table_name, backup_file_directory, prefix="diffbackup")
    if diff_file:
        with open(diff_file, 'r', newline='') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Read and skip header
            records = [tuple(row) for row in reader]

        # Update data menggunakan diff backup dengan upsert
        for record in records:
            # Build dynamic SQL for upsert
            insert_columns = ', '.join(headers)  # All column names
            value_placeholders = ', '.join(['%s' for _ in headers])  # Placeholder for values
            update_assignments = ', '.join([f"{header} = EXCLUDED.{header}" for header in headers[1:]])  # Avoid updating 'id'

            query = f"""
                INSERT INTO {table_name} ({insert_columns})
                VALUES ({value_placeholders})
                ON CONFLICT (id) DO UPDATE SET {update_assignments};
            """
            cursor.execute(query, record)  # Passing record tuple directly

        conn.commit()
        print(f"Diff backup applied successfully from {diff_file}")

# Contoh pemanggilan fungsi
table_name = 'employee'  # Sesuaikan dengan tabel yang ingin direstore
# restore_full_backup(table_name)
# restore_diff_backup(table_name)

# Menutup koneksi database
cursor.close()
conn.close()
