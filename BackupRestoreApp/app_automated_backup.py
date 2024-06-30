from datetime import datetime
import os
from app_backup import find_latest_backup
from config import connect_db
import csv
from apscheduler.schedulers.background import BackgroundScheduler

def perform_backup(backup_type="full"):
    conn = connect_db()
    cursor = conn.cursor()

    backup_file_directory = os.path.join(os.getcwd(), "backup")
    if not os.path.exists(backup_file_directory):
        os.makedirs(backup_file_directory)

    # Retrieve all non-system tables
    cursor.execute("""
        SELECT tablename FROM pg_catalog.pg_tables
        WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';
    """)
    tables = cursor.fetchall()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for table in tables:
        table_name = table[0]

        if backup_type == "diff":
            _, latest_backup_time = find_latest_backup(table_name, backup_file_directory)
            if not latest_backup_time:
                continue  # Skip diff backup if no full backup is available
            cursor.execute(f"SELECT * FROM {table_name} WHERE updated_at > %s", (latest_backup_time,))
        else:
            cursor.execute(f"SELECT * FROM {table_name}")

        records = cursor.fetchall()
        if not records:
            continue  # No changes detected

        backup_file = os.path.join(backup_file_directory, f"{backup_type}backup_{timestamp}_{table_name}.csv")

        with open(backup_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([i[0] for i in cursor.description])  # Write header
            writer.writerows(records)

        print(f"{backup_type.capitalize()} backup of {table_name} completed successfully. File saved as {backup_file}")

    cursor.close()
    conn.close()

def schedule_backups():
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: perform_backup("full"), 'cron', day_of_week='sun', hour=0)  # Weekly full backup
    scheduler.add_job(lambda: perform_backup("diff"), 'cron', hour='*/4')  # Differential backup every 4 hours
    scheduler.start()

    print("Backup scheduler running. Press Ctrl+C to exit.")
    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

def testing_every_five_sec():
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: perform_backup("full"), 'interval', seconds=5)  # Full backup every 5 seconds
    scheduler.start()

    print("Backup scheduler running. Press Ctrl+C to exit.")
    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

if __name__ == "__main__":
    # schedule_backups()
    testing_every_five_sec()
