import psycopg2
from config import connect_db

def ship_logs():
    # Koneksi ke database primary untuk mengambil log
    conn_primary = connect_db()
    cursor_primary = conn_primary.cursor()
    cursor_primary.execute("SELECT * FROM transaction_log WHERE shipped = FALSE")
    logs = cursor_primary.fetchall()

    # Koneksi ke database secondary untuk menyimpan log
    conn_secondary = connect_db(name="CONFIG_SECONDARY")
    cursor_secondary = conn_secondary.cursor()

    # Menyalin log ke database secondary
    for log in logs:
        cursor_secondary.execute(
            "INSERT INTO transaction_log (log_id, table_name, operation_type, operation_timestamp, operation_details) VALUES (%s, %s, %s, %s, %s)",
            (log[0], log[1], log[2], log[3], log[4])
        )
    
    # Tandai log sebagai shipped di database primary
    if logs:
        log_ids = tuple(log[0] for log in logs)
        cursor_primary.execute(
            "UPDATE transaction_log SET shipped = TRUE WHERE log_id IN %s",
            (log_ids,)
        )
    
    # Commit transaksi di kedua database
    conn_primary.commit()
    conn_secondary.commit()

    # Menutup koneksi
    cursor_primary.close()
    conn_secondary.close()
    conn_primary.close()
    conn_secondary.close()

    print("Log shipping completed.")

ship_logs()