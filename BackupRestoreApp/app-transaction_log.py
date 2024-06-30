from config import connect_db

def fetch_transaction_logs():
    # Koneksi ke database PostgreSQL
    conn = connect_db()
    cursor = conn.cursor()

    # Mengambil log transaksi
    cursor.execute("SELECT * FROM transaction_log ORDER BY operation_timestamp DESC")
    logs = cursor.fetchall()

    # Menampilkan log atau menyimpannya ke dalam file
    for log in logs:
        print(log)  # Atau menulis ke dalam file
    
    # Menutup koneksi
    cursor.close()
    conn.close()

fetch_transaction_logs()
