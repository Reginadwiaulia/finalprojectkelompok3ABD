import psycopg2
from config import connect_db
from datetime import datetime

def log_error_directly(error_msg, context):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO error_log (error_message, context, error_timestamp)
            VALUES (%s, %s, %s)
        """, (error_msg, context, datetime.now()))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def simulate_insert_error():
    conn = connect_db()
    cursor = conn.cursor()
    try:
        # Correcting the intentional error to demonstrate another type of error
        cursor.execute("INSERT INTO employee (name, age, gender) VALUES (%s, %s, %s)", ("John Doe", "not-number", "Male"))
        # Introduce an error by violating some constraint or logic here if needed
    except psycopg2.DataError as e:
        print(f"Caught a data error: {e}")
        log_error_directly(str(e), "Data insertion error in employee table")
    except Exception as e:
        print(f"Caught an error: {e}")
        log_error_directly(str(e), "General error in employee table")
    finally:
        cursor.close()
        conn.close()

def check_error_log():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM error_log ORDER BY error_timestamp DESC LIMIT 1")
    error_entry = cursor.fetchone()
    cursor.close()
    conn.close()
    return error_entry

if __name__ == "__main__":
    simulate_insert_error()
    error_log_entry = check_error_log()
    if error_log_entry:
        print("Most recent error log entry:")
        print(f"Log ID: {error_log_entry[0]}, Message: {error_log_entry[1]}, Context: {error_log_entry[2]}, Timestamp: {error_log_entry[3]}")
    else:
        print("No error logs found.")
