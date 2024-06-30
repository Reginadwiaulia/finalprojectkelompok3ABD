from config import connect_db

conn = connect_db()
cursor = conn.cursor()

# Create employee table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS employee (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        age INT,
        gender VARCHAR(10),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
''')

# Create transaction log table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS transaction_log (
        log_id SERIAL PRIMARY KEY,
        table_name VARCHAR(255),
        operation_type VARCHAR(50),
        operation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        operation_details TEXT,
        shipped BOOLEAN DEFAULT FALSE
    );
''')

# Create error_log table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS error_log (
        log_id SERIAL PRIMARY KEY,
        error_message TEXT,
        context TEXT,
        error_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
''')

# Function to handle logging and error capturing
cursor.execute('''
    CREATE OR REPLACE FUNCTION log_operations()
    RETURNS TRIGGER AS $$
    BEGIN
        IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN
            INSERT INTO transaction_log(table_name, operation_type, operation_details)
            VALUES (TG_TABLE_NAME, TG_OP, row_to_json(COALESCE(NEW, OLD)));
        END IF;
        RETURN NULL;
    EXCEPTION
        WHEN OTHERS THEN
            INSERT INTO error_log(error_message, context)
            VALUES (SQLERRM, 'Error during ' || TG_OP || ' on ' || TG_TABLE_NAME);
            RAISE;
    END;
    $$ LANGUAGE plpgsql;
''')

# Create a single trigger for all operations
cursor.execute('''
    CREATE TRIGGER employee_operations
    AFTER INSERT OR UPDATE OR DELETE ON employee
    FOR EACH ROW EXECUTE FUNCTION log_operations();
''')

# Fungsi trigger untuk update `updated_at`
cursor.execute('''
    CREATE OR REPLACE FUNCTION update_modified_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = NOW(); -- Set updated_at ke waktu sekarang
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
''')

# Trigger untuk `updated_at`
cursor.execute('''
    CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON employee
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();
''')

# secondary database
conn2 = connect_db(name="CONFIG_SECONDARY")
cursor2 = conn2.cursor()
cursor2.execute('''
    CREATE TABLE IF NOT EXISTS transaction_log (
        log_id SERIAL PRIMARY KEY,
        table_name VARCHAR(255),
        operation_type VARCHAR(50),
        operation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        operation_details TEXT
    );
''')
conn2.commit()

# Menyisipkan data dummy
dummy_data = [
    ("Alice", 30, "Female"),
    ("Bob", 25, "Male"),
    ("Charlie", 35, "Male")
]

cursor.executemany('''
    INSERT INTO employee (name, age, gender) VALUES (%s, %s, %s)
''', dummy_data)
conn.commit()

# Cleanup
cursor.close()
conn.close()
