import psycopg2

CONFIG = {
    "dbname":"employee_db", 
    "user":"postgres", 
    "password":"postgres", 
    "host":"localhost"
}

CONFIG_PRIMARY = CONFIG
CONFIG_SECONDARY = {
    "dbname":"employee_secondary_db", 
    "user":"postgres", 
    "password":"postgres", 
    "host":"localhost"
}

def connect_db(database_url=None, name="CONFIG"):
    if database_url:
        return psycopg2.connect(database_url)
    elif name == "CONFIG":
        return psycopg2.connect(
            dbname=CONFIG['dbname'],
            user=CONFIG['user'],
            password=CONFIG['password'],
            host=CONFIG['host'],
        )
    elif name == "CONFIG_SECONDARY":
        return psycopg2.connect(
            dbname=CONFIG_SECONDARY['dbname'],
            user=CONFIG_SECONDARY['user'],
            password=CONFIG_SECONDARY['password'],
            host=CONFIG_SECONDARY['host'],
        )
    else:
        raise Exception("!!! Invalid Database")