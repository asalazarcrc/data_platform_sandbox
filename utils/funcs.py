import os 
import pyodbc

def blackswan_sql_conn(database):
    database = database
    server = os.getenv("BLACKSWAN_DB_HOST")
    user = os.getenv("BLACKSWAN_DB_USER")
    password = os.getenv("BLACKSWAN_DB_PW")

    return pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+user+';PWD='+ password)
