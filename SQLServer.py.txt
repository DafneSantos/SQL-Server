import pyodbc
import pandas as pd

class SQLServer:
    def __init__(self, server, database, trusted=True, user=None, password=None):
        if trusted:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"Trusted_Connection=yes;"
            )
        else:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={user};"
                f"PWD={password};"
            )

        self.conn = pyodbc.connect(conn_str)
        self.cursor = self.conn.cursor()

    def query(self, sql):
        df = pd.read_sql(sql, self.conn)
        return df

    def execute(self, sql):
        self.cursor.execute(sql)
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()
