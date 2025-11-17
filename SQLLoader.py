class SQLLoader:
    def __init__(self, sqlserver):
        self.sqlserver = sqlserver

    def load_dataframe(self, df, table_name, if_exists="append"):
        cursor = self.sqlserver.cursor

        if if_exists == "replace":
            cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")
            self.sqlserver.conn.commit()

        columns = ", ".join([f"[{c}] NVARCHAR(MAX)" for c in df.columns])
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NULL "
            f"CREATE TABLE {table_name} ({columns});"
        )
        self.sqlserver.conn.commit()

        for _, row in df.iterrows():
            placeholders = ", ".join(["?"] * len(row))
            sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.execute(sql, tuple(row.values))

        self.sqlserver.conn.commit()

        print(f"{len(df)} linhas inseridas em {table_name}.")
