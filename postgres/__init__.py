import pandas as pd
from sqlalchemy import create_engine
from config import Config

class DB_postgres:
    conn_string = f"""postgresql://{Config.POSTGRES_user}:{Config.POSTGRES_password}@{Config.POSTGRES_host}:{Config.POSTGRES_port}/{Config.POSTGRES_dbname}"""

    @staticmethod
    def postgres_append_table(df, table):
        db = create_engine(DB_postgres.conn_string)
        with db.connect() as conn:
            df.to_sql(table, con=conn, schema=Config.POSTGRES_schema, if_exists='append', index=False)
            conn.commit()
        db.dispose()