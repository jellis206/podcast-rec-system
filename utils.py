import pandas as pd
import io
import psycopg

from typing import List

def fast_pg_insert(df: pd.DataFrame, connection: str, table_name: str, columns: List[str]) -> None:
    """
        Inserts data from a pandas DataFrame into a PostgreSQL table using the COPY command for fast insertion.

        Parameters:
        df (pd.DataFrame): The DataFrame containing the data to be inserted.
        connection (str): The connection string to the PostgreSQL database.
        table_name (str): The name of the target table in the PostgreSQL database.
        columns (List[str]): A list of column names in the target table that correspond to the DataFrame columns.

        Returns:
        None
    """
    with psycopg.connect(connection) as conn:
        with conn.cursor() as cur:
            _buffer = io.StringIO()
            df.to_csv(_buffer, sep=";", index=False, header=False)
            _buffer.seek(0)

            # psycopg3 uses copy() method with a different syntax
            columns_str = ', '.join(columns)
            with cur.copy(f"COPY {table_name} ({columns_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER ';', NULL '')") as copy:
                while True:
                    data = _buffer.read(8192)
                    if not data:
                        break
                    copy.write(data)
        conn.commit()