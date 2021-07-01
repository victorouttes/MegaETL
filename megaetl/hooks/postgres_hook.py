import numpy as np
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extensions import register_adapter, AsIs


# ADAPTERS for compatibility between psycopg2 and pandas
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


def addapt_numpy_float32(numpy_float32):
    return AsIs(numpy_float32)


def addapt_numpy_int32(numpy_int32):
    return AsIs(numpy_int32)


def addapt_numpy_array(numpy_array):
    return AsIs(tuple(numpy_array))


register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)
register_adapter(np.float32, addapt_numpy_float32)
register_adapter(np.int32, addapt_numpy_int32)
register_adapter(np.ndarray, addapt_numpy_array)


class PostgreSqlHook(PostgresHook):
    def insert_pandas_df(self, df: pd.DataFrame, table: str, if_exists='replace', primary_key=None):
        """
        Insert a pandas dataframe in database table.
        :param df: Pandas dataframe
        :param table: Postgres table name
        :param if_exists: "replace" or "append", same as dataframe.to_sql argument
        :param primary_key: Name of primary key to be created at data table, optional
        :return:
        """
        df = df.replace(r'^\s*$', np.NAN, regex=True)
        engine = self.get_sqlalchemy_engine()

        if primary_key and if_exists == 'replace':
            with engine.connect() as conn:
                conn.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')

        df.to_sql(
            table,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=5000
        )

        if primary_key and if_exists == 'replace':
            with engine.connect() as conn:
                conn.execute(f'ALTER TABLE "{table}" ADD PRIMARY KEY ("{primary_key}")')


