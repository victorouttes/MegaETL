import sys

import sqlalchemy
from airflow.hooks.mssql_hook import MsSqlHook


class MsSqlServerHook(MsSqlHook):
    def get_uri(self):
        """
        Get connection URI
        :return: URI (string)
        """
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        login = ''
        if conn.login:
            login = '{conn.login}:{conn.password}@'.format(conn=conn)
        host = conn.host
        if conn.port is not None:
            host += ':{port}'.format(port=conn.port)
        uri = '{conn.conn_type}://{login}{host}/'.format(
            conn=conn, login=login, host=host)
        if conn.schema:
            uri += conn.schema
        return uri

    def get_conn(self):
        """
        Get the connection to SqlServer
        :return:
        """
        conn = super(MsSqlServerHook, self)
        conn_uri = conn.get_uri().replace('://', '+pymssql://')
        conn = sqlalchemy.create_engine(conn_uri).connect()
        return conn

    def get_engine(self):
        """
        Get SqlAlchemy engine
        :return:
        """
        conn = super(MsSqlServerHook, self)
        conn_uri = conn.get_uri().replace('://', '+pymssql://').replace('\\\\', '\\')
        engine = sqlalchemy.create_engine(conn_uri)
        return engine

    def get_pandas_df(self, sql, parameters=None, **kwargs):
        """
        Run query on SqlServer instance and return as pandas dataframe.
        :param sql: query to run
        :param parameters:
        :return:
        """
        if sys.version_info[0] < 3:
            sql = sql.encode('utf-8')
        import pandas.io.sql as psql
        conn = self.get_conn().connect()
        df = psql.read_sql(sql, con=conn, params=parameters, **kwargs)
        conn.close()
        return df

    def bulk_dump(self, table, tmp_file):
        super(MsSqlServerHook, self).bulk_dump(table, tmp_file)

    def bulk_load(self, table, tmp_file):
        super(MsSqlServerHook, self).bulk_load(table, tmp_file)
