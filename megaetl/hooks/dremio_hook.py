import sys

import sqlalchemy
from airflow.hooks.jdbc_hook import JdbcHook


class DremioHook(JdbcHook):
    def get_conn(self):
        """
        Get Dremio connection.
        :return: connection
        """
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        host_w_port = conn.host
        login = conn.login
        psw = conn.password
        uri = 'dremio://{}:{}@{}/dremio;SSL=0'.format(login, psw, host_w_port)
        conn = sqlalchemy.create_engine(uri)
        return conn

    def get_pandas_df(self, sql, parameters=None, **kwargs):
        """
        Run query on Dremio instance and return as pandas dataframe.
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
        super(DremioHook, self).bulk_dump(table, tmp_file)

    def bulk_load(self, table, tmp_file):
        super(DremioHook, self).bulk_load(table, tmp_file)
