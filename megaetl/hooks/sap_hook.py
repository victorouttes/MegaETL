from typing import List

from airflow.hooks.dbapi_hook import DbApiHook
from sap_rfc_data_collector.sap import SAP


class SAPHook(DbApiHook):
    """
    Create a UI connection from type JIRA.
    """
    conn_name_attr = 'sap_conn_id'
    default_conn_name = 'sap_default'

    def get_conn(self):
        """
        Get SAP connection.
        :return: connection
        """
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        login = conn.login
        psw = conn.password
        host = conn.extra_dejson.get('host', None)
        service = conn.extra_dejson.get('service', None)
        group = conn.extra_dejson.get('group', None)
        sysname = conn.extra_dejson.get('sysname', None)
        client = conn.extra_dejson.get('client', None)
        lang = conn.extra_dejson.get('lang', None)
        conn = SAP(
            host=host,
            service=service,
            group=group,
            sysname=sysname,
            client=client,
            lang=lang,
            user=login,
            password=psw,
        )
        return conn

    def get_sap_pandas_df(self, table: str, columns: List[str], where=None, page_size: int = 1000):
        df = self.get_conn().get_dataframe(
            table=table,
            columns=columns,
            where=where,
            page_size=page_size
        )
        return df

    def get_sap_pandas_df_by_page(self,
                                  table: str,
                                  columns: List[str],
                                  where=None,
                                  page_size: int = 1000,
                                  page: int = 1):
        df = self.get_conn().get_dataframe(
            table=table,
            columns=columns,
            where=where,
            page_size=page_size,
            page=page
        )
        return df

    def bulk_dump(self, table, tmp_file):
        super(SAPHook, self).bulk_dump(table, tmp_file)

    def bulk_load(self, table, tmp_file):
        super(SAPHook, self).bulk_load(table, tmp_file)
