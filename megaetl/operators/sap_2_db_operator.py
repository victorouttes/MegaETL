"""
Class for transfer data between some databases to S3 (json or parquet).
"""
from datetime import datetime
from typing import List

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from megaetl.hooks.postgres_hook import PostgreSqlHook
from megaetl.hooks.sap_hook import SAPHook


class TransferSAP2DB(BaseOperator):
    @apply_defaults
    def __init__(self,
                 sap_conn_id: str,
                 sap_table: str,
                 sap_columns: List[str],
                 dest_db: str,
                 dest_conn_id: str,
                 dest_table: str,
                 sap_where: str = None,
                 sap_page_size: int = 1000,
                 if_exists: str = 'update',
                 dest_primary_key: str = None,
                 partial_commit: bool = True,
                 *args, **kwargs):
        super(TransferSAP2DB, self).__init__(*args, **kwargs)
        self.sap_conn_id = sap_conn_id
        self.sap_table = sap_table
        self.sap_columns = sap_columns
        self.sap_where = sap_where
        self.sap_page_size = sap_page_size
        self.dest_db = dest_db
        self.dest_conn_id = dest_conn_id
        self.dest_table = dest_table
        self.dest_primary_key = dest_primary_key
        self.partial_commit = partial_commit
        self.if_exists = if_exists

    def execute(self, context):
        if self.dest_db.lower() not in ['postgres']:
            raise AirflowException(
                '"dest_db" must be one of ["postgres"]'
            )

        source = SAPHook(self.sap_conn_id)
        df = source.get_sap_pandas_df(
            table=self.sap_table,
            columns=self.sap_columns,
            where=self.sap_where,
            page_size=self.sap_page_size
        )

        dest = None
        if self.dest_db.lower() == 'postgres':
            dest = PostgreSqlHook(self.dest_conn_id)

        df['extraction_date'] = datetime.today()
        dest.insert_pandas_df(
            df=df,
            table=self.dest_table,
            if_exists=self.if_exists,
            primary_key=self.dest_primary_key
        )
