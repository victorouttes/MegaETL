"""
Class for transfer data between some databases to S3 (json or parquet).
"""
from datetime import datetime
from io import BytesIO

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.oracle_hook import OracleHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from megaetl.hooks.mssql_hook import MsSqlServerHook
from megaetl.hooks.postgres_hook import PostgreSqlHook


class TransferDB2S3(BaseOperator):
    @apply_defaults
    def __init__(self,
                 source_db: str,
                 source_conn_id: str,
                 source_table: str,
                 dest_s3_conn_id: str,
                 dest_s3_bucket: str,
                 dest_s3_key: str,
                 filetype: str = 'json',
                 chunksize: int = None,
                 query: str = None,
                 *args, **kwargs):
        super(TransferDB2S3, self).__init__(*args, **kwargs)
        self.source_db = source_db
        self.source_conn_id = source_conn_id
        self.source_table = source_table
        self.dest_s3_conn_id = dest_s3_conn_id
        self.dest_s3_bucket = dest_s3_bucket
        self.dest_s3_key = dest_s3_key
        self.filetype = filetype
        self.chunksize = chunksize
        self.query = query

    def execute(self, context):
        if self.source_db.lower() not in ['oracle', 'postgres', 'mssql', 'mysql']:
            raise AirflowException(
                '"source_db" must be one of ["oracle", "postgres", "mssql", "mysql"]'
            )

        if self.filetype.lower() not in ['json', 'parquet']:
            raise AirflowException(
                '"filetype" must be one of ["json", "parquet"]'
            )

        if not self.query:
            self.query = 'select * from {}'.format(self.source_table)

        source = None
        if self.source_db.lower() == 'oracle':
            source = OracleHook(self.source_conn_id)
        elif self.source_db.lower() == 'postgres':
            source = PostgreSqlHook(self.source_conn_id)
        elif self.source_db.lower() == 'mssql':
            source = MsSqlServerHook(self.source_conn_id)
        elif self.source_db.lower() == 'mysql':
            source = MySqlHook(self.source_conn_id)

        dest = S3Hook(self.dest_s3_conn_id)
        today = datetime.today()
        conn = source.get_conn()

        if self.chunksize:
            # delete keys
            p_sp = self.dest_s3_key.split('/')
            prefix = '/'.join(p_sp[0:len(p_sp) - 1])
            keys_delete = dest.list_keys(bucket_name=self.dest_s3_bucket, prefix=prefix)
            if keys_delete:
                dest.delete_objects(bucket=self.dest_s3_bucket, keys=keys_delete)

            chunks = pd.read_sql(self.query, con=conn, chunksize=self.chunksize)
            part = 1
            for chunk in chunks:
                chunk['extraction_date'] = today
                name = self.dest_s3_key + str(part)

                if self.filetype.lower() == 'json':
                    json_str = chunk.to_json(path_or_buf=None, orient='records', date_format='iso')
                    # Load Into S3
                    dest.load_string(
                        string_data=json_str,
                        key=name + '.json',
                        bucket_name=self.dest_s3_bucket,
                        replace=True
                    )
                elif self.filetype.lower() == 'parquet':
                    out_buffer = BytesIO()
                    chunk.to_parquet(
                        out_buffer,
                        compression='snappy',
                        engine='pyarrow',
                        allow_truncated_timestamps=True,
                        use_deprecated_int96_timestamps=True
                    )
                    # Load Into S3
                    dest.load_bytes(
                        bytes_data=out_buffer.read(),
                        key=name + '.parquet',
                        bucket_name=self.dest_s3_bucket,
                        replace=True
                    )
        else:
            df = pd.read_sql(self.query, con=conn)
            df['extraction_date'] = today
            if self.filetype.lower() == 'json':
                json_str = df.to_json(path_or_buf=None, orient='records', date_format='iso')
                # Load Into S3
                dest.load_string(
                    string_data=json_str,
                    key=self.dest_s3_key + '.json',
                    bucket_name=self.dest_s3_bucket,
                    replace=True
                )
            elif self.filetype.lower() == 'parquet':
                out_buffer = BytesIO()
                df.to_parquet(
                    out_buffer,
                    compression='snappy',
                    engine='pyarrow',
                    allow_truncated_timestamps=True,
                    use_deprecated_int96_timestamps=True
                )
                # Load Into S3
                dest.load_bytes(
                    bytes_data=out_buffer.read(),
                    key=self.dest_s3_key + '.parquet',
                    bucket_name=self.dest_s3_bucket,
                    replace=True
                )
