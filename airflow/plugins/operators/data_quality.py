from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self , redshift_conn_id , table_list, *args, **kwargs):
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list
        super(DataQualityOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info('Starting Data Quality Check...')
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        for table in self.table_list:
            records = self.hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")