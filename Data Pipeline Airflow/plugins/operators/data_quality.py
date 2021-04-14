from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,redshift_conn_id,tables, *args, **kwargs):
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        super(DataQualityOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info('Starting data quality checks')
        #connect to Redshift
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        self.log.info("Connection made using " + self.redshift_conn_id)
        #run checks sql
        for table in self.tables:
            checks_sql = "SELECT count(*) FROM {}".format(table)
            records = self.hook.get_records(checks_sql)
            if len(records)<1 or len(records[0])<1 or records[0][0]<1:
                self.log.error(f"Data quality checks failed for {table}")
            self.log.info("Data checks successfully completed!")
