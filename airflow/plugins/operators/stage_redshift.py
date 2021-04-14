from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
#define parameters to be used for the s3 to redshift copy statement
    @apply_defaults
    def __init__(self , redshift_conn_id , table , s3_bucket, s3_path , s3_access_key_id , s3_secret_access_key, region , sql_create_statement ,*args, **kwargs):
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_access_key = s3_secret_access_key
        self.region = region
        self.sql_create_statement = sql_create_statement
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
#execute the create table and copy statement
    def execute(self, context):
        self.log.info('Starting Stage To Redshift load....')
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        conn = self.hook.get_conn()
        cursor = conn.cursor()

        log.info("Connection made using " + self.redshift_conn_id)
        drop_table_statement = "DROP TABLE IF EXISTS {0};".format(self.table)
        copy_statement = """
        COPY {0}
        FROM 's3://{1}/{2}'
        access_key_id '{3}' secret_access_key '{4}'
        region '{5}' json 'auto' """.format(
        self.table, self.s3_bucket, self.s3_path,
        self.s3_access_key_id, self.s3_secret_access_key,
        self.region)
        
        
        cursor.execute(drop_table_statement)
        cursor.execute(self.sql_create_statement)
        cursor.execute(copy_statement)
        cursor.close()
        conn.commit()
        log.info("S3 to Redshift load completed")





