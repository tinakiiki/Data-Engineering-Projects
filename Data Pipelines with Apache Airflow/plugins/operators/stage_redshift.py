from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self, redshift_conn_id , table , s3_bucket, s3_key ,aws_conn_id, region ,*args,**kwargs):
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_id = aws_conn_id 
        self.region = region
        

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info('Starting Stage To Redshift load....')
        #connect to redshift 
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        self.log.info("Connection made using " + self.redshift_conn_id)
        execution_date = context["execution_date"]
        ds = context["ds"]
        if self.s3_key=="log_data":
            self.s3_key = f"log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json"
        #get aws key and secret
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        #define sql statements
        copy_statement = """
        COPY {}
        FROM 's3://{}/{}'
        access_key_id '{}' secret_access_key '{}'
        region '{}' json 'auto' """.format(
        self.table, self.s3_bucket, self.s3_key,
        credentials.access_key, credentials.secret_key,
        self.region)
        #execute copy from s3 to redshift
        cursor.execute(copy_statement)
        cursor.close()
        conn.commit()
        self.log.info("S3 to Redshift load completed")




