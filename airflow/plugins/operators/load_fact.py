from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self , redshift_conn_id , destination_table , sql_insert_query , sql_create_query , column_names , primary_key , *args, **kwargs):
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_insert_query = sql_insert_query 
        self.sql_create_query = sql_create_query
        self.column_names = column_names
        self.primary_key = primary_key

        super(LoadFactOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info('Starting Stage To Redshift load....')
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        conn = self.hook.get_conn()
        cursor = conn.cursor()

        log.info("Connection made using " + self.redshift_conn_id)
        insert_table_statement = "INSERT INTO {0}({1}) ON CONFLICT ON CONSTRAINT {3} DO NOTHING ;".format(self.destination_table, self.column_names,self.primary_key)

        cursor.execute(drop_table_statement) 
        cursor.execute(insert_table_statement)
        cursor.close()
        conn.commit()
        log.info("Create table and load data complete!")
