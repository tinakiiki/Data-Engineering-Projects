from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,redshift_conn_id,destination_table,sql_insert_query,append,*args,**kwargs):
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_insert_query = sql_insert_query
        self.append = append

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info('Starting Stage To Redshift load....')
        #connect to Redshift
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        self.log.info("Connection made using " + self.redshift_conn_id)
        #define delete statement
        delete_records_statement = "DELETE FROM {}".format(self.destination_table)
        #define insert statement
        insert_table_statement = "INSERT INTO {}{};".format(self.destination_table, self.sql_insert_query)
        #execute delete and insert
        if self.append == 'False':
            cursor.execute(delete_records_statement)
        cursor.execute(insert_table_statement)
        cursor.close()
        conn.commit()
        self.log.info("Load fact table complete!")