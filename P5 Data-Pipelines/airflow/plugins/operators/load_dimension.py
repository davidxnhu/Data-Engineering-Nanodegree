from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.truncated = mode

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
              
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        

        if self.truncated:
            self.log.info('Start deleting data on Table {} Redshift'.format(self.table))
            redshift.run("TRUNCATE TABLE {};".format(self.table))
            
        self.log.info('Load Dimension table from staging tables')
        redshift.run("INSERT INTO {} {}".format(self.table, self.sql_query))