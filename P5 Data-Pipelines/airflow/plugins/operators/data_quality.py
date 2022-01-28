from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 check_column="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.check_column=check_column

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        
        
        # Check if table has rows

        redshift_hook = PostgresHook(self.redshift_conn_id)

        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table} ")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
        
        records = redshift_hook.get_records(f"SELECT COUNT({self.check_column}) FROM {self.table} WHERE {self.check_column} IS NULL")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records >= 1:
            raise ValueError(f"Data quality check failed. {self.table} contained {records[0][0]} NULL values in column {self.check_column}")
        self.log.info(f"Data quality on table {self.table} check passed with zero NULL records in column {self.check_column}")       
        
