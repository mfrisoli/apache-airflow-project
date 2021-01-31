from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Operator to check that data exist in tables

    arg:
    - redshift_conn_id: Redshift Hook
    - tables: list of table names
    """


    ui_color = '#89DA59'
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        
        redshift = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            self.log.info('Checking table: {}'.format(table))
            data = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))
            
            if len(data) < 1:
                raise ValueError("Table {} is empty".format(table))
            
            if data[0][0] < 1:
                raise ValueError("Table {} has no rows".format(table))
            
            self.log.info('Data Quality SUCCESS for table: {}'.format(table))

        self.log.info('DATA QUALITY COMPLETE!')
                
                
                
            
            