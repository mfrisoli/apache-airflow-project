from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    sql_insert = """
        INSERT INTO {} ({}) 
        {}
    
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 columns="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.columns = columns

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(self.redshift_conn_id)
        
        # Render Complete SQL query
        sql_insert_render = LoadDimensionOperator.sql_insert.format(
            self.table,
            self.columns,
            self.sql_query
        )
        
        redshift.run(sql_insert_render)
        
        self.log.info('LoadDimensionOperator SUCCESS!')
