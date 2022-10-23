from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_query='',
                 table='',
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        if self.truncate:
            self.log.info(f'Truncating {self.table} table before inserting the new data...')
            redshift.run(f'TRUNCATE TABLE {self.table}')
            
        self.log.info(f'Inserting data to {self.table} table...')
        redshift.run(LoadDimensionOperator.insert_sql.format(self.table, self.sql_query))
        self.log.info(f"Success: {self.task_id}")

