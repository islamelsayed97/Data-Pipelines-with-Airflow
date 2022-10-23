from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        dq_checks = [
            {'test_sql': f'SELECT COUNT(*) FROM {table}', 'Unexpected_result': 0, 'explanation_error' : ' contained 0 rows'},
            {'check_sql': f"SELECT COUNT(*) FROM {t} WHERE {t_id} is null", 'expected_result': 0, 'explanation_error' : f' {t_id} contained null values'}
                    ]
        tables_id = {'songplays':'playid', 'users':'userid', 'songs':'songid', 'artists':'artistid', 'time':'start_time'}
        
        for table in self.tables:
            self.log.info(f"Checking Data quality on {table} table...")
            self.log.info(f"Checking on number of rows...")
            records = redshift.get_records(dq_checks[0]['test_sql'].format(table))
            
            if len(records) == dq_checks[0]['Unexpected_result'] or len(records[0]) == dq_checks[0]['Unexpected_result']:
                raise ValueError(f"Data quality check failed. {table} returned no results")
        
            elif records[0][0] == dq_checks[0]['Unexpected_result']:
                raise ValueError(f"Data quality check failed. {table} {dq_checks[0]['explanation_error']}")
            else:    
                self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
                
            self.log.info(f"Checking on null values...")
            null_records = redshift.get_records(dq_checks[1]['test_sql'].format(table, tables_id[table]))
            if null_records[0][0] == dq_checks[1]['expected_result']:
                self.log.info(f"Data quality on table {table} check passed with no null values in {tables_id[table]}")
            else:
                raise ValueError(f"Data quality check failed. {dq_checks[1]['explanation_error']}")
        
        
        