from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#FAD7A0'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 test_cases,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for (test_query, expected_result)  in self.test_cases:
            self.log.info(f'Running Data Quality Check: {test_query}')
            records = redshift.get_records(test_query)

            if len(records) < 1 or len(records[0]) < 1:
                self.log.info(f"Data quality check failed. {test_query} returned no results")
                raise ValueError(f"Data quality check failed. {test_query} returned no results")

            num_records = records[0][0]
            if num_records != expected_result:
                self.log.info(f"Data quality check failed. {test_query} contained {num_records} rows but expected {expected_result}")
                raise ValueError(f"Data quality check failed. {test_query} contained {num_records} rows but expected {expected_result}")
