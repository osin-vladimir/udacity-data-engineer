from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_checks = data_checks

    def execute(self, context):
        """
        Performs data quality checks
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for data_check in self.data_checks:
            records = redshift_hook.get_records(data_check["sql"])
            num_records = records[0][0]

            assert (num_records != data_check["expected_result"]) , \
            f"Data quality test returned unexpected result."

        self.log.info('DataQualityOperator not implemented yet')
