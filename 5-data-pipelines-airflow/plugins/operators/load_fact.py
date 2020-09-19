from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query = "",
                 mode = "",
                 table_name = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table_name = table_name
        self.mode = mode

    def execute(self, context):
        """
        Inserts data into fact tables from staging tables.
        Support append or delete insertion modes.
        """
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if self.mode == "truncate-insert":
            redshift_hook.run(f"truncate table {self.table_name}")

            self.log.info(f'Loading data into dimensional table {self.table_name} ...')
            redshift_hook.run(self.sql_query)
            self.log.info(f'Loading data into dimensional table {self.table_name} complete')

        elif self.mode == "append":
            self.log.info(f'Appending data into dimensional table {self.table_name} ...')
            redshift_hook.run(self.sql_query)
            self.log.info(f'Appending operation for dimensional table {self.table_name} completed')
