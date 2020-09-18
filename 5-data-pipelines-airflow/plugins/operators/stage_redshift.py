from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_path",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table_name="",
                 s3_path="",
                 region="",
                 json_format="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.s3_path = s3_path
        self.region= region
        self.json_format = json_format
        self.aws_credentials_id = aws_credentials_id
        self.execution_date = kwargs.get('execution_date')
        self.kwargs = kwargs

    def execute(self, context):
        """
        Performs copy procedure from S3 to AWS Redshift.
        """

        # initialize aws hook to get user credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        # constructing query for aws redshift
        query = f"""copy {self.table_name} from '{self.s3_path}'
                    access_key_id '{credentials.access_key}'
                    secret_access_key '{credentials.secret_key}'
                    compupdate off region '{self.region}'
                    timeformat as 'epochmillisecs'
                    truncatecolumns blanksasnull emptyasnull
                    format as json '{self.json_format}';
                 """

        # executing aws redshift query
        self.log.info(f'Staging table {self.table_name} to AWS Redshift...')
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        redshift_hook.run(query)
        self.log.info(f"Table {self.table_name} staged successfully!")









