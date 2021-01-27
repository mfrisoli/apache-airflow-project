from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    stage_sql_template = """
    COPY {}
    FROM '{}'
    CREDENDIALS 'aws_iam_role={}'
    json '{}'
    REGION '{}'
    TIMEFORMAT as 'epochmillisecs'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 json="",
                 region="",
                 aws_iam_role="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.aws_credentials = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_hook = AwsHook,
        self.json = json,
        self.region = region,
        self.aws_iam_role = aws_iam_role

    def execute(self, context):
        
        self.log.info('StageToRedshiftOperator not implemented yet')
        # Hooks
        aws_hook = AwsHook(self.aws_credentials)
        redshift = PostgresHook(self.redshift_conn_id)
        
        # Bucket render i.e. udacity-dend/log_data
        s3_render_key = self.s3_key.format(**context)
        
        bucket = "s3://{}/{}".format(self.s3_bucket, s3_render_key)
        
        # SQL Statement Format
        copy_sql = StageToRedshiftOperator.stage_sql_template.format(
            self.table,
            bucket,
            self.aws_iam_role,
            self.json,
            self.region
        )
        
        redshift.run(copy_sql)
        self.log.info('StageToRedshiftOperator SUCCESS')
        





