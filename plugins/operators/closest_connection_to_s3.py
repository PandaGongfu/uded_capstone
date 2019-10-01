from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import sys
sys.path.append('../')

from helpers import ClosestConnectionLogic

FILE_CLOSEST_CONNECTIONS = 'closest_connections.csv'
FILE_CLOSEST_CONNECTIONS_DATA = 'closest_connections_data.csv'

class ClosestConnectionOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_conn_id="",      
                 *args, **kwargs):

        super(ClosestConnectionOperator, self).__init__(*args, **kwargs)
        self.s3_conn_id=s3_conn_id      
        

    def execute(self, context):
        # get aws credentials for accesing s3 bucket
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        credentials = s3_hook.get_credentials()
        
        # calculate closest connection and upload to s3
        clc_logic = ClosestConnectionLogic()
        closest_connections_raw, closest_connections = clc_logic.get_closest_connections(credentials)
        clc_logic.write_to_s3(closest_connections_raw, FILE_CLOSEST_CONNECTIONS)
        clc_logic.write_to_s3(closest_connections, FILE_CLOSEST_CONNECTIONS_DATA)

