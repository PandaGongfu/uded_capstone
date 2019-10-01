from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import sys
sys.path.append('../')

from helpers import UserRecLogic

FILE_REC = 'user_rec.csv'
FILE_REC_REASON = 'user_rec_reasons.csv'
FILE_SCORE = 'score.csv'

class UserRecOperator(BaseOperator):
    ui_color = '#F5B7B1'

    @apply_defaults
    def __init__(self,
                 s3_conn_id="",             
                 *args, **kwargs):

        super(UserRecOperator, self).__init__(*args, **kwargs)
        self.s3_conn_id=s3_conn_id      
        
    def execute(self, context):
        # get aws credentials for accesing s3 bucket
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        credentials = s3_hook.get_credentials()

        # find user recs 
        user_logic = UserRecLogic()
        rec, rec_reasons = user_logic.get_user_rec(credentials)

        # limit user recs to only those with scores over 0.75
        score=pd.read_csv(user_logic.read_from_s3(FILE_SCORE, credentials)['Body'])
        high_scores = score[score['pred']>0.75]
        rec = pd.merge(rec, high_scores, left_on='user_rec', right_on='user_id')
        rec_reasons = pd.merge(rec_reasons, high_scores, left_on='user_rec', right_on='user_id')

        # write results to s3
        rec_only = rec[['user_id','user_rec']].copy()   
        rec_reasons.drop('score', axis=1, inplace=True)     
        user_logic.write_to_s3(rec_only, FILE_REC)
        user_logic.write_to_s3(rec_reasons, FILE_REC_REASON)