from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import sys
sys.path.append('../')

from helpers import ReputationScoreLogic

FILE_SCORE = 'score.csv'

class ReputationScoreOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,    
                 *args, **kwargs):

        super(ReputationScoreOperator, self).__init__(*args, **kwargs)     
        

    def execute(self, context):
        # calc reputation scores
        score_logic = ReputationScoreLogic()
        user_scores = score_logic.get_reputation_score()
        score_logic.write_to_s3(user_scores, FILE_SCORE)