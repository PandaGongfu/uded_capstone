import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (ClosestConnectionOperator, LoadClosestConnectionOperator, ReputationScoreOperator, 
                               UserRecOperator, LoadUserRecOperator, LoadUserRecReasonOperator, DataQualityOperator)

LAST_DATE = datetime.now().date().strftime("%Y%m%d")
S3_BUCKET = 'ds-data-store'
S3_PATH = 'graph_data/' + LAST_DATE
FILE_CLOSEST_CONNECTIONS_DATA = 'closest_connections_data.csv'
FILE_REC = 'user_rec.csv'
FILE_REC_REASON = 'user_rec_reasons.csv'

default_args = {
    'owner': 'yw',
    'start_date': datetime(2019, 9, 26),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False,   
}

dag = DAG('user_rec_pipeline_dag',
          default_args=default_args,
          description='User Recommendation Pipeline with Airflow',
          schedule_interval='0 8 * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

find_closest_connections = ClosestConnectionOperator(
    task_id='find_closest_connections',
    dag=dag,
    s3_conn_id="s3_conn",
)

load_closest_connections = LoadClosestConnectionOperator(
    task_id='load_closest_connections',
    dag=dag,
    table="closest_connections",
    temp_table="tt",
    redshift_conn_id="redshift",
    s3_conn_id="s3_conn",   
    data_path=os.path.join('s3://',S3_BUCKET, S3_PATH, FILE_CLOSEST_CONNECTIONS_DATA)
)

calc_reputation_scores = ReputationScoreOperator(
    task_id='calc_reputation_scores',
    dag=dag,   
)

find_user_rec = UserRecOperator(
    task_id='find_user_rec',
    dag=dag,
    s3_conn_id="s3_conn",   
)

load_user_rec = LoadUserRecOperator(
    task_id='load_user_rec',
    dag=dag,
    table="recommendations",
    temp_table="tt_user_rec",
    redshift_conn_id="redshift",
    s3_conn_id="s3_conn",   
    data_path=os.path.join('s3://',S3_BUCKET, S3_PATH, FILE_REC) 
)

load_user_rec_reason = LoadUserRecReasonOperator(
    task_id='load_user_rec_reason',
    dag=dag,
    table="rec_reasons",
    temp_table="tt_user_rec_reason",
    redshift_conn_id="redshift",
    s3_conn_id="s3_conn",  
    data_path=os.path.join('s3://',S3_BUCKET, S3_PATH, FILE_REC_REASON)      
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    redshift_conn_id='redshift',
    test_cases=[
        # no null data
        ("SELECT COUNT(user_id) from graph_recs.closest_connections WHERE user_id IS NULL OR rec_id IS NULL", 0),
        ("SELECT COUNT(user_id) from graph_recs.recommendations WHERE user_id IS NULL OR rec_id IS NULL", 0),
        ("SELECT COUNT(user_id) from graph_recs.user_rec_reasons WHERE user_id IS NULL OR rec_id IS NULL", 0),

        # no expired results
        ("SELECT COUNT(user_id) from graph_recs.closest_connections WHERE updated_at < now()- interval \'1 day\'", 0),       
        ("SELECT COUNT(user_id) from graph_recs.user_rec WHERE updated_at < now()- interval \'1 day\'", 0),   
        ("SELECT COUNT(user_id) from graph_recs.user_rec_reasons WHERE updated_at < now()- interval \'1 day\'", 0),              
    ],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Define dependencies
start_operator >> find_closest_connections
start_operator >> calc_reputation_scores
find_closest_connections >> load_closest_connections

load_closest_connections >> find_user_rec
calc_reputation_scores >> find_user_rec
find_user_rec >> load_user_rec
find_user_rec >> load_user_rec_reason

load_closest_connections >> run_quality_checks
load_user_rec >> run_quality_checks
load_user_rec_reason >> run_quality_checks

run_quality_checks >> end_operator