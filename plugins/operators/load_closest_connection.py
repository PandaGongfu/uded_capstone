from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook

class LoadClosestConnectionOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 s3_conn_id="", 
                 table="",     
                 temp_table="",   
                 data_path="",    
                 *args, **kwargs):

        super(LoadClosestConnectionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.temp_table = temp_table
        self.redshift_conn_id = redshift_conn_id
        self.s3_conn_id=s3_conn_id   
        self.data_path=data_path   
        

    def execute(self, context):
        # get aws credentials for accesing s3 bucket
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        credentials = s3_hook.get_credentials()

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # create temp table
        create_temp_query =  f("""
        DROP TABLE IF EXISTS graph_recs.{self.temp_table};
        CREATE TABLE graph_recs.{self.temp_table} (
        user_id             INT    NOT NULL,
        closest_connection  INT    NOT NULL,
        weight              Decimal     NOT NULL
        );
        """)  
        redshift_hook.run(create_temp_query)   
        self.log.info("create temp table complete.")    

        # copy closest connection into temp table
        data_copy = (f"""
                        copy graph_recs.{self.temp_table}
                        from '{data_path}'
                        ACCESS_KEY_ID '{credentials.access_key}'
                        SECRET_ACCESS_KEY '{credentials.secret_key}'          
                        csv;
                     """)        
        
        redshift_hook.run(data_copy)
        self.log.info("copy data from s3 to temp table complete.") 

        # Upsert: if closest connection is already in db, only update weight and updated_at
        update_query = f"""
            INSERT INTO graph_recs.{self.table} (user_id, closest_connection, weight, created_at, updated_at)
            SELECT t.user_id, t.closest_connection, t.weight, current_timestamp, current_timestamp FROM graph_recs.tt t
            ON CONFLICT (user_id, closest_connection) 
            DO UPDATE SET weight = EXCLUDED.weight, updated_at = EXCLUDED.updated_at;
        """          
        redshift_hook.run(update_query)   
        self.log.info(f"update {self.table} complete.") 

        # Drop temp table
        drop_query = f"DROP TABLE IF EXISTS graph_recs.{self.temp_table};"
        redshift_hook.run(drop_query)   
        self.log.info(f"drop {self.temp_table} complete.") 

        # Remove data where updated_at is more than 24 hours
        delete_query = f"DELETE FROM graph_recs.{self.table} WHERE updated_at < now()- interval \'1 day\'" 
        redshift_hook.run(delete_query)   
        self.log.info(f"delete expired data complete.")         