import os
import pandas as pd
import numpy as np
import datetime as dt
from itertools import chain
import boto3
import logging
from time import time
from io import StringIO

##### Define Task Config #####

log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
LOGGER = logging.getLogger(__name__)

LAST_DATE = dt.datetime.now().date().strftime("%Y%m%d")

S3_BUCKET = 'ds-data-store'
S3_PATH = 'graph_data/' + LAST_DATE
N_REC = 20

FILE_CLOSEST_CONNECTIONS = 'closest_connections.csv'
FILE_FOLLOW = 'follow.csv'
FILE_USER = 'user.csv'

def agg_top_closest_connections(x):
    return ','.join(x.head(N_REC).values)

def agg_top_values(x):
    return ';'.join(x.head(N_REC).values)

def agg_weight(x):
    return ','.join([f'{w:.8f}' for w in x.values])

def agg_closest(x):
    return ','.join(x.values)

class UserRecLogic():
    def __init__(self):
        pass

    def read_from_s3(self, filename, credentials):
        s3_client = boto3.client('s3', aws_access_key_id=credentials.access_key, aws_secret_access_key=credentials.secret_key)
        return s3_client.get_object(Bucket=S3_BUCKET, Key=S3_PATH + '/{:s}'.format(filename))

    def write_to_s3(self, content, filename):
        # define S3 connection in function call to avoid timeout
        csv_buffer = StringIO()
        content.to_csv(csv_buffer, index=False)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(S3_BUCKET, os.path.join(S3_PATH, filename)).put(Body=csv_buffer.getvalue())

    def get_user_rec(self, credentials):
        # Read input data - closest connections and follow
        closest_connections_raw = pd.read_csv(self.read_from_s3(FILE_CLOSEST_CONNECTIONS, credentials)['Body'], dtype={'user_id':str})
        LOGGER.info(f'closest_connections: {closest_connections_raw.shape[0]}')    
        follow = pd.read_csv(self.read_from_s3(FILE_FOLLOW, credentials)['Body'], header=None, names=['user_id', 'engaged_with_user_id', 'timestamp', 'rel_type'], dtype={'user_id':str, 'engaged_with_user_id':str})
        user = pd.read_csv(self.read_from_s3(FILE_USER, credentials)['Body'], header=None, usecols=[0, 11], names=['user_id', 'suspended'], dtype={'user_id':str})

        closest_connections = pd.merge(closest_connections_raw, user, on='user_id', how='left')
        closest_connections = closest_connections[~closest_connections['suspended'].isin([1])].copy()
        closest_connections['engaged_with_user_id'] = closest_connections['engaged_with_user_id'].apply(lambda x: x.split(','))
        closest_connections['weight'] = closest_connections['weight'].apply(lambda x: [float(w.strip()) for w in x.split(',')])

        # create dataframe that contains all second level engagements, i.e. the interactions from a user's closest connections to their closest connections.
        first_degree_connections = pd.DataFrame({
                "user_id": np.repeat(closest_connections['user_id'].values, closest_connections['engaged_with_user_id'].str.len()),
                "engaged_with_user_id": list(chain.from_iterable(closest_connections['engaged_with_user_id'])),
                "weight": list(chain.from_iterable(closest_connections['weight']))
            })
        LOGGER.info(f'first: {first_degree_connections.shape[0]}')
        
        start = time()
        second_degree_connections = first_degree_connections.merge(first_degree_connections, left_on="engaged_with_user_id", right_on="user_id", suffixes=("", "_sec_deg"))  
        second_degree_connections.drop(['weight', 'user_id_sec_deg'], inplace=True, axis=1)
        LOGGER.info(f'second: {second_degree_connections.shape[0]}')
        LOGGER.info(f'self join took: {time()-start:.2f}s')

        # remove recs that are already followed or the user himself
        start = time() 
        tmp = pd.merge(second_degree_connections, follow[['user_id', 'engaged_with_user_id']], left_on=['user_id', 'engaged_with_user_id_sec_deg'], right_on=['user_id', 'engaged_with_user_id'], how='left', indicator=True, suffixes=("", "_follow"))
        data = tmp[tmp['_merge'] == 'left_only'].copy()
        data.drop(['_merge', 'engaged_with_user_id_follow'], axis=1, inplace=True)
        data = data[data['user_id'] != data['engaged_with_user_id_sec_deg']].copy()
        LOGGER.info(f'remove followed took: {time()-start:.2f}s')
        
        # Aggregate by second level user engagements
        final = data.groupby(['user_id', 'engaged_with_user_id_sec_deg']).agg({'weight_sec_deg': [sum, agg_weight], 'engaged_with_user_id': agg_closest}).reset_index().rename(columns={'engaged_with_user_id': 'reasons_to_follow'})
        final.columns = [' '.join(col).strip() for col in final.columns.values]

        LOGGER.info(f'final: {final.shape[0]}')
        LOGGER.info(f'groupby took: {time()-start:.2f}s')

        start = time() 
        final.sort_values(['user_id', 'weight_sec_deg sum'], ascending=[True, False], inplace=True)
        LOGGER.info(f'sorting took: {time()-start:.2f}s')  

        final.rename(columns={'engaged_with_user_id_sec_deg':'user_rec', 'weight_sec_deg agg_weight': 'weights', 'reasons_to_follow agg_closest': 'reasons'}, inplace=True)  

        # get rec and rec_reasons as separate dataframes 
        rec = final.groupby('user_id').head(N_REC).reset_index(drop=True)
        rec['weights'] = rec['weights'].apply(lambda x: x.split(','))
        rec['reasons'] = rec['reasons'].apply(lambda x: x.split(','))

        rec_reasons = pd.DataFrame({
                "user_id": np.repeat(rec['user_id'].values, rec['reasons'].str.len()),
                "user_rec": np.repeat(rec['user_rec'].values, rec['reasons'].str.len()),
                "reason": list(chain.from_iterable(rec['reasons'])),
                "weight": list(chain.from_iterable(rec['weights']))
            })
        LOGGER.info(f'rec reasons: {rec_reasons.shape[0]}')    
        
        return rec, rec_reasons