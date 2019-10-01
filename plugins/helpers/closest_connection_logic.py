import os
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
import boto3
import logging
from io import StringIO

##### Define Task Config #####

log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
LOGGER = logging.getLogger(__name__)

LAST_DATE = dt.datetime.now().date().strftime("%Y%m%d")
CURRENT_TIME = dt.datetime.now().strftime('%s')

S3_BUCKET = 'ds-data-store'
S3_PATH = 'graph_data/' + LAST_DATE

N_CLOSEST = 50
LOOKBACK_DAYS = 90

FILE_FOLLOW = 'follow.csv'
FILE_REPLY = 'reply.csv'
FILE_MENTION = 'mention.csv'
FILE_LIKE = 'like.csv'
FILE_USER = 'user.csv'

def agg_closest(x):
    return ','.join(x.head(N_CLOSEST).values)

def agg_weight(x):
    return ','.join(['{:.8f}'.format(w) for w in x.head(N_CLOSEST).values])


def get_decay_weight(x):
    # use 90-day windown for network engagements
    year = int(x[:4])
    month = int(x[5:7])
    day = int(x[8:10])

    hour = int(x[11:13])
    minute = int(x[14:16])
    second = int(x[17:19])

    return max(0, 1 - (int(CURRENT_TIME) - int(dt.datetime(year, month, day, hour, minute, second).strftime('%s'))) / 60 / 60 / 24 / LOOKBACK_DAYS)


class ClosestConnectionLogic():
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

    def get_closest_connections(self, credentials):
        # limit follow within the LOOKBACK_DAYS window
        follow = pd.read_csv(self.read_from_s3(FILE_FOLLOW, credentials)['Body'], header=None, names=['user_id', 'engaged_with_user_id', 'timestamp'])
        start_date=  (dt.datetime.now() - relativedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
        follow['flag']=follow['timestamp'].apply(lambda x: int(''.join(x[:10].split('-')))>=int(start_date))
        follow_recent = follow[follow['flag']].copy()
        follow_recent.drop('flag', axis=1, inplace=True)

        reply = pd.read_csv(self.read_from_s3(FILE_REPLY, credentials)['Body'], header=None,
                            names=['user_id', 'engaged_with_user_id', 'message_id', 'timestamp','rel_type'], usecols=[0, 1, 3])
        mention = pd.read_csv(self.read_from_s3(FILE_MENTION, credentials)['Body'], header=None,
                                names=['user_id', 'engaged_with_user_id', 'message_id', 'timestamp', 'rel_type'], usecols=[0, 1, 3])
        like = pd.read_csv(self.read_from_s3(FILE_LIKE, credentials)['Body'], header=None, names=['user_id', 'engaged_with_user_id', 'message_id', 'timestamp', 'rel_type'],
                            usecols=[0, 1, 3])
        user = pd.read_csv(self.read_from_s3(FILE_USER, credentials)['Body'], header=None, usecols=[0, 11], names=['user_id', 'suspended'], dtype={'user_id':str})

        user_engagements = pd.concat([reply, mention, like, follow_recent], axis=0)
        user_engagements['user_id'] = user_engagements['user_id'].astype(str)
        user_engagements['engaged_with_user_id'] = user_engagements['engaged_with_user_id'].astype(str) 
        LOGGER.info(f'user engagements: {user_engagements.shape[0]}')

        # Remove suspended users
        suspend = user[user['suspended'].isin([1])].copy()
        tmp = pd.merge(user_engagements, suspend[['user_id']], left_on='engaged_with_user_id', right_on='user_id', how='left', indicator=True, suffixes=("", "_suspend"))  
        user_engagements_valid = tmp[tmp['_merge'] == 'left_only'].copy()
        user_engagements_valid.drop(['_merge', 'user_id_suspend'], axis=1, inplace=True)
        LOGGER.info(f'user engagements after removing suspended users: {user_engagements_valid.shape[0]}')

        user_engagements_valid['weight'] = user_engagements_valid['timestamp'].apply(lambda x: get_decay_weight(x))
        user_engagements_valid = user_engagements_valid[user_engagements_valid['weight'] > 0].copy()
        engagements_count = user_engagements_valid[['user_id', 'engaged_with_user_id', 'weight']].groupby(['user_id', 'engaged_with_user_id']).agg({'weight': sum}).reset_index()
        engagements_count.sort_values(['user_id', 'weight'], ascending=[True, False], inplace=True)
        closest_connections_raw = engagements_count.groupby('user_id').agg({'engaged_with_user_id': agg_closest, 'weight': agg_weight}).reset_index()

        closest_connections = engagements_count.groupby('user_id').head(N_CLOSEST).reset_index(drop=True)
        LOGGER.info(f'closest connections: {closest_connections.shape[0]}')       
        return closest_connections_raw, closest_connections

