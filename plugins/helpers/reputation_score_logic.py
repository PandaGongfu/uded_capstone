import numpy as np
import pandas as pd
import json
from collections import Counter
from datetime import datetime
from datetime import date
import re
import string
import os
from io import StringIO

from keras.models import model_from_json
MAX_FEATURES = 18900
MAXLEN = 1500

def read_file(f):
    twits = []
    for line in open(f, encoding="utf-8"):
        twits.append(json.loads(line))
    return twits

def msg_level_df(twits, test=False):
    """ 
    Convert raw data to data frame. 
    """ 
    fmt = '%Y-%m-%dT%H:%M:%SZ'

    user = []
    recommend = []
    body = []

    calendar_date = []

    link_desc = []

    for twit in twits:
        user.append(twit['user_id'])
        if not test:
            recommend.append(twit['recommended'])
        body.append(twit['body'])
        calendar_date.append(datetime.date(datetime.strptime(twit['created_at'], fmt)))

        desc = ''
        if 'links' in twit:
            if 'description' in twit['links'][0]:
                if(twit['links'][0]['description']!=None):
                    desc += twit['links'][0]['description'] 
        link_desc.append(desc)

    if not test:
        df = pd.DataFrame({'user':user, 'recommend':recommend, 'calendar_date':calendar_date,'body':body, 'link_desc':link_desc})
    else:
        df = pd.DataFrame({'user':user, 'calendar_date':calendar_date,'body':body, 'link_desc':link_desc})

    # Create weekly bins for aggregating messages
    df['daydiff'] = df['calendar_date'].apply(lambda x: (x- date(2018,9,9)).days)
    bins = range(0, 365, 7)
    df['date_range'] = pd.cut(df['daydiff'], bins)        
    return df

def clean_twits(s):
    """
    Clean the body of messages with regex.
    """
    regex_user = re.compile('\@\w+')
    regex_link = re.compile('https?:\/\/[^\s]+')
    regex_punctuation = re.compile('[{}]'.format(''.join(['\\'+p for p in string.punctuation])))
    regex_nonAscii = re.compile('[^\x00-\x7F]')
    regex_number = re.compile('\d+')
    
    s = re.sub(regex_user, '', s)  
    s = re.sub(regex_link, 'http', s)    
    s = re.sub(regex_punctuation, '', s)    
    s = re.sub(regex_nonAscii, '', s)
    s = re.sub(regex_number, '', s) 
    return s.lower()

def text_join(x):
    return ' '.join(x)

def weekly_level_df(df):
    """
    Aggregate messages by user and week.
    """
    txt_df = df[['user','date_range','body','link_desc']].copy()
    txt_df['clean_body'] = txt_df['body'].apply(clean_twits)
    txt_df['clean_link_desc'] = txt_df['link_desc'].apply(clean_twits)
    txt_weekly = txt_df.groupby(['user', 'date_range']).agg({'clean_body':text_join, 'clean_link_desc':text_join}).reset_index()
        
    return txt_weekly

# Score Prediction
def pred_pct(x):
    return sum(x)/len(x)

def pred_y(x, df, model):
    preds = model.predict([x], batch_size=1024, verbose=1)

    y_pred = np.zeros(len(preds))
    for i in range(len(preds)):
        y_pred[i] = 1 if preds[i]>=.5 else 0
    
    pred_df = pd.concat([df, pd.DataFrame({'pred':y_pred})], axis=1)
    pred_grp= pred_df.groupby('user_id').agg({'pred':pred_pct}).reset_index() 
    pred_grp.sort_values('user_id',inplace=True)

    return pred_grp

def model_predict(training_weekly, testing_weekly):    
    X_train = training_weekly['clean_body'].values   
    X_test = testing_weekly['clean_body'].values    
    tokenizer = text.Tokenizer(num_words=MAX_FEATURES)
    tokenizer.fit_on_texts(list(X_train))
    X_test = tokenizer.texts_to_sequences(X_test)
    x_test = sequence.pad_sequences(X_test, MAX_LEN)
    
    # Load Trained Model
    json_file = open('cnn_model.json', 'r')
    loaded_model_json = json_file.read()
    json_file.close()
    loaded_model = model_from_json(loaded_model_json)
    loaded_model.load_weights("cnn_model.h5")
    loaded_model.compile(loss='binary_crossentropy', optimizer='rmsprop', metrics=['accuracy'])    

    return pred_y(x_test, testing_weekly, loaded_model)

class ReputationScoreLogic():
    def __init__(self):
        pass

    def write_to_s3(self, content, filename):
        # define S3 connection in function call to avoid timeout
        csv_buffer = StringIO()
        content.to_csv(csv_buffer, index=False)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(S3_BUCKET, os.path.join(S3_PATH, filename)).put(Body=csv_buffer.getvalue())

    def get_reputation_score(self):
        msg_data_train = read_file('st_messages_train.data')
        msg_df_train = msg_level_df(msg_data_train)
        msg_weekly_train = weekly_level_df(msg_df_train)

        msg_data_test = read_file('st_messages_test.data')
        msg_df_test = msg_level_df(msg_data_test)
        msg_weekly_test = weekly_level_df(msg_df_test)

        return model_predict(msg_weekly_train, msg_weekly_test)