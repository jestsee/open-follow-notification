import tweepy
import time
import pandas as pd
import numpy as np
import datetime
import schedule
import re

consumer_key = "SL7TTEu8JrT0Ornb4xCv4IoX5"
consumer_secret = "arkgteZxNV3cXjuHSrEkcGE5ArV9HgXZ9tFSqylb8IPbkFBl90"
access_token = "856252793962856449-MMp1RMouRCG6PkpKFgK7xYluFjC0eMy"
access_token_secret = "4VTU3H2aRID8exCvZoV1ILzw95XTCjULTdvXS8qVkp4V0"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

dff = pd.read_csv("twitter.csv")


def send_dm(usn, tweet_id):
    bae = api.get_user(screen_name='baelieves')
    text = 'https://twitter.com/'+usn+'/status/' + str(tweet_id)
    api.send_direct_message(bae.id, text)


keywords = ['open follow', 'opfol', 'open follback', 'openfollow']


def job():
    MINUTES = 1
    for index, row in dff.iterrows():
        twt = row['username']
        domain = row['name']

        tweets = api.user_timeline(screen_name=twt, count=50)
        data = pd.DataFrame(
            data=[tweet.text for tweet in tweets], columns=['Tweets'])

        data['ID'] = np.array([tweet.id for tweet in tweets])
        data['Date'] = pd.to_datetime(
            np.array([tweet.created_at for tweet in tweets]))
        data['Date'] = pd.to_datetime(data['Date']).dt.tz_localize(None)
        data['Text'] = np.array([tweet.text for tweet in tweets])
        data['Username'] = np.array(
            [tweet.author.screen_name for tweet in tweets])

        created_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=MINUTES)
        data = data[(data['Date'] > created_time) & (
            data['Date'] < datetime.datetime.utcnow())]

        ndata = data[data['Tweets'].str.contains(
            "|".join(keywords), regex=True, flags=re.IGNORECASE)].reset_index(drop=True)

        if len(ndata) > 0:
            for row in ndata.itertuples():
                send_dm(row.Username, row.ID)
                print(row.Tweets)
        else:
            print("belom ada tweet yg berkaitan : (")


# yang pertama
job()

schedule.every(59).seconds.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
