import pandas as pd
import json 
from datetime import datetime
import glob
import os 


# consumer_key = '4874163281-wyS1OOIChIbc5unCeacIcGBuPBXGuwwBcmHhHgR'
# consumer_secret = 'Yft4gUhLHkgBvSAJMyBybUWMdDKRJ5mqMisNEzgvFRxD2'
# access_key = 'bKBryjFZBMKWhOuGyLxwg9WHh'
# access_secret = '3WhxMfvN4NdGGxscNZg0O339SEBCTQxcCGgS3uY8usKoi3SA20'
# # Twitter authentication
# auth = tweepy.OAuthHandler(access_key, access_secret)
# auth.set_access_token(consumer_key, consumer_secret)
# Creating an API object api = tweepy.API(auth)

# insert the CSV -- and put it into a dataframe
csv_files = glob.glob("/Users/adeolu.adegboye/Personal_Projects/Twitter_Airflow_Project/*.csv")
print("CSV files found:", csv_files)

def run_twitter_etl():
    for file in csv_files:
        if os.path.isfile(file):
            print(f"Processing file: {file}")
            
            # Read CSV without headers and assign column names
            df = pd.read_csv(file, 
                    dtype={
                'author': 'str',
                'content': 'str',
                'country': 'str',
                'id': 'int',
                'language': 'str',
                'latitude': 'float',
                'longitude': 'float',
                'number_of_shares': 'int',
                'number_of_likes' : 'int',
                'date_time': 'str'  # Read as string to parse later
            })
            df['date_time'] = pd.to_datetime(df['date_time'])
             # Normalize text data (convert to lowercase)
            df['author'] = df['author'].str.lower()
            df['content'] = df['content'].str.lower()
            df['country'] = df['country'].str.lower()
            df['language'] = df['language'].str.lower()

            # Drop unnecessary columns, for example, 'latitude' and 'longitude'
            df = df.drop(columns=['latitude', 'longitude'])

            # Remove rows with missing values
            df = df.dropna()


            # Save the cleaned DataFrame to an S3 bucket
            df.to_csv("s3://adeolu-airflow-youtube-bucket/tweets_cleaned.csv", index=False)

    # df.to_csv("s3://adeolu-airflow-youtube-bucket/tweets.csv")

 
# tweets = api.user_timeline(screen_name='@elonmusk',
#                            # 200 is the maximum allowed count
#                            count=200,
#                            include_rts = False,
#                            # Necessary ro keep full_text
#                            # otherwise only the first 140 words are extracted
#                            tweet_mode = 'extended'
#                            )
# print(tweets)

