import json
import math
import sys
import numpy as np
import pandas as pd
from dataclasses import astuple
import json
from models import FeaturesInfo, PostsInfo, SourcesInfo
from config import INSERT_BATCH_SIZE
from sql import INSERT_FEATURES_QUERY, INSERT_SOURCES_QUERY, INSERT_POSTS_QUERY

sys.path.append(".")
from common.src.db_core import hackathon_db


def preprocess_dataset(dataframe):
    dataframe = dataframe.where(pd.notnull(dataframe), None)
    dataframe['username'] = dataframe['user'].apply(lambda x: x.get('username'))
    dataframe['mentioned_users'] = dataframe['mentionedUsers'].apply(
        lambda x: [el.get('username') for el in x] if x else None)
    dataframe['retweeted_tweet'] = dataframe['retweetedTweet'].apply(
        lambda x: x.get('id') if x and not math.isnan(x) else None)
    dataframe['quoted_tweet'] = dataframe['quotedTweet'].apply(lambda x: x.get('id'))
    dataframe['user_id'] = dataframe['user'].apply(
        lambda x: x.get('id'))

    dataframe = dataframe.drop_duplicates(subset=['username'])
    dataframe = dataframe.replace({np.nan: None})

    return dataframe


def fill_sources():
    try:
        conn = hackathon_db()
    except Exception as e:
        print(e)
    for i in range(15):
        print(f'working with batch: {i}')
        df = pd.read_pickle(f'datasources/data_clean/data_clean_{i}.pkl')
        df = preprocess_dataset(df)

        df = df[['username', 'url', 'user_id', 'date']]

        df = df.rename(columns={
            'date': 'original_timestamp',
            'username': 'name',
            'user_id': 'original_id'
        })

        my_data = []

        for _, data in df.iterrows():
            my_data.append(SourcesInfo.from_df_row(data))

        ingestion_dataset = tuple(map(lambda x: x.to_tuple(), my_data))

        for i in range(0, len(ingestion_dataset), INSERT_BATCH_SIZE):
            batch = ingestion_dataset[i:i + INSERT_BATCH_SIZE]
            conn.execute_query(INSERT_SOURCES_QUERY, params=batch)


def fill_posts():

    try:
        conn = hackathon_db()
    except Exception as e:
        print(e)
    for i in range(15):
        print('Working with ', i)
        df = pd.read_pickle(f'datasources/data_clean/data_clean_{i}.pkl')
        print(df.shape)
        df = preprocess_dataset(df)
        df = df[['url', 'date', 'username', 'user', 'content', 'renderedContent', 'lang', 'outlinks', 'media',
                 'replyCount', 'retweetCount', 'likeCount', 'quoteCount', 'retweeted_tweet', 'retweetedTweet',
                 'quoted_tweet', 'quotedTweet', 'inReplyToTweetId', 'inReplyToUser', 'mentioned_users', 'mentionedUsers',
                 'hashtags', 'coordinates', 'place', 'id']]

        df = df.rename(columns={'date': 'original_timestamp', 'username': 'source_name', 'user': 'source_details',
                                'renderedContent': 'content_rendered', 'outlinks': 'external_urls',
                                'media': 'media_details',
                                'replyCount': 'reply_count', 'likeCount': 'like_count', 'quoteCount': 'quote_count',
                                'retweeted_tweet': 'shared_id', 'retweetedTweet': 'shared_details',
                                'quoted_tweet': 'quoted_id',
                                'quotedTweet': 'quoted_details', 'inReplyToTweetId': 'replied_id',
                                'inReplyToUser': 'replied_source_details', 'mentioned_users': 'mentioned_source_names',
                                'mentionedUsers': 'mentioned_source_details', 'id': 'original_id',
                                'retweetCount': 'share_count'})

        my_data = []
        for _, data in df.iterrows():
            my_data.append(PostsInfo.from_df_row(data))

        ingestion_dataset = tuple(map(lambda x: x.to_tuple(), my_data))

        for i in range(0, len(ingestion_dataset), INSERT_BATCH_SIZE):
            batch = ingestion_dataset[i:i + INSERT_BATCH_SIZE]
            #
            conn.execute_query(INSERT_POSTS_QUERY, params=batch)


def process():

    try:
        conn = hackathon_db()
    except Exception as e:
        print(e)
    indexer = 0
    chunksize = 100_000
    for chunk in pd.read_csv('datasources/data_sample.csv', encoding='utf-8', chunksize=chunksize):
        print(f'working with batch: {indexer}')
        chunk.drop(chunk[chunk['0'] == '0'].index, inplace=True)
        chunk['id'] = chunk['0'].apply(lambda x: json.loads(x).get('id'))
        chunk['content'] = chunk['0'].apply(lambda x: json.loads(x).get('content'))
        chunk.drop(columns=['0'], inplace=True)
        print(f'Chunk {indexer} has been processed')
        print(chunk.shape)

        my_data = []
        for _, data in chunk.iterrows():
            my_data.append(FeaturesInfo.from_df_row(data))

        ingestion_dataset = tuple(map(lambda x: x.to_tuple(), my_data))

        for i in range(0, len(ingestion_dataset), INSERT_BATCH_SIZE):
            batch = ingestion_dataset[i:i + INSERT_BATCH_SIZE]
            conn.execute_query(INSERT_FEATURES_QUERY, params=batch)

        print(f'Chunk {indexer} has been ingested to database')

        indexer += 1


if __name__ == '__main__':
    process()
