"""
Script for loading feature data into PostgreSQL database
"""
import json

import pandas
from postgres_utils import PostrgresService

BATCH_SIZE = 10000

insert_query = """
insert into metabase.features_v1 (
    original_id, 
    twitter_roberta_base_sentiment_latest,
    twitter_roberta_base_emotion,
    bert_finetuned_propaganda_18, 
    fake_news_bert_detect, 
    distilroberta_propaganda_2class
) 
values %s
"""

files = ['../analysis_sentiment_emotions_1prop.jsonl', '../analysis_2prop.jsonl']
dfs = []
for file_name in files:
    df = pandas.read_json(file_name, lines=True)
    column_mapping = {column: column.lower().replace("-", "_") for column in df.columns.tolist()}
    column_mapping['id'] = 'original_id'

    df.rename(columns=column_mapping, inplace=True)
    dfs.append(df)

print("Merging dataframes")
merged_df = pandas.merge(*dfs, on='original_id', how='outer')[['original_id',
                                                               'twitter_roberta_base_sentiment_latest',
                                                               'twitter_roberta_base_emotion',
                                                               'bert_finetuned_propaganda_18',
                                                               'fake_news_bert_detect',
                                                               'distilroberta_propaganda_2class']]
print("Dataframes merged")
postgres_service = PostrgresService()
batch = []
count = 0
for index, row in merged_df.iterrows():
    row = row.to_dict()
    item = []
    for value in row.values():
        item.append(json.dumps(value) if (isinstance(value, dict) or isinstance(value, list)) else value)
    batch.append(tuple(item))

    if len(batch) == BATCH_SIZE or (len(batch) > 0 and count == merged_df.shape[0] - 1):
        postgres_service.insert_batch(insert_query, batch)
        batch = []
    count += 1

postgres_service.close()
