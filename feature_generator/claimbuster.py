"""
Script for retrieving information from Claimbuster API
in order to have data about presence of claim in the provided text
"""
import concurrent.futures
import json
import logging
import time
import requests
import os

from utils.postgres_utils import PostrgresService

INPUT_PATH = 'posts_info.jsonl'
OUTPUT_PATH = 'claimbuster.json'

BATCH_SIZE = 1000
MAX_SIZE = 1000000
THREADS = 10
URL_RETRIES = 10
URL_SLEEP = 5


CLAIMBUSTER_API_KEY = os.environ['CLAIMBUSTER_API_KEY']
HEADERS = {'x-api-key': CLAIMBUSTER_API_KEY}
URL = 'https://idir.uta.edu/claimbuster/api/v2/score/text/'
LOG_PATH = 'claimbuster.log'


def get_score(input_text):
    for i in range(URL_RETRIES):
        try:
            res = None
            res = requests.post(url=URL,
                                json={'input_text': input_text},
                                headers=HEADERS).json()
            return res['results'][0]['score']
        except Exception as exc:
            if i < URL_RETRIES - 1:
                time.sleep(URL_SLEEP * (i + 1))
            else:
                logging.error('Text: %s', input_text)
                logging.error('Result: %s', res)
                logging.error('Error: %s', exc)
                break

    return None


def process_batch(data):
    res = []
    for row in data:
        score = get_score(row['content_rendered'])
        if not score:
            continue

        res.append({'post_id': row['post_id'], 'score': score})

    logging.info('Batch finished: %s scores for %s posts',
                 len(data), len(res))
    return res


def split_data(data):
    for i in range(0, len(data), THREADS):
        yield data[i:i + THREADS]


def process_batches(data):
    """
    Input: [{"post_id", "content_rendered"}, ...]
    Output: [{"post_id", "score"}, ...]
    """
    batches = list(split_data(data))
    res = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(process_batch, batch)
                   for batch in batches if batch]
        for future in concurrent.futures.as_completed(futures):
            try:
                res.extend(future.result())
            except Exception as exc:
                logging.error('Thread Error: %s', exc)

    logging.info('%s batches finished', len(batches))
    return res


def init_logging():
    logging.basicConfig(filename=LOG_PATH, level=logging.INFO,
                        format='%(asctime)s %(levelname)s: %(message)s')


INSERT_QUERY = """
insert into metabase.claimbuster (
    post_id, 
    score
) 
values %s
"""
SELECT_QUERY = """
SELECT 
    id as post_id, 
    content_rendered 
FROM 
    metabase.posts
"""
STAGE_FILE = 'data_stage.jsonl'


def main():
    init_logging()
    timestamp = time.time()

    if os.path.exists(STAGE_FILE):
        os.remove(STAGE_FILE)
    pg_service = PostrgresService('utils/secrets.json')

    row_iterator = pg_service.query_data(SELECT_QUERY, BATCH_SIZE)
    with open(STAGE_FILE, 'r') as f:
        count = 0
        while count < MAX_SIZE:
            row = next(iter(row_iterator), None)
            f.write(json.dumps(row) + '\n')
            count += 1

    total_count = 0
    size = BATCH_SIZE * THREADS
    data = []
    with open(STAGE_FILE, 'r') as f:
        while total_count < MAX_SIZE:
            line = f.readline()
            if not line:
                break
            row = json.loads(line)
            total_count += 1
            data.append(row)
            if len(data) >= size:
                batch_result = [tuple(element.values()) for element in process_batches(data)]
                pg_service.insert_batch(INSERT_QUERY, batch_result)
                data = []
    if data:
        batch_result = [tuple(element.values()) for element in process_batches(data)]
        pg_service.insert_batch(INSERT_QUERY, batch_result)

    pg_service.close()
    logging.info('Processed %s posts in %s s', total_count, (time.time() - timestamp))


if __name__ == '__main__':
    main()
