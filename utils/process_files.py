"""
Process file for extracting linguistic feature using multiprocessing
"""


import json
import multiprocessing as mp
import time
from math import ceil

import pandas as pd

from src.utils.linguistic_features import extract_linguistic_features


def worker(texts: str, output_queue: mp.Queue) -> None:
    """
    Worker for process
    :param texts: Text to extract features: str
    :param output_queue: queue for processes
    :return: None
    """
    for _, t in texts.iterrows():
        result = extract_linguistic_features(t)
        output_queue.put_nowait(result)


def process_data() -> None:
    """
    Read and process data in multiprocess way.
    :return: 0
    """
    start_time = time.time()
    queue = mp.Queue()

    print('reading data...')
    df = pd.read_pickle("../data/Data/data_simple.pkl")
    print('data were read')

    processing_df = df[['pid', 'content']]
    print(processing_df.shape[0])

    n = 7  # Number of processes

    list_len = ceil(processing_df.shape[0] / n)

    df_chunks = [processing_df[i:i + list_len] for i in range(0, processing_df.shape[0], list_len)]

    df_lens = [*map(len, df_chunks)]
    print(df_lens)

    workers = [mp.Process(target=worker, args=(chunk, queue)) for chunk in df_chunks]

    for w in workers:
        w.start()

    for i in range(n):
        results = map(json.dumps, (queue.get() for _ in range(df_lens[i])))
        pd.DataFrame(results).to_csv('../data/features_data/data_sample.csv', mode='a', index=False)
        print('added')
    print('finished')

    for w in workers:
        w.join()

    end_time = time.time()
    print(f"Estimate: {end_time - start_time}")


if __name__ == '__main__':
    main()
