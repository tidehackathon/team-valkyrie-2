import psycopg2
from psycopg2 import extras
import json


class PostrgresService:
    def __init__(self, credentials_file_path):
        with open(credentials_file_path, 'r') as f:
            secrets = json.load(f)

        self.conn = psycopg2.connect(database=secrets['database'],
                                     host=secrets['host'],
                                     user=secrets['user'],
                                     password=secrets['password'],
                                     port=secrets['port'])

    def insert_batch(self, query, data):
        print(f"query {query}, data size {len(data)}")
        cursor = self.conn.cursor()
        extras.execute_values(
            cursor, query, data, template=None, page_size=1000
        )
        self.conn.commit()
        cursor.close()

    def query_data(self, query, batch_size):
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor,
                              name='fetch_large_result') as cursor:
            cursor.itersize = batch_size

            cursor.execute(query)
            for row in cursor:
                yield {element[0]: element[1] for element in row.items()}

    def close(self):
        self.conn.close()
