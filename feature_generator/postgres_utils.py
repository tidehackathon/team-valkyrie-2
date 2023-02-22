import psycopg2
from psycopg2 import extras
import json


class PostrgresService:
    def __init__(self):
        with open('secrets.json', 'r') as f:
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

    def close(self):
        self.conn.close()
