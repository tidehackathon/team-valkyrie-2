import os
from pathlib import Path

BASE_DIR = Path(os.path.dirname(__file__))

with open(BASE_DIR / 'insert_sources.sql', 'rt', encoding='utf-8') as query_file:
    INSERT_SOURCES_QUERY = query_file.read()

with open(BASE_DIR / 'insert_posts.sql', 'rt', encoding='utf-8') as query_file:
    INSERT_POSTS_QUERY = query_file.read()

with open(BASE_DIR / 'insert_features.sql', 'rt', encoding='utf-8') as query_file:
    INSERT_FEATURES_QUERY = query_file.read()
