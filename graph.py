""" Script to read input data about posts, detect possible relationships, insert them into Neo4J,
calculate centralities for all users (separate values for direct references, hashtags,
credible URLs, and uncredible URLs), and export them back.

To save on network traffic, everything is done with local JSON and CSV files.
As the next step, the output files are uploaded into PostgreSQL.
"""
import csv
import json
import re
import time
from urllib.request import urlopen
from neo4j import GraphDatabase

URL = 'bolt://localhost:7687'
USER = 'neo4j'
PASSWORD = '*****'

INPUT_PATH = 'posts.jsonl'
INPUT_PREPARED_PATH = 'posts_prepared.json'
OUTPUT_PATH = 'graph_{}.csv'

INSERT_LIMIT = 1000
PROCESS_LIMIT = 100000
READ_LIMIT = 5000

LABELED_DOMAINS = {
    'bbc.co.uk': 1,
    'bbc.com': 1,
    'cnn.com': 1,
    'nytimes.com': 1,
    'reuters.com': 1,
    'theguardian.com': 1,
    'washingtonpost.com': 1,
    'rt.com': -1,
    'sputniknews.com': -1,
    'tass.com': -1
}
REMOVE_DOMAINS = {'twitter.com'}
DOMAIN_ALIASES = {
    'mol.im': 'dailymail.co.uk',
    'abcn.ws': 'abcnews.go.com',
    'reut.rs': 'reuters.com',
    'youtu.be': 'youtube.com',
    'nyti.ms': 'nytimes.com',
    'a.msn.com': 'msn.com'
}
URL_SHORTENERS = {'bit.ly', 'dlvr.it', 'trib.al', 'ift.tt', 'is.gd'}
REMOVE_TAGS = {'Ukraine', 'Russia', 'StandWithUkraine'}

def insert_data(obj, data):
    obj.run("""
WITH $data AS data
UNWIND data AS tweet
MERGE (s:Source {id: tweet.source_id})
ON CREATE SET s.name = tweet.source_name
FOREACH (domain IN tweet.domains |
  MERGE (d:Domain {domain: domain.domain})
  ON CREATE SET d.score = domain.score
  MERGE (s)-[:POSTED_DOMAIN {id: tweet.original_id}]->(d)
  MERGE (d)-[:DOMAIN_POSTED_BY {id: tweet.original_id}]->(s)
)
FOREACH (tag IN tweet.hashtags |
  MERGE (h:Hashtag {tag: tag})
  MERGE (s)-[:POSTED_TAG {id: tweet.original_id}]->(h)
  MERGE (h)-[:TAG_POSTED_BY {id: tweet.original_id}]->(s)
)
FOREACH (reference IN tweet.references |
  MERGE (s2:Source {id: reference})
  MERGE (s)-[:REFERRED {id: tweet.original_id}]->(s2)
  MERGE (s2)-[:REFERRED_BY {id: tweet.original_id}]->(s)
)
""", data=data)

def clean_data(obj):
    timestamp = time.time()
    obj.run('MATCH (s:Source) WHERE s.name IS NULL DETACH DELETE s')
    obj.run('MATCH (s:Source) WHERE NOT (s)-[]-() DETACH DELETE s')
    print('Cleaned data in {} s'.format(time.time() - timestamp))

def analyze_hashtag_data(obj):
    timestamp = time.time()
    query = """
    CALL gds.graph.project('Hashtags', ['Source', 'Hashtag'],
        ['POSTED_TAG', 'TAG_POSTED_BY'])
    """
    obj.run(query)

    query = """
    CALL gds.eigenvector.write(
        'Hashtags',
        {maxIterations: 20, scaler: 'MinMax', writeProperty: 'hashtagsCentrality'})
    """
    obj.run(query)
    print('Analyzed hashtag data in {} s'.format(time.time() - timestamp))

def analyze_referred_data(obj):
    timestamp = time.time()
    query = """
    CALL gds.graph.project('References', 'Source',
        ['REFERRED', 'REFERRED_BY'])
    """
    obj.run(query)

    query = """
    CALL gds.eigenvector.write(
        'References',
        {maxIterations: 20, writeProperty: 'referencesCentrality'})
    """
    obj.run(query)
    print('Analyzed referred data in {} s'.format(time.time() - timestamp))

def analyze_domain_data(obj):
    timestamp = time.time()
    query = """
    CALL gds.graph.project('Domains', ['Source', 'Domain'],
        ['POSTED_DOMAIN', 'DOMAIN_POSTED_BY'])
    """
    obj.run(query)

    query = """
    MATCH (PosDomain:Domain {score: 1})
    CALL gds.eigenvector.write(
        'Domains',
        {maxIterations: 20, writeProperty: 'posDomainsCentrality', sourceNodes: [PosDomain]})
    YIELD nodePropertiesWritten, ranIterations
    RETURN distinct 'done'
    """
    obj.run(query)

    query = """
    MATCH (NegDomain:Domain {score: -1})
    CALL gds.eigenvector.write(
        'Domains',
        {maxIterations: 20, writeProperty: 'negDomainsCentrality', sourceNodes: [NegDomain]})
    YIELD nodePropertiesWritten, ranIterations
    RETURN distinct 'done'
    """
    obj.run(query)
    print('Analyzed domain data in {} s'.format(time.time() - timestamp))

def read_data(obj, offset, limit):
    res = obj.run("""
    MATCH (s:Source)
    RETURN s.id, s.hashtagsCentrality, s.referencesCentrality,
        s.posDomainsCentrality, s.negDomainsCentrality
    SKIP {} LIMIT {}
    """.format(offset, limit))
    res = [dict(row) for row in res]
    res = [{'id': row['s.id'],
            'hash_c': round(row['s.hashtagsCentrality'] or 0, 6),
            'ref_c': round(row['s.referencesCentrality'] or 0, 6),
            'p_dom_c': round(row['s.posDomainsCentrality'] or 0, 6),
            'n_dom_c': round(row['s.negDomainsCentrality'] or 0, 6)} for row in res]
    return res

def delete_data(obj):
    timestamp = time.time()
    obj.run('MATCH (n) DETACH DELETE n')
    obj.run("CALL gds.graph.drop('Hashtags', false)")
    obj.run("CALL gds.graph.drop('References', false)")
    obj.run("CALL gds.graph.drop('Domains', false)")
    print('Deleted data in {} s'.format(time.time() - timestamp))

def process(row):
    res = {}
    res['source_id'] = str(row['source_details']['id'])
    res['source_name'] = row['source_name']
    res['original_id'] = str(row['original_id'])
    urls = []
    for url in (row['external_urls'] or []):
        if (re.sub(r'^www.', '', url.split('://')[-1].split('/')[0].split(':')[0])
                in URL_SHORTENERS):
            try:
                url = urlopen(url).geturl()
            except Exception:
                pass

        urls.append(url)

    domains = [
        re.sub(r'^www.', '', url.split('://')[-1].split('/')[0].split(':')[0]).lower()
        for url in urls]
    domains = [DOMAIN_ALIASES.get(d, d) for d in domains]
    res['domains'] = [
        {'domain': d, 'score': LABELED_DOMAINS.get(d, 0)} for d in set(domains)
        if d not in REMOVE_DOMAINS]
    res['hashtags'] = [t for t in (row['hashtags'] or []) if t not in REMOVE_TAGS]
    res['references'] = []
    if row['replied_source_details']:
        res['references'].append(str(row['replied_source_details']['id']))

    if row['quoted_details']:
        res['references'].append(str(row['quoted_details']['user']['id']))

    if row['shared_details']:
        res['references'].append(str(row['shared_details']['user']['id']))

    res['references'] = [r for r in set(res['references']) if r != res['source_id']]
    if not res['domains'] and not res['hashtags'] and not res['references']:
        return None

    return res

def prepare():
    data = []
    with open(INPUT_PATH, encoding='utf8') as obj:
        while True:
            line = obj.readline()
            if not line:
                break

            row = process(json.loads(line))
            if not row:
                continue

            data.append(row)

    data.sort(key=lambda row: row['source_id'])
    with open(INPUT_PREPARED_PATH, 'w', encoding='utf8') as obj:
        json.dump(data, obj)

def run(data, num):
    timestamp = time.time()

    driver = GraphDatabase.driver(URL, auth=(USER, PASSWORD))
    with driver.session() as session:
        session.execute_write(delete_data)
        insert_timestamp = time.time()
        while True:
            chunk = data[:INSERT_LIMIT]
            if not chunk:
                break

            data = data[INSERT_LIMIT:]
            session.execute_write(insert_data, chunk)
            print('Inserted {} rows for chunk {}'.format(len(chunk), num))

        print('Inserted data for chunk {} in {} s'.format(num, time.time() - insert_timestamp))
        session.execute_write(clean_data)
        session.execute_write(analyze_hashtag_data)
        session.execute_write(analyze_referred_data)
        session.execute_write(analyze_domain_data)
        read_timestamp = time.time()
        with open(OUTPUT_PATH.format(num), 'w', newline='', encoding='utf8') as obj:
            fieldnames = ['id', 'hash_c', 'ref_c', 'p_dom_c', 'n_dom_c']
            writer = csv.DictWriter(obj, fieldnames=fieldnames)
            writer.writeheader()
            offset = 0
            while True:
                res = session.execute_read(read_data, offset, READ_LIMIT)
                if not res:
                    break

                offset += READ_LIMIT
                for row in res:
                    writer.writerow(row)

        print('Read data for chunk {} in {} s'.format(num, time.time() - read_timestamp))

    print('Finished chunk {} in {} s'.format(num, time.time() - timestamp))


def main():
    timestamp = time.time()
    with open(INPUT_PREPARED_PATH, encoding='utf8') as obj:
        data = json.load(obj)

    num = 0
    while True:
        chunk = data[:PROCESS_LIMIT]
        if not chunk:
            break

        data = data[PROCESS_LIMIT:]
        last_source_id = chunk[-1]['source_id']
        while data and data[0]['source_id'] == last_source_id:
            chunk.append(data.pop(0))

        num += 1
        run(chunk, num)

    print('Finished in {} s'.format(time.time() - timestamp))

if __name__ == '__main__':
    # prepare() # run only once to pre-process input data
    main()
