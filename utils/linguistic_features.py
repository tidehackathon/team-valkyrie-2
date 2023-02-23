import string
from collections import Counter
import re
import spacy
import pandas as pd

NLP = spacy.load("en_core_web_lg")


def named_entities(text: str) -> list:
    """
    Extract entities from text

    :param text: text to process: str
    :return: list of dictionaries with entity description: list
    """
    return [
        {
            "text": ent.text,
            "start_char": ent.start_char,
            "end_char": ent.end_char,
            "label": ent.label_,
        } for ent in NLP(text).ents
    ]


def extract_linguistic_features(data: pd.DataFrame) -> dict:
    """
    Function to process text and return linguistic features such as input,
    lemmatized form, entities, word pos

    :param data: Row of dataframe consists of post_id: int and content: text
    :return: features from data: dict
    """
    # sentences = [sent.text for sent in NLP(text).sents]

    # sentences = text.split('\n\n')

    cleaned_sentence = re.sub(r'[^\w\s]', '', data[1])

    lemmatized = [{"word": word, "lemma": word.lemma_, "pos": word.pos_, "is_stop": word.is_stop} for word in
                  NLP(cleaned_sentence)]

    counts = Counter(item["lemma"] for item in lemmatized)
    my_dict = {
        "input": data[1],
        "lemmatized": [item["lemma"] for item in lemmatized],
        "entities": named_entities(data[1]),
        "words": {
            item["lemma"]: {
                "count": counts[item["lemma"]],
                "pos": item["pos"],
                "is_stop": item["is_stop"]
            } for item in lemmatized
        }
    }

    print('Row ready to go')
    return {
        "pid": data[0], "content": my_dict
    }
