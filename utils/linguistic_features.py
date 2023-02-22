import string
from collections import Counter
import re
import spacy

NLP = spacy.load("en_core_web_lg")


def named_entities(text: str) -> dict:
    return [
        {
            "text": ent.text,
            "start_char": ent.start_char,
            "end_char": ent.end_char,
            "label": ent.label_,
        } for ent in NLP(text).ents
    ]


def text_to_json(data):
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
