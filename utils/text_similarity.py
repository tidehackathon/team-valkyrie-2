import os

os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'

import itertools
import numpy as np
from sentence_transformers import SentenceTransformer, util

text_list = ["Hello, my name is Gustavo", "Hey, I'm Gustavo"]
N = 2
TOKEN_DIFF_THRESHOLD = 50


def get_list_of_shorter_text(sentence1, sentence2):
    texts = []
    temp_text = ''
    for text in sentence1.split('.'):
        if len(temp_text) > len(sentence2):
            texts.append(temp_text)
            temp_text = ''
        temp_text = temp_text + ('. ' + text if temp_text != '' else text)
    return texts


def text_similarity():
    text_list = ["Hello, my name is Gustavo", "Hey, I'm Gustavo"]
    text_pairs_examples = []
    for pair in itertools.combinations(text_list, 2):
        text_pairs_examples.append(pair)
    text_pairs_examples = np.array(text_pairs_examples)

    model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
    print("model created")

    for text1, text2 in text_pairs_examples[:N]:
        print(f'SENTENCE 1: {text1[:100]}')
        print(f'SENTENCE 2: {text2[:100]}')
        token_diff = np.abs(len(model.tokenizer(text1)['input_ids']) - len(model.tokenizer(text2)['input_ids']))

        if token_diff > TOKEN_DIFF_THRESHOLD:
            if len(text1) > len(text2):
                text1 = get_list_of_shorter_text(text1, text2)

            else:
                text2 = get_list_of_shorter_text(text2, text1)
                print(text2)
        texts_embed1 = model.encode(text1, show_progress_bar=False)
        texts_embed2 = model.encode(text2, show_progress_bar=False)
        print(f'Similarity: {util.pytorch_cos_sim(texts_embed1, texts_embed2).mean()}\n\n')


text_similarity()
