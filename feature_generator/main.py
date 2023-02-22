import json
import os
import torch
import yaml
import datetime

from pathlib import Path

import pandas as pd
import numpy as np
from scipy.special import softmax
from transformers import AutoModelForSequenceClassification
from transformers import AutoTokenizer, AutoConfig


def initialize_model(model_path: str, supports_cuda: bool):
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    labels = AutoConfig.from_pretrained(model_path).id2label
    if supports_cuda:
        model = AutoModelForSequenceClassification.from_pretrained(
            model_path,
            device_map="auto"
        )
    else:
        model = AutoModelForSequenceClassification.from_pretrained(model_path)
    return model, tokenizer, labels


def run_inference(input_batch, feature_name, model, tokenizer, labels):
    texts = [preprocess_text(element['content']) for element in input_batch]
    start_prediction = datetime.datetime.utcnow()
    encoded_input = tokenizer(texts, return_tensors='pt', padding=True, max_length=512, truncation=True)
    output = model(**encoded_input)
    print(f"Predicted {feature_name} in {datetime.datetime.utcnow() - start_prediction}")
    for index, element in enumerate(input_batch):
        scores = softmax(output[0][index].detach().numpy())

        ranking = np.argsort(scores)
        ranking = ranking[::-1]
        result = {}
        for i in range(scores.shape[0]):
            label = labels[ranking[i]]
            score = scores[ranking[i]]
            result[label] = np.round(float(score), 4)
        element[feature_name] = result


# Preprocess text (username and link placeholders)
def preprocess_text(text):
    new_text = []
    for t in text.split(" "):
        t = '@user' if t.startswith('@') and len(t) > 1 else t
        t = 'http' if t.startswith('http') else t
        new_text.append(t)
    return " ".join(new_text)


if __name__ == "__main__":
    print(f'Cuda is available: {torch.cuda.is_available()}')
    config = yaml.safe_load(Path('config.yml').read_text())

    features = config.get("features")
    source_data_file = config.get("source_data_file")
    start_from = config.get("start_from")
    end_at = config.get("end_at")
    batch_size = config.get("batch_size")
    dest_file = config.get("dest_file")

    if os.path.exists(dest_file):
        os.remove(dest_file)

    start = datetime.datetime.utcnow()
    models = {}
    for name, model_info in features.items():
        models[name] = initialize_model(**model_info)
    print(f'Models initialized in {datetime.datetime.utcnow() - start}')
    df = pd.read_pickle(source_data_file)
    if start_from:
        df = df[start_from:]
    if end_at:
        df = df[:end_at]
    result_batch = []
    counter = 0
    for index, row in df.iterrows():
        result = row.to_dict()
        result_batch.append({'id': row['id'], 'content': row['content']})
        if len(result_batch) == batch_size or (counter == (df.shape[0] - 1) and len(result_batch) > 0):
            for task_name, model_data in models.items():
                run_inference(result_batch, task_name, *model_data)
            with open(dest_file, "a") as f:
                for result in result_batch:
                    f.write(json.dumps(result) + '\n')
            print(f"--------\nProcessed records {counter + 1}-{counter + 1 - len(result_batch)}."
                  f"Time: {datetime.datetime.utcnow() - start}\n--------")
            result_batch = []
        counter += 1
    print(f'Finished in {datetime.datetime.utcnow() - start}')
