import json
import os
import csv
from collections import defaultdict
MIN_length=4
class LabelField:
    def __init__(self):
        self.label2id = dict()
        self.label_num = 0

    def get_id(self, label):
        if label not in self.label2id:
            self.label2id[label] = self.label_num
            self.label_num += 1
        return self.label2id[label]

def read_metadata(path):
    meta_data = {}
    with open(path, 'r', encoding='utf-8') as file:
        for line in file:
            data = json.loads(line)
            asin = data['parent_asin']
            meta_data[asin] = {
                'title': data.get('title', ''),
                'brand': data.get('brand', ''),
                'category': ' '.join(data.get('category', []))
            }
    return meta_data

def process_interactions(path, user_field, s_field):
    sequences = defaultdict(list)
    with open(path, newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        for row in reader:
            user_id, asin, _, timestamp, history = row
            if len(history.split(' '))<=MIN_length:
                continue
            user_id = user_field.get_id(user_id)
            asin = s_field.get_id(asin)
            sequences[user_id].append((asin, int(timestamp)))
    return sequences

def write_json(data, path):
    with open(path, 'w', encoding='utf-8') as file:
        json.dump(data, file)

Category_list = ["All_Beauty", "Digital_Music", "Video_Games"]
base_path = './Amazon23'
for category_name in Category_list:
    meta_path = f'{base_path}/meta/meta_{category_name}.jsonl'
    output_dir = f'{base_path}/custom/{category_name}'
    os.makedirs(output_dir, exist_ok=True)
    metadata = read_metadata(meta_path)
    user_field = LabelField()
    s_field = LabelField()

    for split in ['train', 'valid', 'test']:
        interaction_path = f'{base_path}/interaction/{category_name}.{split}.csv'
        sequences = process_interactions(interaction_path, user_field, s_field)
        output_file = os.path.join(output_dir, f'{split}.json')
        write_json(sequences, output_file)

    # Writing meta, user map, and item map data
    write_json(metadata, os.path.join(output_dir, 'meta_data.json'))
    write_json(user_field.label2id, os.path.join(output_dir, 'umap.json'))
    write_json(s_field.label2id, os.path.join(output_dir, 'smap.json'))
