import os
from datetime import datetime
from pathlib import Path
import pandas as pd
from dateutil.relativedelta import relativedelta
import dateparser
from pymongo import MongoClient

import torch
import numpy as np
from transformers import AutoTokenizer, AutoModelForSequenceClassification

from helpers import build_combined
import subprocess

# NEW: Import tqdm for progress bars
from tqdm import tqdm

_ROOT = Path(os.path.abspath(os.path.dirname(__file__))).as_posix()

def get_data_path(path):
    return Path('/'.join(_ROOT.split('/')[:-1]), 'data', path).joinpath()

def classify_pipe(uri, model_name, model_location, batch_size):
    evntclass = EventClassifier(uri, model_name, model_location, batch_size, n_gpu=1)
    evntclass.run()

def run_git_commands(commit_message):
    try:
        subprocess.run("git add *.py", shell=True, check=True)
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        subprocess.run(["git", "push"], check=True)
        print("Git commands executed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running Git commands: {e}")

class EventClassifier:
    def __init__(self, uri, model_name, model_location, batch_size, n_gpu=1):
        self.uri = uri
        self.model_name = model_name
        self.model_location = model_location
        self.batch_size = batch_size
        self.n_gpu = n_gpu
        self.device = "cuda" if (torch.cuda.is_available() and n_gpu > 0) else "cpu"

    def get_db_info(self):
        """
        Fetch info needed from the DB and set up label_dict for label index -> label name.
        """
        self.db = MongoClient(self.uri).ml4p
        self.model_info = self.db.models.find_one({'model_name': self.model_name})
        self.label_dict = {v: k for k, v in self.model_info.get('event_type_nums').items()}

    def load_model(self):
        """
        Load the tokenizer from HF and then load local fine-tuned ModernBERT weights.
        Bypasses any local tokenizer.json by using trust_remote_code with base_model_id.
        """
        model_path = Path(self.model_location) / self.model_name
        model_path_str = model_path.as_posix()
        print(f"Loading local fine-tuned ModernBERT from: {model_path_str}")

        base_model_id = "answerdotai/ModernBERT-large"
        print(f"Loading tokenizer from: {base_model_id}")
        self.tokenizer = AutoTokenizer.from_pretrained(
            base_model_id,
            trust_remote_code=True
        )

        self.model = AutoModelForSequenceClassification.from_pretrained(
            model_path_str,
            trust_remote_code=True
        )
        self.model.to(self.device)
        self.model.eval()

    def generate_cursor(self, date):
        """
        Create a Mongo cursor for articles in the monthly collection 
        that do NOT have self.model_name set and meet certain conditions.
        """
        colname = f'articles-{date.year}-{date.month}'
        print(f"Colname: {colname}")

        source_domains = self.db.sources.distinct(
            'source_domain',
            filter={
                'include': True,
                'primary_location': {
                    '$in': [
                        # 'CRI','PAK','HND','NIC','SLV','GTM','PAN'
                        'SLB', 'NGA',
                    ]
                }
            }
        )
        self.cursor = self.db[colname].find(
            { 
                self.model_name: {'$exists': False},
                'language_translated': 'en',
                'title_translated': {
                    '$exists': True, 
                    '$ne': '', 
                    '$ne': None, 
                    '$type': 'string'
                },
                'maintext_translated': {
                    '$exists': True, 
                    '$ne': '', 
                    '$ne': None, 
                    '$type': 'string'
                },
                'source_domain': {'$in': source_domains},
                'date_publish': {'$exists': True, '$ne': '', '$ne': None}
            }
        )

    def check_index(self):
        """
        Ensure an index on self.model_name.
        """
        indexes = [list(idx['key'].keys())[0] for idx in self.db.articles.list_indexes()]
        if self.model_name not in indexes:
            self.db.articles.create_index([(self.model_name, 1)], background=True)

    def classify_articles(self):
        """
        Given docs in self.queue, run them through the model, produce top1/top2 labels (if prob>0.3),
        and store everything in self.top1_labels, self.top2_labels, self.all_model_outputs.
        """
        texts = [build_combined(doc) for doc in self.queue]

        inputs = self.tokenizer(
            texts,
            padding=True,
            truncation=True,
            return_tensors="pt"
        ).to(self.device)

        with torch.no_grad():
            outputs = self.model(**inputs)
            logits = outputs.logits
            probs = torch.softmax(logits, dim=-1).cpu().numpy()

        self.top1_labels = []
        self.top2_labels = []
        self.all_model_outputs = []

        for row in probs:
            sorted_indices = np.argsort(row)  # ascending order
            top1_idx = sorted_indices[-1]
            top2_idx = sorted_indices[-2]

            top1_prob = float(row[top1_idx])
            top2_prob = float(row[top2_idx])

            top1_label = self.label_dict[top1_idx]
            if top2_prob > 0.3:
                top2_label = self.label_dict[top2_idx]
            else:
                top2_label = None

            # Full distribution
            label_scores = {
                self.label_dict[i]: float(row[i]) for i in range(len(row))
            }

            self.top1_labels.append(top1_label)
            self.top2_labels.append(top2_label)
            self.all_model_outputs.append(label_scores)

    def insert_info(self):
        """
        Update the DB with env_max, env_sec, model_outputs for each doc in self.queue.
        """
        for nn, doc in enumerate(self.queue):
            try:
                colname = f"articles-{doc['date_publish'].year}-{doc['date_publish'].month}"
            except:
                try:
                    parsed_dt = dateparser.parse(doc['date_publish']).replace(tzinfo=None)
                    colname = f"articles-{parsed_dt.year}-{parsed_dt.month}"
                except Exception as err:
                    colname = f"articles-nodate"
                    print(f"Date parsing issue: {err}")

            try:
                self.db[colname].update_one(
                    {'_id': doc['_id']},
                    {
                        '$set': {
                            f'{self.model_name}': {
                                'env_max': self.top1_labels[nn],
                                'env_sec': self.top2_labels[nn],
                                'model_outputs': self.all_model_outputs[nn]
                            }
                        }
                    }
                )
            except Exception as err:
                print(f"Failed updating doc {doc['_id']}: {err}")

    def clear_queue(self):
        self.queue = []
        self.top1_labels = []
        self.top2_labels = []
        self.all_model_outputs = []

    def run(self):
        self.get_db_info()
        self.check_index()
        self.load_model()

        dates = pd.date_range(
            '2012-1-1',
            datetime.now() + relativedelta(months=1),
            freq='M'
        )

        # Outer progress bar: each month
        for date in tqdm(dates, desc="Monthly Collections"):
            try:
                self.generate_cursor(date)
                data_month = list(self.cursor)
                total_count = len(data_month)

                if total_count == 0:
                    print(f'No Articles to Update for {date}')
                    continue

                self.queue = []

                # Inner progress bar: docs in this month
                for doc in tqdm(data_month, desc=f"Classifying docs in {date}", leave=False):
                    self.queue.append(doc)
                    # Classify in chunks of batch_size*10
                    if len(self.queue) >= (self.batch_size * 10):
                        self.classify_articles()
                        self.insert_info()
                        self.clear_queue()

                # leftover
                if len(self.queue) > 0:
                    self.classify_articles()
                    self.insert_info()
                    self.clear_queue()

            except Exception as err:
                print("Error:", err)
                pass

if __name__ == "__main__":
    classify_pipe(
        'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true',
        'env_classifier',
        '/home/diego/peace/modernbert_models',
        128
    )

    commit_message = "ModernBERT classifier deployment update"
    run_git_commands(commit_message)
