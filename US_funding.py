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
from tqdm import tqdm

# ----------------- Config -----------------
_ROOT = Path(os.path.abspath(os.path.dirname(__file__))).as_posix()
BASE_MODEL_ID = "answerdotai/ModernBERT-large"   # tokenizer base
MODEL_ROOT = "/home/diego/peace/modernbert_models"  # where model folders live
MAX_LEN = 512

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
        self.model_name = model_name  # e.g., 'US_funding_relevance' | 'US_funding_event' | 'US_funding_sentiment'
        self.model_location = model_location  # e.g., '/home/diego/peace/modernbert_models'
        self.batch_size = batch_size
        self.n_gpu = n_gpu
        self.device = "cuda" if (torch.cuda.is_available() and n_gpu > 0) else "cpu"
        self.is_event = (self.model_name == "US_funding_event")
        self.is_sentiment = (self.model_name == "US_funding_sentiment")
        self.is_relevance = (self.model_name == "US_funding_relevance")

    def get_db_info(self):
        """
        Fetch info needed from the DB and set up label_dict for label index -> label name.
        """
        self.db = MongoClient(self.uri).ml4p
        self.model_info = self.db.models.find_one({'model_name': self.model_name})
        if not self.model_info or 'event_type_nums' not in self.model_info:
            raise RuntimeError(f"Could not find mapping for model_name={self.model_name} in db.models")

        # DB stores {"label": idx}; we need idx->label for writing predictions by label name
        self.label_dict = {v: k for k, v in self.model_info['event_type_nums'].items()}

    def load_model(self):
        """
        Load the tokenizer and model from the specified directory.
        """
        model_path = Path(self.model_location) / self.model_name
        model_path_str = model_path.as_posix()
        print(f"Loading model from: {model_path_str}")

        self.tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL_ID, trust_remote_code=True)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_path_str, trust_remote_code=True)
        self.model.to(self.device)
        self.model.eval()

    def generate_cursor(self, date):
        """
        Create a Mongo cursor for articles in the monthly collection that do NOT have self.model_name set.
        For event/sentiment: only pick docs already marked relevant by US_funding_relevance.result == 'Yes'.
        """
        colname = f'articles-{date.year}-{date.month}'
        print(f"Processing collection: {colname}")

        # you can revise this source filter as needed
        source_domains = self.db.sources.distinct(
            'source_domain',
            filter={
                'include': True,
                'primary_location': {'$in': [
                                            # 'MEX','LBR','MDA','SRB','LKA','KGZ','PHL'
                                            # 'MLI','ARM','SLV','ZMB','UGA'
                                            # 'MOZ','COD','SSD','ZWE','GHA','KHM'
                                            # 'BEN', 'UKR', 'GEO', 'GTM','NIC', 'PRY'
                                            # 'MEX','LBR','MDA','SRB','LKA','KGZ','PHL'
                                            # 'UZB','DOM','BLR','AGO','XKX','ALB','MKD','BFA','CMR'
                                            # 'ETH'
                                            # 'KAZ','MWI','MRT','JAM','NAM','NGA','MYS'
                                            'MAR','NER','PAK','NPL'
                                             
                                             ]}
            }
        )
        # source_domains = self.db.sources.distinct('source_domain', filter={'include' : True, 'major_international' : True})
        # source_domains += self.db.sources.distinct('source_domain', filter={'include' : True, 'major_regional' : True})

        base_filter = {
            self.model_name: {'$exists': False},
            'language_translated': 'en',
            'title_translated': {'$exists': True, '$ne': '', '$ne': None, '$type': 'string'},
            'maintext_translated': {'$exists': True, '$ne': '', '$ne': None, '$type': 'string'},
            'source_domain': {'$in': source_domains},
            'date_publish': {'$exists': True, '$ne': '', '$ne': None}
        }

        # event & sentiment only run on relevance-Yes
        if self.is_event or self.is_sentiment:
            base_filter.update({'US_funding_relevance.result': 'Yes'})

        self.current_collection = colname
        self.cursor = self.db[colname].find(base_filter)

    def classify_articles(self):
        """
        Run classification and store only the top predicted label (+ per-class probabilities).
        """
        texts = [build_combined(doc) for doc in self.queue]

        inputs = self.tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=MAX_LEN,
            return_tensors="pt"
        ).to(self.device)

        with torch.no_grad():
            outputs = self.model(**inputs)
            logits = outputs.logits
            probs = torch.softmax(logits, dim=-1).cpu().numpy()

        self.top_labels = []
        self.all_model_outputs = []

        for row in probs:
            top_idx = int(np.argmax(row))
            top_label = self.label_dict[top_idx]  # map id->label using DB mapping

            label_scores = {self.label_dict[i]: float(row[i]) for i in range(len(row))}
            self.top_labels.append(top_label)
            self.all_model_outputs.append(label_scores)

    def insert_info(self):
        """
        Update the DB with <self.model_name> result and model outputs.
        """
        for nn, doc in enumerate(self.queue):
            # pick collection from doc.date_publish to be safe
            try:
                colname = f"articles-{doc['date_publish'].year}-{doc['date_publish'].month}"
            except Exception:
                try:
                    parsed_dt = dateparser.parse(str(doc.get('date_publish'))).replace(tzinfo=None)
                    colname = f"articles-{parsed_dt.year}-{parsed_dt.month}"
                except Exception as err:
                    colname = "articles-nodate"
                    print(f"[WARN] Date parsing issue for _id={doc.get('_id')}: {err}")

            try:
                self.db[colname].update_one(
                    {'_id': doc['_id']},
                    {
                        '$set': {
                            self.model_name: {
                                'result': self.top_labels[nn],
                                'model_outputs': self.all_model_outputs[nn]
                            }
                        }
                    }
                )
            except Exception as err:
                print(f"[ERROR] Failed updating doc {doc.get('_id')}: {err}")

    def clear_queue(self):
        self.queue = []
        self.top_labels = []
        self.all_model_outputs = []

    def run(self):
        self.get_db_info()
        self.load_model()

        dates = pd.date_range('2012-1-1', datetime.now() + relativedelta(months=1), freq='M')

        for date in tqdm(dates, desc=f"Processing Monthly Collections for {self.model_name}"):
            try:
                self.generate_cursor(date)
                data_month = list(self.cursor)
                total_count = len(data_month)

                if total_count == 0:
                    print(f'No Articles to Update for {date}')
                    continue

                self.queue = []
                for doc in tqdm(data_month, desc=f"Classifying docs in {date}", leave=False):
                    self.queue.append(doc)

                    if len(self.queue) >= (self.batch_size * 10):
                        self.classify_articles()
                        self.insert_info()
                        self.clear_queue()

                if len(self.queue) > 0:
                    self.classify_articles()
                    self.insert_info()
                    self.clear_queue()

            except Exception as err:
                print("Error:", err)
                continue

# ----------------- Entrypoint -----------------
if __name__ == "__main__":
    # Choose which model to run by uncommenting one classify_pipe call at a time.
    # All of these write back to fields named exactly as the model_name, e.g. "US_funding_event".
    mongo_uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'

    # 1) Relevance (binary; 'No'/'Yes' from DB mapping)
    classify_pipe(mongo_uri, 'US_funding_relevance', MODEL_ROOT, batch_size=32)

    # 2) Event (4-way; only runs where US_funding_relevance.result == 'Yes')
    classify_pipe(mongo_uri, 'US_funding_event', MODEL_ROOT, batch_size=32)

    # 3) Sentiment (3-way; only runs where US_funding_relevance.result == 'Yes')
    classify_pipe(mongo_uri, 'US_funding_sentiment', MODEL_ROOT, batch_size=32)

    # Optional: commit with a model-specific message
    commit_message = "Update ModernBERT inference for US_funding_* (writes to field named after model)"
    run_git_commands(commit_message)
