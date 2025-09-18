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
BASE_MODEL_ID = "answerdotai/ModernBERT-large"          # tokenizer base
MODEL_ROOT = "/home/diego/peace/modernbert_models"      # where model folders live
MAX_LEN = 512                                           # keep consistent with training

def classify_pipe(uri, model_name, model_location, batch_size):
    runner = EnvPipeline(uri, model_name, model_location, batch_size, n_gpu=1)
    runner.run()

def run_git_commands(commit_message):
    try:
        subprocess.run("git add *.py", shell=True, check=True)
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        subprocess.run(["git", "push"], check=True)
        print("Git commands executed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running Git commands: {e}")

class EnvPipeline:
    """
    Unified environmental inference pipeline:
      - model_name='env_binary'      -> writes to field 'environmental_binary' (result + model_outputs)
      - model_name='env_classifier'  -> writes to field 'env_classifier' (env_max + env_sec + model_outputs)
    """
    def __init__(self, uri, model_name, model_location, batch_size, n_gpu=1):
        self.uri = uri
        self.model_name = model_name                  # 'env_binary' or 'env_classifier'
        self.model_location = model_location          # '/home/diego/peace/modernbert_models'
        self.batch_size = batch_size
        self.n_gpu = n_gpu
        self.device = "cuda" if (torch.cuda.is_available() and n_gpu > 0) else "cpu"

        self.is_binary = (self.model_name == "env_binary")
        self.is_event = (self.model_name == "env_classifier")

        # Where to write in Mongo:
        #   - keep 'environmental_binary' for the binary model (script2 relies on this)
        #   - write to 'env_classifier' for event model
        self.target_field = "environmental_binary" if self.is_binary else self.model_name

    # --------- DB & Model ----------
    def get_db_info(self):
        """
        Fetch info needed from the DB and set up label_dict for label index -> label name.
        Uses db.models[event_type_nums] mapping for this model_name.
        """
        self.db = MongoClient(self.uri).ml4p
        self.model_info = self.db.models.find_one({'model_name': self.model_name})
        if not self.model_info or 'event_type_nums' not in self.model_info:
            raise RuntimeError(f"Could not find mapping for model_name={self.model_name} in db.models")

        # DB mapping is {'label': idx}; we need idx->label for decoding predictions
        self.label_dict = {v: k for k, v in self.model_info['event_type_nums'].items()}

    def load_model(self):
        """
        Load tokenizer from base HF id and the fine-tuned model from local folder.
        """
        model_path = Path(self.model_location) / self.model_name
        mp = model_path.as_posix()
        print(f"[LOAD] {self.model_name} from: {mp}")

        self.tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL_ID, trust_remote_code=True)
        self.model = AutoModelForSequenceClassification.from_pretrained(mp, trust_remote_code=True)
        self.model.to(self.device)
        self.model.eval()

    # --------- Data Cursor ----------
    def generate_cursor(self, date):
        """
        Create a Mongo cursor for monthly collection:
          - Common conditions (English, non-empty title/maintext, date, allowed sources)
          - Only docs that do NOT already have the target_field
          - For env_classifier: require environmental_binary.result == 'Yes'
        """
        colname = f'articles-{date.year}-{date.month}'
        print(f"[COL] {colname}")

        source_domains = self.db.sources.distinct(
            'source_domain',
            filter={
                'include': True,
                'primary_location': {'$in': ['MEX','LBR','MDA','SRB','LKA','KGZ','PHL']}
            }
        )

        # missing-field check depends on where we write:
        missing_field = self.target_field  # 'environmental_binary' or 'env_classifier'

        base_filter = {
            missing_field: {'$exists': False},
            'language_translated': 'en',
            'title_translated': {'$exists': True, '$ne': '', '$ne': None, '$type': 'string'},
            'maintext_translated': {'$exists': True, '$ne': '', '$ne': None, '$type': 'string'},
            'source_domain': {'$in': source_domains},
            'date_publish': {'$exists': True, '$ne': '', '$ne': None}
        }

        if self.is_event:
            base_filter.update({'environmental_binary.result': 'Yes'})

        self.current_collection = colname
        self.cursor = self.db[colname].find(base_filter)

    # --------- Inference ----------
    def classify_binary(self):
        """
        env_binary: store top1 label and full distribution.
        """
        texts = [build_combined(doc) for doc in self.queue]
        inputs = self.tokenizer(
            texts, padding=True, truncation=True, max_length=MAX_LEN, return_tensors="pt"
        ).to(self.device)

        with torch.no_grad():
            outputs = self.model(**inputs)
            probs = torch.softmax(outputs.logits, dim=-1).cpu().numpy()

        self.results = []
        for row in probs:
            top_idx = int(np.argmax(row))
            top_label = self.label_dict[top_idx]
            label_scores = {self.label_dict[i]: float(row[i]) for i in range(len(row))}
            self.results.append({'result': top_label, 'model_outputs': label_scores})

    def classify_event(self):
        """
        env_classifier: store top1 (env_max), top2 (env_sec if prob>0.30), and full distribution.
        """
        texts = [build_combined(doc) for doc in self.queue]
        inputs = self.tokenizer(
            texts, padding=True, truncation=True, max_length=MAX_LEN, return_tensors="pt"
        ).to(self.device)

        with torch.no_grad():
            outputs = self.model(**inputs)
            probs = torch.softmax(outputs.logits, dim=-1).cpu().numpy()

        self.results = []
        for row in probs:
            sorted_idx = np.argsort(row)  # ascending
            top1_idx, top2_idx = int(sorted_idx[-1]), int(sorted_idx[-2])
            top1_prob, top2_prob = float(row[top1_idx]), float(row[top2_idx])
            env_max = self.label_dict[top1_idx]
            env_sec = self.label_dict[top2_idx] if top2_prob > 0.30 else None
            label_scores = {self.label_dict[i]: float(row[i]) for i in range(len(row))}
            self.results.append({'env_max': env_max, 'env_sec': env_sec, 'model_outputs': label_scores})

    # --------- Write Back ----------
    def insert_info(self):
        """
        Update the DB with either:
          - environmental_binary: { result, model_outputs }
          - env_classifier:       { env_max, env_sec, model_outputs }
        """
        for nn, doc in enumerate(self.queue):
            # pick collection robustly from the doc
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
                    {'$set': { self.target_field: self.results[nn] }}
                )
            except Exception as err:
                print(f"[ERROR] Failed updating doc {doc.get('_id')}: {err}")

    # --------- Orchestration ----------
    def clear_queue(self):
        self.queue = []
        self.results = []

    def run(self):
        self.get_db_info()
        self.load_model()

        dates = pd.date_range('2012-1-1', datetime.now() + relativedelta(months=1), freq='M')

        for date in tqdm(dates, desc=f"Monthly Collections :: {self.model_name}"):
            try:
                self.generate_cursor(date)
                docs = list(self.cursor)
                if not docs:
                    print(f'({self.model_name})No Articles to Update for {date}')
                    continue

                self.queue = []
                for doc in tqdm(docs, desc=f"({self.model_name})Classifying docs in {date}", leave=False):
                    self.queue.append(doc)

                    # classify in chunks
                    if len(self.queue) >= (self.batch_size * 10):
                        if self.is_binary:
                            self.classify_binary()
                        else:
                            self.classify_event()
                        self.insert_info()
                        self.clear_queue()

                # leftovers
                if self.queue:
                    if self.is_binary:
                        self.classify_binary()
                    else:
                        self.classify_event()
                    self.insert_info()
                    self.clear_queue()

            except Exception as err:
                print("[ERROR]", err)
                continue

# ----------------- Entrypoint -----------------
if __name__ == "__main__":
    mongo_uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'

    # 1) Binary environmental relevance
    classify_pipe(mongo_uri, 'env_binary', MODEL_ROOT, batch_size=32)

    # 2) Environmental event classifier (requires environmental_binary.result == 'Yes')
    classify_pipe(mongo_uri, 'env_classifier', MODEL_ROOT, batch_size=32)

    # Optional: commit a friendly message (adjust as needed)
    run_git_commands("Merged env_binary + env_classifier ModernBERT inference into single unified script")
