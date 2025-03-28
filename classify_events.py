import os
from datetime import datetime
import os
from pathlib import Path
import pandas as pd
from dateutil.relativedelta import relativedelta
from simpletransformers.classification import ClassificationModel
import re
import dateparser
from pymongo import MongoClient

from helpers import build_combined 
from multiprocessing import Pool
import subprocess

_ROOT = Path(os.path.abspath(os.path.dirname(__file__))).as_posix()



def get_data_path(path):
    return Path('/'.join(_ROOT.split('/')[:-1]), 'data', path).joinpath()

def classify_pipe(uri, model_name, model_location, batch_size):
    evntclass = EventClassifier(uri, model_name, model_location, batch_size, n_gpu=1)
    evntclass.run()

def run_git_commands(commit_message):
    try:
        # Add only Python files using shell globbing
        subprocess.run("git add *.py", shell=True, check=True)
        # Commit changes with a message
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        # Push changes to the repository
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

    def get_db_info(self):
        """
        gets the info needed from the db
        """
        self.db = MongoClient(self.uri).ml4p # TODO: integrate db into uri
        self.model_info = self.db.models.find_one({'model_name': self.model_name})
        self.label_dict = {v:k for k,v in self.model_info.get('event_type_nums').items()}

    def load_model(self):
        """
        load the model
        TODO: convert to base transformers
        """
        self.model = ClassificationModel('roberta'
                            , f'{self.model_location}/{self.model_name}'
                            # , num_labels=18
                            , args={
                                'n_gpu': self.n_gpu
                                , 'eval_batch_size':self.batch_size}
                                ,use_cuda = False)

    def generate_cursor(self, date):
        """
        creates the cursor for finding articles to process
        """
        colname = f'articles-{date.year}-{date.month}'
        print("Colname", colname)
        source_domains = self.db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['ENV_BLR','ENV_BFA','ENV_ALB','ENV_AGO','ENV_NGA','ENV_SLV','ENV_BEN','ENV_PAK','ENV_HND']}})
        # source_domains = ['divergentes.com', 'revistafactum.com', 'alharaca.sv']
        # source_domains = self.db.sources.distinct('source_domain', filter={'include' : True, 'major_international' : True})
        # source_domains += self.db.sources.distinct('source_domain', filter={'include' : True, 'major_regional' : True})
        # source_domains = self.db.sources.distinct('source_domain', filter={'include' : True})


        
        self.cursor = self.db[colname].find(
                { 
                    self.model_name: {'$exists': False},
                    # # in the proper language
                    'language_translated': 'en',
                    # meant to be used
                    # 'include': True, 
                    
                    # has a title
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
                    #'language':{'$nin':['hi','ha']}
                    'date_publish':{
                        '$exists': True,
                        '$ne': '',
                        '$ne': None},
                        # 'civic_new': None
                }
                # ,batch_size = 1
            )
            

    def check_index(self):
        """
        checks to make sure that the model_name is indexed
        """
        indexes = [list(ll['key'].keys())[0] for ll in self.db.articles.list_indexes()]
        if self.model_name not in indexes:
            self.db.articles.create_index([(self.model_name, 1)], background=True)

    
    def zip_outputs(self, outputs):
        _mo = []

        for oo in outputs:
            _mo.append({self.label_dict[nn]: float(mo) for nn, mo in enumerate(oo)})

        return _mo


    def classify_articles(self):
        """
        function for classifying the articles in the queue
        """
        # coup_re = re.compile(r'(coup|depose|overthrow|oust)')
        
        
        # do the predictions
        texts = [build_combined(doc) for doc in self.queue]
        
        preds, self.model_outputs = self.model.predict(texts)
        
        
        # modify outputs
        model_max = [float(max(mo)) for mo in self.model_outputs] 
        # print(model_max)
        # print(len(model_max))
        self.event_types = [self.label_dict[ii] for ii in preds]
        
        # print()
        # apply the cutoff
        for ii in range(len(model_max)):
            _et = self.event_types[ii]
            if self.model_name == 'civic1' and model_max[ii] < self.model_info['event_type_cutoffs'][_et]:
                self.event_types[ii] = '-999'
            # if self.model_name == 'civic1' and self.event_types[ii] != '-999' and coup_re.search(texts[ii]):
            #     self.event_types[ii] = 'coup'
            # if self.model_name == 'civic_new' and self.event_types[ii] != '-999' and coup_re.search(texts[ii]):
            #     self.event_types[ii] = 'coup'
            if self.model_name == 'RAI' and model_max[ii] < self.model_info['event_type_cutoffs'][_et]:
                self.event_types[ii] = '-999'
        # zip up the model outputs
        self.model_outputs = self.zip_outputs(self.model_outputs)


    def insert_info(self):
        """
        inserts the docs into the db
        """
        for nn, _doc in enumerate(self.queue):
            try:
                colname = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
            except:
                try:
                    date = dateparser.parse(_doc['date_publish']).replace(tzinfo = None)
                    colname = f"articles-{date.year}-{date.month}"
                except Exception as err:
                    colname = f"articles-nodate"
                    print('date issues- {err}')

            try:
            # print(self.event_types[nn])
                self.db[colname].update_one(
                    {
                        '_id': _doc['_id']
                    },
                    {
                        '$set':{
                            #f'event_type_{self.model_name}': self.event_types[nn],
                            f'{self.model_name}': {
                                'event_type': self.event_types[nn],
                                'model_outputs': self.model_outputs[nn]
                            }
                        }
                    }
                )
            except Exception as err:
                print(f'Failed updating --- {err}')

    def clear_queue(self):
        self.queue = []
        del self.event_types
        del self.model_outputs

    def process_article_data(self, doc):
        """
        Process the individual article data
        """
        try:
            # Directly process the doc here, without passing the entire model or DB object to workers
            self.queue.append(doc)
            if len(self.queue) >= (self.batch_size * 10):
                self.classify_articles()
                self.insert_info()
                self.clear_queue()
        except Exception as err:
            print(f"Error processing doc: {doc['_id']} - {err}")


    def run(self):
        self.get_db_info()
        self.check_index()
        self.load_model()

        dates = pd.date_range('2012-1-1', datetime.now()+relativedelta(months=1), freq='M') #'2020-12-1', freq='M')#datetime.now()+relativedelta(months=1), freq='M')
        for date in dates:

            try:  
                self.generate_cursor(date)
                # TODO: separate processes for queue and processing
                self.queue = []
                data_month = [doc for doc in self.cursor]
                
                if len(data_month)==0:
                    print('No Articles to Be Updated')
                    continue
                
                for doc in data_month:
                    self.queue.append(doc)

                    if len(self.queue) >= (self.batch_size * 10): 
                        self.classify_articles()
                        self.insert_info()
                        self.clear_queue()
                    

                self.classify_articles()
                self.insert_info()
                self.clear_queue()
            except Exception as err:
                print("Error!", err)
                pass

        
 
if __name__ == "__main__":
    
    classify_pipe('mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true', 'RAI', '/home/diego/peace/finetuned-transformers', 128)
    # civic_new
    # RAI
    # Git operations
    commit_message = "civic/rai classfier update"
    run_git_commands(commit_message)
    