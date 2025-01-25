'''
created on 2024-5-14
zung-ru
'''
import os
import getpass
from dateutil.relativedelta import relativedelta
import pandas as pd
import os
from pathlib import Path
import re
import pandas as pd
import time
from dotenv import load_dotenv
from pymongo import MongoClient
import requests
import pandas as pd
import os
import re
from random import randint
from time import sleep
import sys
import time
from tqdm import tqdm
from pymongo import MongoClient
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse
#import torch
#from transformers import MarianMTModel, MarianTokenizer
from pymongo import MongoClient
import nltk
import six
import json
import random
from cliff.api import Cliff
import multiprocessing


uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
db = MongoClient(uri).ml4p

dates = pd.date_range('2012-1-1', datetime.now()+relativedelta(months=1), freq='M')

for dt in dates:
    collection_name = f'articles-{dt.year}-{dt.month}'
    collection = db[collection_name]
    
    # Define the query
    query = {
        'cliff_locations.AGO': ['Republic of Angola'],
        '$and': [
            {
                '$or': [
                    {'title_translated': {'$regex': 'West Africa([^A-Za-z]|$)', '$options': 'i'}},
                    {'maintext_translated': {'$regex': 'West Africa([^A-Za-z]|$)', '$options': 'i'}}
                ]
            },
            {
                '$nor': [
                    {'title_translated': {'$regex': 'Angola', '$options': 'i'}},
                    {'maintext_translated': {'$regex': 'Angola', '$options': 'i'}}
                ]
            }
        ]
    }

    # Perform the update
    update_result = collection.update_many(
        query,
        [
            {
                '$set': {
                    'cliff_locations.None': {
                        '$cond': {
                            'if': {'$isArray': '$cliff_locations.None'},
                            'then': {
                                '$setUnion': ['$cliff_locations.None', ['West Africa']]
                            },
                            'else': ['West Africa']
                        }
                    }
                }
            },
            {
                '$unset': 'cliff_locations.AGO'  # remove the 'AGO' field
            }
        ]
    )


    print(f'Collection: {collection_name} - Modified documents: {update_result.modified_count}')

