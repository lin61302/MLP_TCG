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
from pymongo import ASCENDING
import dateparser
uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'


db = MongoClient(uri).ml4p
today = pd.Timestamp.now()
load_dotenv()
#uri = os.getenv('DATABASE_URL')




def save_file(docs, year, month, language):
    df = pd.DataFrame(docs)
    df.to_csv(f'/home/diego/peace/tweet_files/{language}_{year}_{month}.csv')
    print("Saving ", f'{language}_{year}_{month}.csv')



dates = pd.date_range('2022-3-1','2023-1-20', freq='D')#datetime.now()+relativedelta(days=1)
# doc_t = []
total = 0
month = dates[0].month
languages = ["ukrainian", "russian"]
docs = []
for language in languages:
    for date in dates:
        year = date.year
        if month != date.month:
            if len(docs)==0:
                continue
            else:
                save_file(docs, year, month, language)
                docs = []
            
        month = date.month
            
        colname = f"tweets-{date.year}-{date.month}-{date.day}-{language}"
        cur = db[colname].find({})
        doc = [dd for dd in cur]
        if len(doc) == 0:
            continue
        else:
            docs += doc
            print(f"{date}: {len(doc)}/{len(docs)} ")