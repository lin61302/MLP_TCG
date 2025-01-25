import os
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from pathlib import Path
import pandas as pd
from dateutil.relativedelta import relativedelta
from simpletransformers.classification import ClassificationModel
import re
import dateparser
from pymongo import MongoClient
uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
today = pd.Timestamp.now()
db = MongoClient(uri).ml4p
df = pd.DataFrame()
df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd') , freq='M')
df.index = df['date']
df['year'] = [dd.year for dd in df.index]
df['month'] = [dd.month for dd in df.index]
ss=0
ss2=0
queue = []
print(df.index)
count=0
#docs=[]
for dt in df.index:
    colname = f'articles-{dt.year}-{dt.month}'
    cur = db[colname].find({}).count()
    cur2 = db[colname].find({'include':True}).count()
    ss+=cur
    ss2+=cur2
    print(colname,': ',cur,' ,Total: ',ss, "  Total(included): ", ss2)