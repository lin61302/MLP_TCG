from deep_translator import GoogleTranslator
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
import torch
from transformers import MarianMTModel, MarianTokenizer
from pymongo import MongoClient
import nltk
import six


today = pd.Timestamp.now()

# url = "https://nlp-translation.p.rapidapi.com/v1/translate"

# headers = {
#     'x-rapidapi-host': "nlp-translation.p.rapidapi.com",
#     'x-rapidapi-key': "bdc32820ffmsh588cc1f65cabc7ep195c0fjsna6396a971023"
#     }

uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
# today = pd.Timestamp.now()
db = MongoClient(uri).ml4p


def pull_data(colname, src, lan):
    docs=[]
    cursor =  db[colname].find(        
        {
            'source_domain': {'$in':src},
            'include' : True,
            'language':lan,
            'title' :{'$type': 'string'},
            'title' : {'$not': {'$type': 'null'}},
            'title' : {'$not': {'$eq':None}},
            'title': {'$ne': ''},
            
            'maintext' :{'$type': 'string'},
            'maintext': {'$ne': ''},
            'maintext' : {'$not': {'$type': 'null'}},
            'maintext' : {'$not': {'$eq':None}},
            'maintext' : {'$not': {'$type': 'number'}},
            '$or':[
            {'title_translated': {'$exists': False}},
            {'maintext_translated': {'$exists': False}}
            ]
        }
    )
    docs2 = [doc for doc in cursor]
    docs=[]
    for doc in docs2:
        try:
            if len(doc['maintext'].strip())>5 and len(doc['title'].strip())>3:
                docs.append(doc)
        except:
            pass
    for i, doc in enumerate(docs):
        try:
            s = docs[i]['maintext'].replace('\n', '')
            s = docs[i]['maintext'].replace('"', '')
            s = docs[i]['maintext'].replace('“', '')
            docs[i]['maintext'] = s[:600]
        except:
            pass
    return docs

def translate_text(lan, list_text):
    
    

    try:
        list_text = [text.strip() for text in list_text]
    except:
        pass

    max_trial = 2
    while max_trial>0:
        try:
            translated_text = GoogleTranslator(source=lan, target='en').translate_batch(list_text)
        except Exception as err:
            print(err)
            # translated_text = None   
        try:
            if translated_text:
                break
        except:
            max_trial -= 1
            pass

    try:
        translated_text = [text.replace('\\', '') for text in translated_text]
    except:
        pass

    return translated_text

# Use any translator you like, in this example GoogleTranslator
# output -> Weiter so, du bist großartig
def split_list(list_to_split, batch_size=500):
    length = len(list_to_split)
    wanted_parts= (length//batch_size)+1
    return [ list_to_split[i*length // wanted_parts: (i+1)*length // wanted_parts] for i in range(wanted_parts) ] 


def main():

    uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'

    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd') , freq='M') # , '2020-1-30', freq='M')#
    df.index = df['date']
    df['year'] = [dd.year for dd in df.index]
    df['month'] = [dd.month for dd in df.index]


    
    lan = 'uz'
    #lan = az, uz, ka, ne, sw, sr, am
    # src = ['english.onlinekhabar.com', 'en.setopati.com', 'thehimalayantimes.com', 'kathmandupost.com', 'nepalitimes.com']
    src = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['UZB']}})
    # src = db.sources.distinct('source_domain', filter={'include' : True, 'major_regional' : True})
    print(src)

    


    for dt in df.index:
        colname = f'articles-{dt.year}-{dt.month}'
        docs = pull_data(colname=colname, src=src, lan=lan)
        print(colname,':',len(docs))
        if len(docs)==0:
            continue
        batch_size = 30
        batched_lists = split_list(docs, batch_size = batch_size)

        
        for index, batch in enumerate(batched_lists):

            # try:
            list_title = [doc['title'] for doc in batch]
            list_maintext = [doc['maintext'] for doc in batch]
            
            
            list_trans_title = translate_text(lan=lan, list_text=list_title)
            
            if len(batch)>1:
                print(f"\n{colname} (Batch {index+1}/{len(batched_lists)}) \n1st: (title) {list_trans_title[1]} \nlast: (title) {list_trans_title[-1]}\n")
            list_trans_maintext = translate_text(lan=lan, list_text=list_maintext)
            if len(batch)>1:
                print(f"\n{colname} (Batch {index+1}/{len(batched_lists)}) \n1st: (maintext) {list_trans_maintext[1][:200]} \nlast: (maintext) {list_trans_maintext[-1][:200]}\n")
            # if len(batch)>1:
                # print(f"{colname} (Batch {index}/{len(batched_lists)}) \n1st: (maintext) {list_trans_maintext[1]} \nlast: (maintext) {list_trans_maintext[-1]}")

            if len(list_title)!=len(list_trans_title) or len(list_maintext)!=len(list_trans_maintext):
                print('length not match, break')
                break

                
                
            # except Exception as err:
            #     print(err)
            #     pass
            
            for i, doc in enumerate(batch):
                
                try:
                    if list_trans_title[i]==None or list_trans_maintext[i]==None:
                        print('no maintext or title translated, no upload, continue')
                        continue

                    db[colname].update_one(
                        {'_id': doc['_id']},
                        {
                        '$set': {
                            'language_translated': 'en',
                            'title_translated': list_trans_title[i],
                            'maintext_translated': list_trans_maintext[i]
                        }
                    }
                    )
                    
                except Exception as err: 
                    print(err)
                    pass
            print('\n',colname, f'(Batch {index+1}/{len(batched_lists)}) (title)', list_trans_title[i][:50], '\n(maintext)',list_trans_maintext[i][:50],'\n')
        
                

main()
