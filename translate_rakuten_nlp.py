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
# from transformers import MarianMTModel, MarianTokenizer
from pymongo import MongoClient
import nltk
import six

today = pd.Timestamp.now()

uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
db = MongoClient(uri).ml4p

def pull_data(colname, src, lan):
    docs = []
    cursor = db[colname].find(
        {
            'source_domain': {'$in': src},
            'include': True,
            'language': lan,
            'title': {'$type': 'string', '$ne': '', '$not': {'$type': 'null', '$eq': None}},
            'maintext': {'$type': 'string', '$ne': '', '$not': {'$type': 'null', '$eq': None, '$type': 'number'}}
        }
    )
    docs = [doc for doc in cursor]
    for i, doc in enumerate(docs):
        try:
            s = docs[i]['maintext'].replace('\n', '')
            s = s.replace('"', '')
            s = s.replace('“', '')
            docs[i]['maintext'] = s[:800]  # Limit to first 800 characters
        except:
            pass

        try:
            t = docs[i]['title'].replace('\n', '')
            t = t.replace('"', '')
            t = t.replace('“', '')
            docs[i]['title'] = t[:800]  # Limit to first 800 characters
        except:
            pass

    return docs

def translate_text(lan, text):
    url = "https://nlp-translation.p.rapidapi.com/v1/translate"

    headers = {
        'x-rapidapi-key': "e530af9f1dmsh2838648ebeddfb5p1acd2bjsn657d52d899a7",
        'x-rapidapi-host': "nlp-translation.p.rapidapi.com"
    }

    try:
        text = text.strip()
    except:
        pass

    try:
        payload = {"text": text, "to": "en", "from": lan}
        response = requests.request("GET", url, headers=headers, params=payload)
        print(response.text)
        translated_text = re.findall('"translated_text":{"en":"(.*)"},', response.text)[0]
    except Exception as err:
        print(err)
        translated_text = None

    try:
        translated_text = translated_text.replace('\\', '')
    except Exception as err:
        print(err)
        pass

    return translated_text

def main():

    uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'

    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='M')
    df.index = df['date']
    df['year'] = [dd.year for dd in df.index]
    df['month'] = [dd.month for dd in df.index]

    lan = 'tet'
    src = db.sources.distinct('source_domain', filter={'include': True, 'primary_location': {'$in': ['TLS']}})
    print(src)

    for dt in df.index:
        colname = f'articles-{dt.year}-{dt.month}'
        docs = pull_data(colname=colname, src=src, lan=lan)
        title_translated = []
        maintext_translated = []
        print(colname, ':', len(docs))
        for i, doc in enumerate(docs):
            try:
                trans_title = translate_text(lan=lan, text=docs[i]['title'])
                title_translated.append(trans_title)
                print(colname, '(title):', trans_title, '---', i, '/', len(docs))
            except Exception as err:
                print(err)
                title_translated.append(None)
                pass

            try:
                trans_maintext = translate_text(lan=lan, text=docs[i]['maintext'][:800])
                maintext_translated.append(trans_maintext)
                print(colname, '(maintext):', trans_maintext, '---', i, '/', len(docs))
            except Exception as err:
                print(err)
                maintext_translated.append(None)
                pass

        for i, doc in enumerate(docs):
            try:
                if title_translated[i] is None or maintext_translated[i] is None:
                    print('no maintext or title translated, no upload, continue')
                    continue

                db[colname].update_one(
                    {'_id': doc['_id']},
                    {
                        '$set': {
                            'language_translated': 'en',
                            'title_translated': title_translated[i],
                            'maintext_translated': maintext_translated[i]
                        }
                    }
                )
                print(colname, title_translated[i], maintext_translated[i])
            except Exception as err:
                print(err)

main()

#KAZ kk to 2017-9
