'''
created on 2021-12-1
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
import pycountry
import random
from cliff.api import Cliff
import multiprocessing




def location_pipeline(mongo_uri, batch_size, sources, my_cliff):
    db = MongoClient(uri).ml4p
    locs = Clifftag(mongo_uri=uri, batch_size=batch_size, sources=sources, my_cliff=my_cliff)
    locs.run()




class Clifftag:

    def __init__(self, mongo_uri, batch_size, sources, my_cliff):
        """
        :param mongo_uri: the uri for the db
        :param batch_size: run one batch size at a time (geoparse, update)
        :param sources: source domains of interest
        :param my_cliff: Cliff api server url
        """
        self.mongo_uri = mongo_uri
        self.batch_size = batch_size
        self.sources = sources
        self.db = MongoClient(mongo_uri).ml4p
        self.my_cliff = my_cliff 

    def pull_data(self, date):
        self.colname = f'articles-{date.year}-{date.month}'
        cursor = self.db[self.colname].find(
            {
                #'id': '60882a63f8bb748cadce4bf0'
                'source_domain': {'$in':self.sources},
                'include': True,
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
                'coord_cliff_locations':{'$exists':False}
                # 'title_translated':{'$regex': "Iran's supreme"},
                
                
                #'mordecai_locations.' + country_code : {'$exists' : True}
                #'mordecai_locations':{'$exists':True}
            }
        )
        docs = [doc for doc in cursor]
        return docs

    def split_list(self, list_to_split):
        '''
        batchify monthly data into multiple lists
        '''
        length = len(list_to_split)
        wanted_parts= (length//self.batch_size)+1

        return [ list_to_split[i*length // wanted_parts: (i+1)*length // wanted_parts] 
             for i in range(wanted_parts) ]

    def fix_text(self, text):
        '''
        fix confusing marks and signs
        '''
        try:
            text = text.replace('\n', '')
            text = text.replace('“', "")
            text = text.replace('"', "")
            # text = text.replace("'", "")
            text = text.replace("”", "")
        except:
            print('Error in fixing/ replacing text')
        
        return text
    
    def combine_text(self, title, text):
        '''
        combine title with maintext
        '''
        f_text = ''
        try:
            if title and text:
                f_text += title + ". " + text
            elif text:
                f_text += text
            elif title:
                f_text += title
        except:
            pass

        return f_text

    def final_text(self, batched_list):
        '''
        process text with fix_text and combine_text 
        '''

        final_text_list = []


        for index, doc in enumerate(batched_list):   
            title = doc['title_translated']
            maintext = doc['maintext_translated']

            title_fixed = self.fix_text(title)
            maintext_fixed = self.fix_text(maintext)

            try:
                final_text_list.append({'_id':doc['_id'], 'text':self.combine_text(title_fixed, maintext_fixed)})
            except KeyError:
                try:
                    final_text_list.append({'_id':doc['_id'], 'text':maintext_fixed})
                except:
                    final_text_list.append({'_id':doc['_id'], 'text':title_fixed})
            
        return final_text_list



    def cliff_location(self, doc):
        '''
        cliff geoparsing api
        '''
        
        text = doc['text']
        # print(text)
        response = self.my_cliff.parse_text(text)
        dic_geo = {}
        try:
            #print(response)
            for i in response['results']['places']['mentions']:
                
                iso = pycountry.countries.get(alpha_2= i['countryCode'])
                # print(iso)
                
                try:
                    iso = iso.alpha_3
                except:
                    iso = 'None'
                    #print(i['countryCode'],'---- no alpha3 iso code')

                if i['countryCode'] =='XK':
                    iso = 'XKX'
                    
                if i['name'] == "Capitol" and iso == "MYS":
                    continue

                coord = {'name':i['name'], 'lat':i['lat'], 'lon':i['lon']}
                if iso in dic_geo.keys():
                    
                    if coord in dic_geo[iso]:
                        pass
                    else:
                        dic_geo[iso].append(coord)
                        # dic_geo[iso][i['name']] = {'lat':i['lat'], 'lon':i['lon']}
                
                else:
                    dic_geo[iso]=[]    
                    dic_geo[iso].append(coord)
                
        except Exception as err:
            print(f'error in detecting - {err}')
        
        return dic_geo

    def update_info(self, final_list):
        
        for nn, _doc in enumerate(final_list):
            try:
                self.db[self.colname].update_one(
                    {
                        '_id': _doc['_id']
                    },
                    {
                        '$set':{
                            'coord_cliff_locations':_doc['cliff_locations']
                                    
                        }
                    }
                )
                print(f'Updated!!({nn+1}/{len(final_list)}) - {self.colname} - {_doc["cliff_locations"]}')
                
            except Exception as err:
                print(f'FAILED updating!----- {err} ')
        

    def run(self):
        dates = pd.date_range('2012-1-1', datetime.now()+relativedelta(months=1), freq='M')

        for date in dates:    
            month_data = self.pull_data(date)

            if len(month_data) > self.batch_size:
                batched_lists = self.split_list(month_data)

                for batched_index, batched_list in enumerate(batched_lists):

                    print('--------',batched_index,'/',len(batched_lists),'--------')
                    final_list = self.final_text(batched_list = batched_list)

                    for i, doc in enumerate(final_list):
                        try:
                            locations = self.cliff_location(doc= doc)
                        except:
                            locations = None
                        print(f'({batched_index+1}/{len(batched_lists)}){self.colname}---{i+1}/{len(final_list)} --- {locations}')
                        
                        final_list[i]['cliff_locations'] = locations

                    proc = multiprocessing.Process(target=self.update_info(final_list=final_list))
                    proc.start()
            else:
                final_list = self.final_text(batched_list = month_data)

                for i, doc in enumerate(final_list):
                    try:
                        locations = self.cliff_location(doc= doc)
                    except:
                        locations = None

                    print(f'(1/1){self.colname}---{i+1}/{len(final_list)} --- {locations}')
                    final_list[i]['cliff_locations'] = locations

                proc = multiprocessing.Process(target=self.update_info(final_list=final_list))
                proc.start()

                        






if __name__ == '__main__':

    uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
    db = MongoClient(uri).ml4p
    my_cliff = Cliff('http://localhost:8080')
    # source_domains = ['balkaninsight.com']
    source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['GHA']}})
    # source_domains += ['balkaninsight.com']
    # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['MLI']}, 'primary_language':language})
    # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'major_international' : True})
    # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'major_regional' : True})

    location_pipeline(mongo_uri=uri, batch_size=128, sources=source_domains, my_cliff=my_cliff)
    
    # 'ALB', 'BEN', 'COL', 'ECU', 'AGO', 'ETH', 'GEO', 'KEN', 'PRY', 'MLI', 'MAR', 'NGA', 'SRB', 'SEN', 'TZA', 'UGA', 'UKR', 'ZWE', 'MRT','MYS', 'ZMB', 'XKX', 'NER', 'JAM', 'HND', 'PHL' 'GHA', 'RWA', 'GTM', 'BLR', 'COD', 'KHM', 'TUR', 'BGD', 'SLV', 'ZAF', 'TUN', 'IDN', 'NIC',  'ARM', 'LKA',  'CMR',// 'HUN', 'MWI', 'UZB', 'IND', 'MOZ'