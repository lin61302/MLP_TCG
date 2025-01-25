'''
created on 2021-12-1
zung-ru
'''
import logging
from mordecai3 import Geoparser
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
# from cliff.api import Cliff
import multiprocessing

logging.getLogger('elasticsearch').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)



def location_pipeline(mongo_uri, batch_size, sources):
    db = MongoClient(uri).ml4p
    locs = NewMordecaitag(mongo_uri=uri, batch_size=batch_size, sources=sources)
    locs.run()




class NewMordecaitag:

    def __init__(self, mongo_uri, batch_size, sources):
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
        self.geo = Geoparser()

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
                # 'new_mordecai_locations':{'$exists':False}
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



    def mordecai_location(self, doc):
        '''
        cliff geoparsing api
        '''
        
        text = doc['text']
        result_dict = {}
        city_set = set()  # Keep track of cities to avoid duplicates

        try:
            result = self.geo.geoparse_doc(text)

            for entry in result['geolocated_ents']:
                country_code3 = entry['country_code3']
                city_name = entry['name']
                
                if country_code3 not in result_dict:
                    result_dict[country_code3] = []
                
                # Add city to the list only if it hasn't been added before for the same country code
                if city_name not in city_set:
                    result_dict[country_code3].append(city_name)
                    city_set.add(city_name)
                
        except Exception as err:
            print(f'error in detecting - {err}')
        
        return result_dict

    def update_info(self, final_list):
        
        for nn, _doc in enumerate(final_list):
            try:
                self.db[self.colname].update_one(
                    {
                        '_id': _doc['_id']
                    },
                    {
                        '$set':{
                            'new_mordecai_locations':_doc['new_mordecai_locations']
                                    
                        }
                    }
                )
                print(f'Updated!!({nn+1}/{len(final_list)}) - {self.colname} - {_doc["new_mordecai_locations"]}')
                
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
                            locations = self.mordecai_location(doc= doc)
                        except:
                            locations = None
                        print(f'({batched_index+1}/{len(batched_lists)}){self.colname}---{i+1}/{len(final_list)} --- {locations}')
                        
                        final_list[i]['new_mordecai_locations'] = locations

                    proc = multiprocessing.Process(target=self.update_info(final_list=final_list))
                    proc.start()
            else:
                final_list = self.final_text(batched_list = month_data)

                for i, doc in enumerate(final_list):
                    try:
                        locations = self.mordecai_location(doc= doc)
                    except:
                        locations = None

                    print(f'(1/1){self.colname}---{i+1}/{len(final_list)} --- {locations}')
                    final_list[i]['new_mordecai_locations'] = locations

                proc = multiprocessing.Process(target=self.update_info(final_list=final_list))
                proc.start()

                        






if __name__ == '__main__':

    uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
    db = MongoClient(uri).ml4p
    # source_domains = ['balkaninsight.com']
    source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['SSD']}})
    # source_domains += ['balkaninsight.com']
    # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['MLI']}, 'primary_language':language})
    # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'major_international' : True})
    # source_domains += db.sources.distinct('source_domain', filter={'include' : True, 'major_regional' : True})

    location_pipeline(mongo_uri=uri, batch_size=128, sources=source_domains)
    
    # 'ALB', 'BEN', 'COL', 'ECU', 'AGO', 'ETH', 'GEO', 'KEN', 'PRY', 'MLI', 'MAR', 'NGA', 'SRB', 'SEN', 'TZA', 'UGA', 'UKR', 'ZWE', 'MRT','MYS', 'ZMB', 'XKX', 'NER', 'JAM', 'HND', 'PHL' 'GHA', 'RWA', 'GTM', 'BLR', 'COD', 'KHM', 'TUR', 'BGD', 'SLV', 'ZAF', 'TUN', 'IDN', 'NIC',  'ARM', 'LKA',  'CMR',// 'HUN', 'MWI', 'UZB', 'IND', 'MOZ'

# result = geo.geoparse_doc("I visited Alexanderplatz in Berlin and tokyo.")

# result_dict = {}
# for entry in result['geolocated_ents']:
#     country_code3 = entry['country_code3']
#     city_name = entry['name']
    
#     if country_code3 not in result_dict:
#         result_dict[country_code3] = []
    
#     result_dict[country_code3].append(city_name)

# # Print the result_dict
# print(result_dict)


# import logging
# from mordecai3 import Geoparser
# from pymongo import MongoClient
# from datetime import datetime
# from dateutil.relativedelta import relativedelta
# import pandas as pd
# import multiprocessing

# # Set up logging configuration
# logging.getLogger('elasticsearch').setLevel(logging.WARNING)
# logging.getLogger('urllib3').setLevel(logging.WARNING)

# class NewMordecaitag:

#     def __init__(self, mongo_uri, batch_size, sources):
#         self.mongo_uri = mongo_uri
#         self.batch_size = batch_size
#         self.sources = sources
#         self.db = MongoClient(mongo_uri).ml4p
#         self.geo = Geoparser()

#     def pull_data(self, date):
#         self.colname = f'articles-{date.year}-{date.month}'
#         cursor = self.db[self.colname].find({
#             'source_domain': {'$in': self.sources},
#             'include': True,
#             'title_translated': {'$exists': True, '$ne': '', '$type': 'string'},
#             'maintext_translated': {'$exists': True, '$ne': '', '$type': 'string'},
#         })
#         return [doc for doc in cursor]

#     def split_list(self, list_to_split):
#         length = len(list_to_split)
#         wanted_parts = (length // self.batch_size) + 1
#         return [list_to_split[i * length // wanted_parts: (i + 1) * length // wanted_parts]
#                 for i in range(wanted_parts)]

#     def fix_text(self, text):
#         try:
#             text = text.replace('\n', '')
#             text = text.replace('“', "")
#             text = text.replace('"', "")
#             text = text.replace("”", "")
#         except:
#             print('Error in fixing/replacing text')
#         return text

#     def combine_text(self, title, text):
#         f_text = ''
#         try:
#             if title and text:
#                 f_text += title + ". " + text
#             elif text:
#                 f_text += text
#             elif title:
#                 f_text += title
#         except:
#             pass
#         return f_text

#     def final_text(self, batched_list):
#         final_text_list = []
#         for index, doc in enumerate(batched_list):
#             title = doc['title_translated']
#             maintext = doc['maintext_translated']
#             title_fixed = self.fix_text(title)
#             maintext_fixed = self.fix_text(maintext)
#             try:
#                 final_text_list.append({'_id': doc['_id'], 'text': self.combine_text(title_fixed, maintext_fixed)})
#             except KeyError:
#                 try:
#                     final_text_list.append({'_id': doc['_id'], 'text': maintext_fixed})
#                 except:
#                     final_text_list.append({'_id': doc['_id'], 'text': title_fixed})
#         return final_text_list

#     def mordecai_location(self, doc):
#         text = doc['text']
#         result_dict = {}
#         city_set = set()
#         try:
#             result = self.geo.geoparse_doc(text)
#             for entry in result['geolocated_ents']:
#                 country_code3 = entry['country_code3']
#                 city_name = entry['name']
#                 if country_code3 not in result_dict:
#                     result_dict[country_code3] = []
#                 if city_name not in city_set:
#                     result_dict[country_code3].append(city_name)
#                     city_set.add(city_name)
#         except Exception as err:
#             print(f'error in detecting - {err}')
#         return result_dict

#     def update_info(self, final_list):
#         for nn, _doc in enumerate(final_list):
#             try:
#                 self.db[self.colname].update_one(
#                     {'_id': _doc['_id']},
#                     {'$set': {'new_mordecai_locations': _doc['new_mordecai_locations']}}
#                 )
#                 print(f'Updated!!({nn+1}/{len(final_list)}) - {self.colname} - {_doc["new_mordecai_locations"]}')
#             except Exception as err:
#                 print(f'FAILED updating!----- {err}')

#     def process_batch(self, batched_list):
#         final_list = self.final_text(batched_list)
#         for i, doc in enumerate(final_list):
#             try:
#                 locations = self.mordecai_location(doc=doc)
#             except:
#                 locations = None
#             print(f'{self.colname}---{i+1}/{len(final_list)} --- {locations}')
#             final_list[i]['new_mordecai_locations'] = locations
#         self.update_info(final_list=final_list)

#     def run(self):
#         dates = pd.date_range('2012-1-1', datetime.now() + relativedelta(months=1), freq='M')
#         for date in dates:
#             month_data = self.pull_data(date)
#             if len(month_data) > self.batch_size:
#                 batched_lists = self.split_list(month_data)
#                 for batched_index, batched_list in enumerate(batched_lists):
#                     print('--------', batched_index, '/', len(batched_lists), '--------')
#                     self.process_batch(batched_list=batched_list)
#             else:
#                 self.process_batch(batched_list=month_data)


# if __name__ == '__main__':
#     # uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn

#     uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
#     db = MongoClient(uri).ml4p
#     source_domains = db.sources.distinct('source_domain', filter={'include': True, 'primary_location': {'$in': ['SSD']}})
    
#     location_pipeline = NewMordecaitag(mongo_uri=uri, batch_size=128, sources=source_domains)
#     location_pipeline.run()