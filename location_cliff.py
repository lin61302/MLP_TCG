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
import subprocess




def location_pipeline(mongo_uri, batch_size, sources, my_cliff, DZA_or_PAK, int_reg, AGO_ind):
    db = MongoClient(uri).ml4p
    locs = Clifftag(mongo_uri=uri, batch_size=batch_size, sources=sources, my_cliff=my_cliff, DZA_or_PAK=DZA_or_PAK, int_reg=int_reg, AGO_ind=AGO_ind)
    locs.run()

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




class Clifftag:

    def __init__(self, mongo_uri, batch_size, sources, my_cliff, DZA_or_PAK, int_reg, AGO_ind):
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
        self.ssd_sources = self.db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['SSD']}}) + self.db.sources.distinct('source_domain', filter={'include' : True, 'major_international' : True}) + self.db.sources.distinct('source_domain', filter={'include' : True, 'major_regional' : True})
        self.DZA_or_PAK = DZA_or_PAK
        self.int_reg = int_reg
        self.AGO_ind = AGO_ind

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
                # 'language':'es',
                # 'es_translation_update': True,
                'cliff_locations':{'$exists':False}
                # 'title_translated':{'$regex': "Iran's supreme"},
                
                
                #'mordecai_locations.' + country_code : {'$exists' : True}
                #'mordecai_locations':{'$exists':True}
            }
        )
        docs = [doc for doc in cursor]
        return docs

    def pull_data_en(self, date):
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
                'en_cliff_locations':{'$exists':False},
                'language': 'en'
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
    
    def check_ssd(self, doc, cliff_dict):
        if 'SSD' not in cliff_dict:
            ll = []
            if 'South Sudan' in doc['text']:
                ll.append('South Sudan')
            if (('Juba' in doc['text'])) and ('SDN' in cliff_dict):
                ll.append('Juba')
                
            if len(ll)>0:
                cliff_dict['SSD'] = ll

        return cliff_dict

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
                final_text_list.append({'_id':doc['_id'], 'text':self.combine_text(title_fixed, maintext_fixed), 'source_domain':doc['source_domain']})
            except KeyError:
                try:
                    final_text_list.append({'_id':doc['_id'], 'text':maintext_fixed, 'source_domain':doc['source_domain']})
                except:
                    final_text_list.append({'_id':doc['_id'], 'text':title_fixed, 'source_domain':doc['source_domain']})
            
        return final_text_list
    
    def final_text_en(self, batched_list):
        '''
        process text with fix_text and combine_text 
        '''

        final_text_list = []


        for index, doc in enumerate(batched_list):
            title = doc['title_translated']
            maintext = doc['maintext_translated'][:1200]



            title_fixed = self.fix_text(title)
            maintext_fixed = self.fix_text(maintext)

            try:
                final_text_list.append({'_id':doc['_id'], 'text':self.combine_text(title_fixed, maintext_fixed), 'source_domain':doc['source_domain']})
            except KeyError:
                try:
                    final_text_list.append({'_id':doc['_id'], 'text':maintext_fixed, 'source_domain':doc['source_domain']})
                except:
                    final_text_list.append({'_id':doc['_id'], 'text':title_fixed, 'source_domain':doc['source_domain']})
            
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
            # print(response)
            for i in response['results']['places']['mentions']:
                
                iso = pycountry.countries.get(alpha_2= i['countryCode'])
                
                try:
                    iso = iso.alpha_3
                except:
                    iso = 'None'
                    #print(i['countryCode'],'---- no alpha3 iso code')

                if i['countryCode'] =='XK':
                    iso = 'XKX'
                    
                if i['name'] == "Capitol" and iso == "MYS":
                    continue

                    
                if iso in dic_geo.keys():
                    if i['name'] in dic_geo[iso]:
                        pass
                    else:
                        dic_geo[iso].append(i['name'])
                
                else:
                    dic_geo[iso]=[]
                    dic_geo[iso].append(i['name'])
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
                            'cliff_locations':_doc['cliff_locations']
                                    
                        }
                    }
                )
                print(f'Updated!!({nn+1}/{len(final_list)}) - {self.colname} - {_doc["cliff_locations"]}')

                
            except Exception as err:
                print(f'FAILED updating!----- {err} ')
    
    def update_info_en(self, final_list):
        
        for nn, _doc in enumerate(final_list):
            try:
                self.db[self.colname].update_one(
                    {
                        '_id': _doc['_id']
                    },
                    {
                        '$set':{
                            'en_cliff_locations':_doc['cliff_locations_en']
                                    
                        }
                    }
                )
                print(f'Updated!!({nn+1}/{len(final_list)}) - {self.colname} - {_doc["cliff_locations_en"]}')

                
            except Exception as err:
                print(f'(en)FAILED updating!----- {err} ')


    def special_update(self):
        try:
            condition = {
                '$or': [
                    {'title_translated': {'$regex': r"Ghaza([^A-Za-z]|$)"}},
                    {'maintext_translated': {'$regex': r"Ghaza([^A-Za-z]|$)"}}
                ]
            }

            
            
            update_result = self.db[self.colname].update_many(
                condition,
                {'$set': {'cliff_locations.PSE': ['Gaza']}}
            )
                    # print(f'Updated!!({nn+1}/{len(final_list)}) - {self.colname} - {_doc["cliff_locations"]}')
            print(f"{self.colname} - Gaza updated: {update_result.modified_count}")

                
        except Exception as err:
            print(f'FAILED updating Gaza!----- {err} ')
    
    def AGO_removal(self, dt):
        
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

        print(f'Collection: {collection_name} - Modified \"Angola\" documents: {update_result.modified_count}')
        

    def run(self):
        dates = pd.date_range('2012-1-1', datetime.now() + relativedelta(months=1), freq='M')

        for date in dates:

            # Update Ghaza to Gaza
            if self.DZA_or_PAK:
                proc = multiprocessing.Process(target=self.special_update())
                proc.start()   

            month_data = self.pull_data(date)
            
            # Determine the lists to process based on the batch size
            if len(month_data) > self.batch_size:
                lists_to_process = self.split_list(month_data)
            else:
                lists_to_process = [month_data]

            for batched_index, batched_list in enumerate(lists_to_process):
                print(self.colname, '--------', batched_index, '/', len(lists_to_process), '--------')
                final_list = self.final_text(batched_list=batched_list)

                for i, doc in enumerate(final_list):
                    try:
                        locations = self.cliff_location(doc=doc)
                    except Exception as err:
                        locations = None
                        print(err)

                    if final_list[i]['source_domain'] in self.ssd_sources:
                        locations = self.check_ssd(final_list[i], locations)

                    print(f'({batched_index+1}/{len(lists_to_process)}){self.colname}---{i+1}/{len(final_list)} --- {locations}')
                    final_list[i]['cliff_locations'] = locations

                proc = multiprocessing.Process(target=self.update_info(final_list=final_list))
                proc.start()

            if self.AGO_ind:
                proc = multiprocessing.Process(target=self.AGO_removal(date))
                proc.start()   

##################

            # for originally english articles only
            if int_reg:
        
                month_data_en = self.pull_data_en(date)
                month_data_en = [doc for doc in month_data_en if doc['language'] == 'en']

                if len(month_data_en) > self.batch_size:
                    en_lists_to_process = self.split_list(month_data_en)
                else:
                    en_lists_to_process = [month_data_en]

                for en_batched_index, en_batched_list in enumerate(en_lists_to_process):
                    print('--------', en_batched_index, '/', len(en_lists_to_process), '--------')
                    en_final_list = self.final_text_en(batched_list=en_batched_list)

                    for i, doc in enumerate(en_final_list):
                        try:
                            locations = self.cliff_location(doc=doc)
                        except Exception as err:
                            locations = None
                            print(err)

                        if en_final_list[i]['source_domain'] in self.ssd_sources:
                            locations = self.check_ssd(en_final_list[i], locations)

                        print(f'({en_batched_index+1}/{len(en_lists_to_process)}){self.colname}---{i+1}/{len(en_final_list)} --- {locations}')
                        en_final_list[i]['cliff_locations_en'] = locations

                    proc = multiprocessing.Process(target=self.update_info_en(final_list=en_final_list))
                    proc.start() 

                if self.AGO_ind:
                    proc = multiprocessing.Process(target=self.AGO_removal(date))
                    proc.start()      
                

                        






if __name__ == '__main__':

    uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
    db = MongoClient(uri).ml4p
    my_cliff = Cliff('http://localhost:8080')
    # source_domains = ['balkaninsight.com']
    country_list = []

    ############
    int_reg = True
    AGO_ind = False
    country_list = ['ENV_AZE','ENV_KGZ','ENV_IDN','ENV_MDA','ENV_MKD','ENV_COD','ENV_KAZ','ENV_LKA','ENV_ECU','ENV_GTM',]
    source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : country_list}})
    
    # source_domains = ['divergentes.com', 'revistafactum.com', 'alharaca.sv']
    # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['MLI']}, 'primary_language':language})


    if 'DZA' in country_list:
        source_domains_dza = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['DZA']}})
        location_pipeline(mongo_uri=uri, batch_size=128, sources=source_domains_dza, my_cliff=my_cliff, DZA_or_PAK = True, int_reg = int_reg, AGO_ind = AGO_ind)
    if 'PAK' in country_list:
        source_domains_pak = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['PAK']}})
        location_pipeline(mongo_uri=uri, batch_size=128, sources=source_domains_pak, my_cliff=my_cliff, DZA_or_PAK = False, int_reg = int_reg, AGO_ind = AGO_ind)

    source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : country_list}})
    location_pipeline(mongo_uri=uri, batch_size=128, sources=source_domains, my_cliff=my_cliff, DZA_or_PAK = False, int_reg = int_reg, AGO_ind = AGO_ind)


########## international or regional

    # int_reg = True
    # AGO_ind = True
    # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'major_international' : True})
    # source_domains += db.sources.distinct('source_domain', filter={'include' : True, 'major_regional' : True})
    # location_pipeline(mongo_uri=uri, batch_size=128, sources=source_domains, my_cliff=my_cliff, DZA_or_PAK = False, int_reg = int_reg, AGO_ind = AGO_ind)

###########
    # Git operations
    commit_message = "cliff geoparsing update"
    run_git_commands(commit_message)
    



    
    # 'ALB', 'BEN', 'COL', 'ECU', 'AGO', 'ETH', 'GEO', 'KEN', 'PRY', 'MLI', 'MAR', 'NGA', 'SRB', 'SEN', 'TZA', 'UGA', 'UKR', 'ZWE', 'MRT','MYS', 'ZMB', 'XKX', 'NER', 'JAM', 'HND', 'PHL' 'GHA', 'RWA', 'GTM', 'BLR', 'COD', 'KHM', 'TUR', 'BGD', 'SLV', 'ZAF', 'TUN', 'IDN', 'NIC',  'ARM', 'LKA',  'CMR',// 'HUN', 'MWI', 'UZB', 'IND', 'MOZ'

    # "PPLC": "Capital City",
    #     "PPLA": "City",
    #     "PPLA2": "Town",
    #     "PPL": "Populated Place",
    #     "ADM1": "ADM1",
    #     "ADM2": "ADM2",
    #     "ADM3": "ADM3",
    #     "ADM4": "ADM4",
    #     "PCLI": "Country",
    #     "PCLD": "Dependent Political Entity",
    #     "PCLF": "Freely Associated State",
    #     "PCLH": "Historical Political Entity",
    #     "PCLN": "Political Entity",
    #     "PCLS": "Semi-independent Political Entity",
    #     "PPLX": "Section of Populated Place",
    #     "PPLQ": "Abandoned Place",
    #     "PPLW": "Destroyed Place",
    #     "PPLH": "Historical Populated Place",
    #     "PPLS": "Populated Places",
    #     "PPLF": "Farm Village",
    #     "PPLL": "Locality",
    #     "PPLG": "Housing Development",
    #     "PPLZ": "Village Center",
    #     "ADMD": "Administrative Division",
    #     "LTER": "Lease Territory",
    #     "MNA": "Minor Area",
    #     "ZN": "Zone",
    #     "PRSH": "Parish"