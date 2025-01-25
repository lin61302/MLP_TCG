import os
import getpass
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import dateparser
import json
from tqdm import tqdm
from p_tqdm import p_umap
from pymongo import MongoClient
from pymongo.errors import CursorNotFound
from pymongo.errors import DuplicateKeyError

from helpers import regex_from_list
#from peacemachine.helpers import download_url


uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'

db = MongoClient(uri).ml4p

def remove_blacklist(col_name, source_domains):
    
    sources = db.sources.find(
        {   
            'source_domain' : {'$in' : source_domains},
            'blacklist_url_patterns': {'$ne': []}
            
        }
    )
    for source in sources:
        while True:
            try:
                black = source.get('blacklist_url_patterns')
               
                black_re = regex_from_list(black)

                result = db[col_name].delete_many(
                    {
                        'source_domain': source.get('source_domain'),
                        'url': {'$regex': black_re},
                        
                    }
                )
                print(str(result.deleted_count) + ' articles were deleted from {} in blacklist process!'.format(col_name))
            except CursorNotFound:
                print('Cursor error, restarting')
                continue


            break

            


def include_sources(colname, source_domains):
    
    for source in source_domains:
        db[colname].update_many(
            {
                'source_domain': source
            },
            {
                '$set': {
                    'include': True
                }
            }
        )
    
# def add_whitelist(colname, source_domains):
#     """
#     filters whitelist sites
#     """

#     # sources without whitelists first
#     sources = db.sources.find(
#         {
#             # 'major_international' : True,
#             # 'include': True,
#             # 'source_domain' : 'nytimes.com',
#             'source_domain' : {'$in' : source_domains},
#             '$or': [
#                 {'whitelist_url_patterns': []},
#                 {'whitelist_url_patterns': {'$exists': False}}
#             ]
#         }
#     )
#     for source in sources:
#         # deal with the sources that don't have a whitelist
#         if not(source.get('whitelist_url_patterns')):
#             db[colname].update_many(
#                 {
#                     'source_domain': source.get('source_domain')
#                 },
#                 {
#                     '$set': {
#                         'include': True
#                     }
#                 }
#             )

#     # sources with whitelists next
#     sources = db.sources.find(
#         {   
#             # 'major_international' : True,
#             # 'include': True,
#             # 'source_domain' : 'nytimes.com',
#             'source_domain' : {'$in' : source_domains},
#             '$or': [
#                 {'whitelist_url_patterns': {'$ne': []}}
#             ]
#         }
#     )
#     for source in sources:
#         source_regex = regex_from_list(source.get('whitelist_url_patterns'), compile=False)
#         db[colname].update_many(
#             {
#                 'source_domain': source.get('source_domain'),
#                 'url': {'$regex': source_regex}
#             },
#             {
#                 '$set': {
#                     'include': True
#                 }
#             }
#         )
    

# def add_otherlist(colname, source_domains):
    # """
    # doesn't delete but doesn't include sites with other_url_patterns
    # """

    # sources = db.sources.find(
    #     {
    #         # 'major_international' : True,
    #         # 'include' : True,
    #         # 'source_domain' : 'nytimes.com',
    #         'source_domain' : {'$in' : source_domains},
    #         'other_url_patterns': {
    #             '$exists': True,
    #             '$ne': []
    #         }
    #     }
    # )

    # for source in sources:
    #     source_regex = regex_from_list(source.get('other_url_patterns'), compile=False)
    #     db[colname].update_many(
    #         {
    #             'source_domain': source.get('source_domain'),
    #             'url': {'$regex': source_regex}
    #         },
    #         {
    #             '$set': {
    #                 'include': False
    #             }
    #         }
    #     )


def create_year_month(colname, source_domains):
    print("Collection", colname)
    cursor = db[colname].find(
        {
            # 'source_domain' : 'nytimes.com',
            # 'major_international' : True,
            'include' : True,
            'source_domain' : {'$in' : source_domains},
            'date_publish': {
                '$exists': True,
                '$type': 'date'
            },
            'month': {'$exists': False}
        }
    )
    for doc in cursor:
        db[colname].update_one(
            {'_id': doc['_id']},
            {
                '$set':{
                    'year': doc['date_publish'].year,
                    'month': doc['date_publish'].month
                }
            }
        )


def dedup_collection(colname, source_domains):

    # print('STARTING DEDUP')


    cursor = db[colname].find({'source_domain': {'$in': source_domains}})
    # cursor = db[colname].find({'include' : True}, batch_size=1)

    mod_count = 0
    
    docs = [ doc for doc in cursor]
    
    for _doc in docs:
        
        try:
            # if len(_doc['title'])>30:
            #     title1 = _doc['title'][:-4]
            # else:
            #     title1 = _doc['title']
            
            #title1 = _doc['title']

            # if len(_doc['url'])>45:
            #     url1 = 'http' + _doc['url'][5:]
            #     #url1=_doc['url']+'amp/'
            # else:
            #     continue
                
            url1 = 'http' + _doc['url'][5:]

            # if len(_doc['url'])>55:
            #     string1 = _doc['url'].split('https://www.bd-pratidin.com')[-1]
            #     string2 = 'https://web.archive.org/'

                #url1 = _doc['url'][:-4]
                # url1='https://www.bd-pratidin.com/'+'amp/'+_doc['url'][28:]
                #url1 = 'https://web.archive.org/web/'20210216040309/https://www.bd-pratidin.com/
            # else:
            #     continue
            #url1 = _doc['url'][:-2]

            # get the date filters
            #start = _doc['date_publish'].replace(hour=0, minute=0, second=0)
            
            # end = start + relativedelta(days=1)
            cur = db[colname].find(
                {   
                    'source_domain': _doc['source_domain'],
                    #'date_publish': {'$gte': start},
                    # '_id': {'$ne': _doc['_id']},
                    'title': _doc['title'],
                    '_id': {'$ne': _doc['_id']},
                    # 'maintext': _doc['maintext']
                    #'title': {'$regex':title1}
                    # 'url':_doc['url']
                    # 'url':{'$regex':url1}
                    # 'url':url1
                    # '$and':[{'url':{'$regex':string1}},
                    #         {'url':{'$regex':string2}}
                        # ]
                    # 'maintext_translated':{'$regex':'- No, no, no.'}
                }
            )
            
            docs2 = [doc for doc in cur]
            for dd in docs2:
                try:
                    db['deleted-articles'].insert_one(dd)
                except DuplicateKeyError:
                    pass
            
            res = db[colname].delete_many(
                {   
                    'source_domain': _doc['source_domain'],
                    #'date_publish': {'$gte': start},
                    # '_id': {'$ne': _doc['_id']},
                    'title': _doc['title'],
                    '_id': {'$ne': _doc['_id']},
                    # 'maintext': _doc['maintext'],
                    #'title': {'$regex':title1}
                    #'title': {'$regex':title1}
                    # 'url':_doc['url']
                    # 'url':{'$regex':url1}
                    # 'url':url1
                    # 'url':{'$regex':url1}
                    # '$and':[{'url':{'$regex':string1}},
                    #         {'url':{'$regex':string2}}
                        # ]
                    # 'maintext_translated':{'$regex':'- No, no, no.'}
                    
                }
            )

            if res.deleted_count != 0:
                mod_count += res.deleted_count
        except:
            pass
    
    print(f'{colname} DELETED: {mod_count}')


def set_none_language(colname, sd):
    cur = db[colname].find({'include' : True, 'source_domain' : sd, 'language' : None})
    for doc in cur:
        try:
            db[colname].update_one(
                {
                    '_id': doc['_id']
                },
                {
                    '$set': {
                        'language' : 'tr',
                    }
                }
            )
        except KeyError:
            pass

def set_xinhuanet_language(colname, sd, url_re, lang):
    cur = db[colname].find({ 'source_domain' : sd, 'language' : 'en'})
    for doc in cur:
        try:
            db[colname].update_one(
                {
                    '_id': doc['_id']
                },
                {
                    '$set': {
                        'language' : lang,
                        # 'language_translated': 'en',
                        # 'title_translated': doc['title'],
                        # 'maintext_translated': doc['maintext']
                    }
                }
            )
        except KeyError:
            pass


def set_kosovasot_language(colname, sd):
    cur = db[colname].find({'include' : True, 'source_domain' : sd, 'language' : 'en'})
    for doc in cur:
        try:
            db[colname].update_one(
                {
                    '_id': doc['_id']
                },
                {
                    '$set': {
                        'language' : 'sq',
                        # 'language_translated': 'en',
                        # 'title_translated': doc['title'],
                        # 'maintext_translated': doc['maintext']
                    }
                }
            )
        except KeyError:
            pass

def set_language_params(colname):
    print(colname)
    cur = db[colname].find({'include' : True, 'language': 'en', 'language_translated' : {'$exists' : False}})
    for doc in cur:
        try:
            db[colname].update_one(
                {
                    '_id': doc['_id']
                },
                {
                    '$set': {
                        'language_translated': 'en',
                        'title_translated': doc['title'],
                        'maintext_translated': doc['maintext']
                    }
                }
            )
        except KeyError:
            pass


if __name__ == "__main__":
    colnames = [ll for ll in db.list_collection_names() if ll.startswith('articles-')]
    colnames = [ll for ll in colnames if ll != 'articles-nodate']
    colnames = [ll for ll in colnames if (int(ll.split('-')[1]) >= 2022) and (int(ll.split('-')[1]) <= 2023)]
    # colnames = ['articles-2023-9','articles-2022-11']
    
    # sort by most recent
    # colnames = sorted(colnames, key = lambda x: (int(x.split('-')[1]), int(x.split('-')[2])), reverse=True)
    # colnames = ['articles-2019-1',]
    # colnames = [ll for ll in colnames if ll.startswith('articles-2021')]
    # source_domains = ['diken.com.tr']
    # source_domains = ['faceofmalawi.com','malawivoice.com']
    # source_domains = ['sof.uz','anhor.uz','asiaterra.info','daryo.uz']
    source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['ETH']}})
    # source_domains += db.sources.distinct('source_domain', filter={'include':True, 'major_international':True})
    # source_domains += db.sources.distinct('source_domain', filter={'include':True, 'major_regional':True})
    # for source in source_domains:
        
        
    print('STARTING BLACKLIST')
    for colname in colnames:
        remove_blacklist(colname, source_domains)

    print('INCLUDING SOURCES')
    for colname in colnames:
        include_sources(colname, source_domains)

    # print('DE-DUPLICATION PROCESS')
    # for colname in colnames:
    #     dedup_collection(colname, source_domains)

    # print('CREATING YEAR AND MONTH FIELDS')
    # for colname in colnames:
    #     create_year_month(colname, source_domains)

    # print('SETTING LANGUAGE KEYS FOR GEORGIATODAY')
    # for colname in colnames:
    #     set_none_language(colname, 'georgiatoday.ge')

    # print('SETTING LANGUAGE KEYS FOR KOSOVA-SOT')
    # for colname in colnames:
    #     print(colname)
    #     set_kosovasot_language(colname, 'kosova-sot.info')

    # print('SETTING LANGUAGE KEYS FOR JAMAICAOBSERVER')
    # for colname in colnames:
    #     print(colname)
    #     set_none_language(colname, 'jamaicaobserver.com')

    # print('SETTING LANGUAGE KEYS FOR XINHUANET')
    # for colname in colnames:
    #     print(colname)
    #     set_xinhuanet_language(colname, 't24.com.tr', 't24.com.tr', 'tr')
        # set_xinhuanet_language(colname, 'xinhuanet.com', 'gz.xinhuanet.com', 'zh')

    print('SETTING LANGUAGE KEYS FOR ENGLISH SOURCES')
    for colname in colnames:
        set_language_params(colname)


