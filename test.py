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

db = MongoClient(uri).ml4p
today = pd.Timestamp.now()
load_dotenv()
#uri = os.getenv('DATABASE_URL')

uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'


countries = [
#         ('Albania', 'ALB'),
#         ('Benin', 'BEN'),
#         ('Colombia', 'COL')
#         ('Ecuador', 'ECU'),
#         ('Ethiopia', 'ETH'),
#         ('Georgia', 'GEO'),
#         ('Kenya', 'KEN')
#         ('Paraguay', 'PRY'),
#         ('Mali', 'MLI')
#         ('Morocco', 'MAR'),
#         ('Nigeria', 'NGA')
#         ('Serbia', 'SRB')
#         ('Senegal', 'SEN'),
#         ('Tanzania', 'TZA'),
#         ('Uganda', 'UGA'),
#         ('Ukraine', 'UKR'),
#         ('Zimbabwe', 'ZWE'),
#         ('Mauritania','MRT'),
#         ('Zambia', 'ZMB'),
#          ('Kosovo', 'XKX')
#         ('Niger', 'NER'),
#         ('Jamaica', 'JAM'),
#         ('Honduras', 'HND'),
#         ('Rwanda', 'RWA')
#       ('Philippines', 'PHL')
#         ('Ghana', 'GHA')
#     ('Guatemala', 'GTM')
#     ('Tanzania','TZA')
    #('Benin','BEN')
#     ('Turkey','TUR')
#     ('Ecuador','ECU')
#     ('Senegal','SEN')
#     ('Ukraine','UKR')
    # ('Turkey','TUR')
    ('Cambodia', 'KHM'),
#         ('Congo','COD')
#     ('Bangladesh','BGD')
#     ('El Slavador','SLV'),
#     ('South Africa','ZAF')
#     ('Tunisia','TUN')
    # ('Indonesia','IDN')
#     ('Angola','AGO')
#     ('Armenia','ARM')
#     ('Malaysia','MYS')
#     ('Sri Lanka','LKA')
#     ('Nicaragua','NIC')
#     ('Cameroon','CMR')
#     ('Hungary','HUN')
#     ('Malawi','MWI')
#     ('Uzbekistan','UZB')
    # ('India','IND')
    
    ]


db = MongoClient(uri).ml4p
for ctup in countries:
    print('Starting: '+ctup[0])

    country_name = ctup[0]
    country_code = ctup[1]

    loc = [doc['source_domain'] for doc in db['sources'].find(
            {
                'primary_location': {'$in': [country_code]},
                'include': True
            }
        )]
    # loc = ['sozcu.com.tr', 'posta.com.tr', 'sabah.com.tr', 'diken.com.tr']
    print(loc)
    ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
    regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True})]

print(regionals)

#########one month sample
colname = 'articles-2022-11'#articles-2021-11'
#{'telegraf.al': 1906, 'gazetatema.net': 3906, 'kosova-sot.info': 2214, 'koha.net': 1055, 'panorama.com.al': 3954, 'gazetashqiptare.al': 942}
mod_count = 0
cur = db[colname].find(
            { #'$or':[{'membership_hub':{'$exists':True}},{'membership_auth':{'$exists':True}}],
                'source_domain':{'$in':['t24.com.tr']},
                # 'source_domain':{'$nin':[ 'phnompenhpost.com']},
#                 'cliff_locations.GHA':'Tamale'
#                 'maintext':{'$regex':'Tamale'}
#                 'language':{'$nin':[ 'hi']},
#                 'include':{'$exists':False},
#                 'civic_new.event_type':'protest',
#                 'civic_related':{'$exists':True}
#                 'civic_new':{'$exists':False},
#                 'include':True,
#                 'civic_new':{'$exists': False},
#                 'civic_new':{'$not': {'$type': 'string'}},
#                 'title_translated':{'$regex': "Iran's supreme"},
#                 'event_type_civic_new':'protest',
#                 'event_type_civic_new':{'$exists': False},
#                 'event_type_civic_new_2':'defamationcase'
#                 'date_publish':{'$regex':'0'}
                
            
                
                'language':{'$nin':[ 'tr']},
#                 'civic1': {'$exists': True},
#                 'civic1.event_type':'defamationcase',
#                 'civic1.event_type': {'$in':['defamationcase','legalaction']},
#                 'event_type_civic1':'-999'
#                 'civic1.event_type':{'$exists':False}
#                 'include':True,
#                 'language_translated':{'$exists':False}
                #'event_type':'IDP',
                #'keyword':'ukraine',
                #'ukr_locations':{'$ne':[]}
                 #'retweet?':'No'
                #'keyword':'ukraine'
                #'keyword':{'$in':['російський','війни']}  #'війни'
                #'_id':{'$exists':True}
                #'_id':ObjectId('607cebb333803f410205dc7d')
                #'source_domain':{'$in':[ 'kyivindependent.com']},
                
                #'$and':[{'event_type':'IDP'}]
                
#                'civic1':None,#{'$not':{'$type': 'string'}},
                
                
#                 'include':True,
                # 'cliff_locations':{'$eq':{}}
                # 'language':{'$eq':'en'},
                #'tr_translation':{'$exists':True},
                #'tr_translation':{'$exists':True}
                #'km2_translation':{'$exists':True}
                #'Country_Georgia':'No',
#                 'civic1':{'$exists':False},
# 
                #'url':{'$regex':'https://www.hariansib.com/detail/Marsipature-Hutanabe/Jumat-Barokah-di-Masjid-An-Nur-Seibuluh--Kapolres-Sergai-Serukan-Tingkatkan-Toleransi-Umat-Beragama-'}
                #'url':{'$in':xx}
                #'url':  {'$in':['http://pravda.com.ua/']}
#                 'maintext_translated': {'$regex': '- No, no, no.'}
                #'maintext':{'$not':{'$eq':None}},
#                 'maintext_translated':None,
#                 'include': True,
#                 'RAI':{'$exists':False},
#                 'maintext_translated':{'$exists':True},
#                 'event_type_civic_new':{'$exists':False},
                #'maintext_translated':'',
#                 'maintext_translated':{'$not':{'$eq':None}},
#                 'maintext':{'$regex':'To provide the best'}
#                 'maintext':{'$regex':'We use cookies'}
                
                #'not':[{'url':{'$regex':'/web.archive.org/web/'}}],
                #'test_civic1':{'$exists':True}
                
                #'download_via':'wayback_alt',
                #'download_via':{'$exists':True}
                #'maintext_translated':None,#{'$not':{'$type':'string'}},
#                 'civic_new':{'$exists':False},
#                 'language_translated':{'$exists':True}
#                 'cliff_locations': {'$exists' : False}
            
#                 'title_translated':{'$exists': True,
#                         '$ne': '',
#                         '$ne': None,
#                         '$type': 'string'},'maintext_translated':{'$exists': True,
#                         '$ne': '',
#                         '$ne': None,
#                         '$type': 'string'},
#                 #'maintext':None
#                 'cliff_locations' : {'$exists' : False}
                #'language':{'$in':['rw']},
                #'test_maintext_translated':{'$exists':True}
                #'url':url
                
#                 '$nor': [
#                     {'cliff_locations.' + country_code : {'$exists' : True}},
#                     {'cliff_locations' : {}}
#                 ]
#                 'title': {
#                         '$exists': True,
#                         '$ne': '',
#                         '$ne': None,
#                         '$type': 'string'
#                     },
#                     'maintext': {
#                         '$exists': True,
#                         '$ne': '',
#                         '$ne': None,
#                         '$type': 'string'
#                     }
            }
        )

docs= [doc for doc in cur]

print(len(docs))

for index, doc in enumerate(docs):
    try:
        print(index,'(',doc['language'],doc['source_domain'], ')',doc['maintext'][:150])
    except:
        pass





# dates = pd.date_range('2022-3-1','2023-1-20', freq='D')#datetime.now()+relativedelta(days=1)
# # doc_t = []
# total = 0
# for date in dates:
    
#     colname = f"tweets-{date.year}-{date.month}-{date.day}"#f'articles-2021-11'   -ukrainian
#     colname2 = f"tweets-{date.year}-{date.month}-{date.day}-ukrainian"
#     colname3 = f"tweets-{date.year}-{date.month}-{date.day}-russian"
# #{'telegraf.al': 1906, 'gazetatema.net': 3906, 'kosova-sot.info': 2214, 'koha.net': 1055, 'panorama.com.al': 3954, 'gazetashqiptare.al': 942}
# #     mod_count = 0
#     cur = db.command("collstats", colname)['count']
#     cur2 = db.command("collstats", colname2)['count']
#     cur3 = db.command("collstats", colname3)['count']
    
# #     cur = db[colname].count()
# #     cur2 = db[colname2].count()
# #     cur3 = db[colname3].count()
    
#     total += cur
#     total += cur2
#     total += cur3
#     print(date, f': {cur} + {cur2} + {cur3}, total: {total}')
