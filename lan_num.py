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

today = pd.Timestamp.now()
load_dotenv()
#uri = os.getenv('DATABASE_URL')
uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'


countries = [
        # ('Albania', 'ALB'),
        # ('Bangladesh', 'BGD')
        # ('Benin', 'BEN'),
#         ('Colombia', 'COL')
#         ('Ecuador', 'ECU'),
#         ('Ethiopia', 'ETH'),
#         ('Georgia', 'GEO'),
#         ('Kenya', 'KEN')
        # ('Paraguay', 'PRY'),
#         ('Mali', 'MLI')
        # ('Morocco', 'MAR')
#         ('Nigeria', 'NGA')
#         ('Serbia', 'SRB')
        # ('Senegal', 'SEN'),
#         ('Tanzania', 'TZA'),
#         ('Uganda', 'UGA'),
        # ('Ukraine', 'UKR'),
        # ('Cambodia', 'KHM')
        # ('Zimbabwe', 'ZWE'),
        # ('Mauritania','MRT'),
#         ('Zambia', 'ZMB'),
#          ('Kosovo', 'XKX')
        # ('Niger', 'NER'),
        # ('Jamaica', 'JAM'),
        # ('Honduras', 'HND'),
#         ('Rwanda', 'RWA')
#       ('Philippines', 'PHL')
#         ('Ghana', 'GHA')
    #('Guatemala', 'GTM')
    #('Tanzania','TZA')
    #('Benin','BEN')
#     ('Turkey','TUR')
#     ('Ecuador','ECU')
#     ('Senegal','SEN')
        # ('South Africa', 'ZAF')
    # ('Ukraine','UKR')
    # ('Turkey','TUR')
    # ('Nicaragua','NIC')
    # ('Malaysia','MYS')
    # ('Sri Lanka','LKA')
    # ('India','IND')
    # ('Cameroon', 'CMR')
    # ('Indonisia', 'IDN')
    # ('Congo' , 'COD')
    # ('Kazakhstan','KAZ')
    # ('Burkina Faso', 'BFA')
    # ('Env Int', 'ENV_INT')
    # ('Env Reg', 'ENV_REG')
    # ('Env cmr', 'ENV_CMR')
    # ('env dza','ENV_DZA')

    # ('env uzb','ENV_UZB')
    # ('env kaz','ENV_KAZ')
    # ('env col','ENV_COL')
    # ('env dom','ENV_DOM')
    # ('env ago','ENV_AGO')
    # ('env gha', 'ENV_GHA')
    # ('env geo', 'ENV_GEO')
    # ('env hun', 'ENV_HUN')
    # ('env khm','ENV_KHM')
    # ('env lbr','ENV_LBR')
    # ('env mda','ENV_MDA')
    # ('env mkd','ENV_MKD')
    # ('env cod','ENV_COD')
    # ('env kgz','ENV_KGZ')
    # ('env lka','ENV_LKA')
    # ('env aze','ENV_AZE')
    # ('env lbr', 'ENV_LBR')
    # ('env mli', 'ENV_MLI')
    # ('env npl', 'ENV_NPL')
    # ('env phl', 'ENV_PHL')
    # ('env npl','ENV_NPL')
    # ('env uga','ENV_UGA')
    # ('env eth','ENV_ETH')
    # ('env idn','ENV_IDN')
    # ('env mar','ENV_MAR')
    # ('env mys','ENV_MYS')
    # ('env idn', 'ENV_IDN')
    # ('env zaf', 'ENV_ZAF')
    ('env ssd', 'ENV_SSD')
    # ('env ind', 'ENV_IND')
    # ('env moz', 'ENV_MOZ'),
    # ('env tza', 'ENV_TZA')
    # ('env zwe', 'ENV_ZWE')
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
    print(loc)
    ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
    regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True})]

# print(ints)

uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
# 'interfax.com.ua', 'kyivpost.com', 'pravda.com.ua', 'delo.ua', 'kp.ua'
# Zambia, Kosovo, Mauritania, Bangladesh, Bolivia, Bosnia, Cambodia, CAR, DRC, El Salvador, Ghana, Guatemala, Honduras, Hungary, India, Indonesia, Iraq, Jamaica, Jordan, Kazakhstan, Liberia, Libya, Malawi, Malaysia, Mexico, Mongolia, Mozambique, Myanmar, Nepal, Nicaragua, Niger, Pakistan, Philippines, Rwanda, South Africa, South Sudan, Thailand, Yemen
db = MongoClient(uri).ml4p
for ctup in countries:
    #print('Starting: '+ctup[0])

    country_name = ctup[0]
    country_code = ctup[1]

    loc = [doc['source_domain'] for doc in db['sources'].find(
            {
                'primary_location': {'$in': [country_code]},
                'include': True
            }
        )]
    #print(loc)
    ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
    regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True})]

db = MongoClient(uri).ml4p
df = pd.DataFrame()
df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd') , freq='M')
df.index = df['date']
df['year'] = [dd.year for dd in df.index]
df['month'] = [dd.month for dd in df.index]
queue = []
print(df.index)
dic={}

sources = loc
print(sources)

# for source in sources:
dic = {}
for dt in df.index:
    colname = f'articles-{dt.year}-{dt.month}'
    # print(colname)
    cur = db[colname].find(
            {
                #'id': '60882a63f8bb748cadce4bf0'
                'source_domain': {'$in':loc},
                'include': {'$exists':True},
                #'event_type_RAI':{'$in':['-999']},
            # 'civic1.event_type':{'$exists':True},
                #'maintext_translated':{'$exists':False},
                #'language':'fr'
                #'maintext_translated':{'$type':'number'}
                #'mordecai_locations.' + country_code : {'$exists' : True}
                #'language_translated':{'$exists':True}
                #'mordecai_locations':{'$exists':True}
                
                #'$or': [
                    #{'mordecai_locations.' + country_code : {'$exists' : True}},
                    #{'mordecai_locations' : {}}
                #]
            }
        )
    d = [doc for doc in cur]
    
    for i in d:
        if i['language'] not in dic.keys():
            dic[i['language']] = 0
        else:
            dic[i['language']]+=1
    # for i in d:
    #     if i['download_via'] not in dic[dt.year].keys():
    #         dic[dt.year][i['download_via']] = 0
    #     else:
    #         dic[dt.year][i['download_via']]+=1

print(loc, ': ', dic)
################
# dic_year = {'other': 0, 'wayback': 0, 'direct': 0, 'ccnews': 0, 'Direct2': 0, 'gdelt': 0, 'wayback_alt': 0, 'Direct': 0, 'Wayback_alt': 0, None: 0, '': 0, 'LocalIND': 0}
# dic = {}
# for dt in df.index:
#     dic_year_month_temp = dic_year.copy()
#     dic[f'{dt.year}-{dt.month}'] = dic_year_month_temp
    
#     colname = f'articles-{dt.year}-{dt.month}'
#     print(colname)
#     for key in dic_year.keys():
#         if key == 'other':
#             cur = db[colname].count_documents(
#                     {
#                         #'id': '60882a63f8bb748cadce4bf0'
#                         # 'source_domain': {'$in':[source]},
#                         #'include': {'$exists':True},
#                         #'event_type_RAI':{'$in':['-999']},
#                         'download_via': {'$exists':False}
#                     # 'civic1.event_type':{'$exists':True},
#                         #'maintext_translated':{'$exists':False},
#                         #'language':'fr'
#                         #'maintext_translated':{'$type':'number'}
#                         #'mordecai_locations.' + country_code : {'$exists' : True}
#                         #'language_translated':{'$exists':True}
#                         #'mordecai_locations':{'$exists':True}
                        
#                         #'$or': [
#                             #{'mordecai_locations.' + country_code : {'$exists' : True}},
#                             #{'mordecai_locations' : {}}
#                         #]
#                     }
#                 )

#         else:
#             cur = db[colname].count_documents(
#                     {
#                         #'id': '60882a63f8bb748cadce4bf0'
#                         # 'source_domain': {'$in':[source]},
#                         #'include': {'$exists':True},
#                         #'event_type_RAI':{'$in':['-999']},
#                         'download_via': key
#                     # 'civic1.event_type':{'$exists':True},
#                         #'maintext_translated':{'$exists':False},
#                         #'language':'fr'
#                         #'maintext_translated':{'$type':'number'}
#                         #'mordecai_locations.' + country_code : {'$exists' : True}
#                         #'language_translated':{'$exists':True}
#                         #'mordecai_locations':{'$exists':True}
                        
#                         #'$or': [
#                             #{'mordecai_locations.' + country_code : {'$exists' : True}},
#                             #{'mordecai_locations' : {}}
#                         #]
#                     }
#                 )
#         dic[f'{dt.year}-{dt.month}'][key] = cur
#     print(dic[f'{dt.year}-{dt.month}'])

# print(dic)
##############
        
#     for i in d:
#         if i['language'] not in dic.keys():
#             dic[i['language']] = 0
#         else:
#             dic[i['language']]+=1
#     dic[dt.year] = {}
#     for i in d:
#         try:
#             if i['download_via'] not in dic[dt.year].keys():
#                 dic[dt.year][i['download_via']] = 0
#             else:
#                 dic[dt.year][i['download_via']]+=1
#         except:
#             if 'other' not in dic[dt.year].keys():
#                 dic[dt.year]['other'] = 0
#             else:
#                 dic[dt.year]['other'] += 1
#     print(dic[dt.year])


# print(dic)


#     print(dt, len(d))
#     docs+=d
# df = pd.DataFrame(docs)
# df.to_excel('/Users/zungrulin/Desktop/peace/habarileo_nans.xlsx')
# print(count)

# colname = f'articles-2020-5'
# cur = db[colname].find(
#             {
#                 #'id': '60882a63f8bb748cadce4bf0'
#                 #'source_domain': {'$in':sources},
#                 #'include': {'$exists':True},
#                 #'event_type_RAI':{'$in':['-999']},
#                # 'civic1.event_type':{'$exists':True},
#                 #'maintext_translated':{'$exists':False},
#                 'language':'sq'
#                 #'maintext_translated':{'$type':'number'}
#                 #'mordecai_locations.' + country_code : {'$exists' : True}
#                 #'language_translated':{'$exists':True}
#                 #'mordecai_locations':{'$exists':True}
                
#                 #'$or': [
#                     #{'mordecai_locations.' + country_code : {'$exists' : True}},
#                     #{'mordecai_locations' : {}}
#                 #]
#             }
#         )
# d = [doc for doc in cur]
    
# for i in d:
#     if i['source_domain'] not in dic.keys():
#         dic[i['source_domain']] = 0
#     else:
#         dic[i['source_domain']]+=1


# print(sources, ':', dic)