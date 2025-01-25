import random
from cliff.api import Cliff
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
import re
import bs4
from bs4 import BeautifulSoup
from newspaper import Article
from dateparser.search import search_dates
import dateparser
import requests
from urllib.parse import quote_plus

import time
import json
import multiprocessing
from newsplease import NewsPlease
from datetime import datetime
import pandas as pd
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

#import requests

# url = 'http://localhost:8080/cliff-2.6.1/parse/text?='
#x = parse_text("This is about Einstien at the IIT in New Delhi.")

# payload = f'{url}\"This is about Einstien at the IIT in New Delhi.\"'
# response = requests.request("POST", payload.encode('utf-8'))
# my_cliff = Cliff(url)
# x= my_cliff.parse_text("This is about Einstien at the IIT in New Delhi.")
# df = pd.read_csv('/home/diego/peace/locations_dup.csv')


# from geopy.geocoders import Nominatim

# my_cliff = Cliff('http://localhost:8080')
# coord = [None]*len(df['LOCATION'])
# for index, location in enumerate(df['LOCATION']):
#     # print(location)
#     str = f"I'm in {location.title()}, {location.title()}, Ghana."
#     response = my_cliff.parse_text(str)
#     # print(response)
#     for i in response['results']['places']['mentions']:
#         if i['name'] =='Republic of Ghana':
#             # print('True')
#             continue
#         try:
#             # lat = 
#             # lon = response['results']['places']['mentions']
            
#             if i['name'].lower() == location.lower():
#                 ele = (i['lat'], i['lon'])
#                 coord[index] = ele
#                 print(i['name'])

        
            
#         except:
#             print('pass')

# print(coord)
# df['coord'] = coord

# coord2 = []
# geolocator = Nominatim(user_agent="my_user_agent")
# for location in df['LOCATION']:
#     try:
#         city = location.title()
#         country ="Gh"
#         loc = geolocator.geocode(city+','+ country)
#         coord2.append((loc.latitude, loc.longitude))
#         print("latitude is :-" ,loc.latitude,"longtitude is:-" ,loc.longitude)
#     except:
#         coord2.append(None)
#         print('nothing')
# df['coord2'] = coord2
# # df.to_csv("//home/diego/peace/locations_new_2.csv")






# df.to_csv('/home/diego/peace/locations_new_dup.csv')


my_cliff = Cliff('http://localhost:8080')
string = '''
In Popayán and Cali occupants of crash aircraft recover in Cauca and Guaviare. In hospitals in Cali, Riohacha, and Popayán, the four occupants of the crash aircraft were recovered on Thursday in the department of Cauca and were found alive by the rescue workers.The survivors were found by the Colombian Army and the Colombian Air Force (FAC) after 8:25 a.m. this Friday, near the hill of Timbiquí.The authorities reported that the aircraft's passengers are 

'''
string = '''
Millionaires joined their second match without winning: tied 1-1 with Huila. Relive here, minute by minute, the compromise between 'ambassadors' and opitas.Millionaires paid dearly for the waste and the luxury of the first time.When it had to win and close the encounter, when it seemed to pass over the Huila, it forgave it and those pardons were paid expensive in football. For the second part, the visiting painting closed the spaces and took advantage of one of the few options it had
'''
response = my_cliff.parse_text(string)
print(response)
print(response['results']['places']['mentions'])



#print(response)
# BASE_URL='http://localhost:6053'
# ENDPOINT='api/'
# def get_resource():
#     # resp=requests.get(BASE_URL+ENDPOINT)  <<< Request url malformed
#     resp=requests.get(BASE_URL+"/"+ENDPOINT)
#     print(resp.status_code)
#     print(resp.json())
# get_resource()

# requests.get('http://localhost:8080')

# urls=[]
# p_url = f"https://delo.ua/ru/search/?date_from=2015-01-01&date_to=2015-12-31&page=1&q=a&rubrics%5B0%5D=politics&rubrics%5B1%5D=economy&rubrics%5B2%5D=finance&rubrics%5B3%5D=society&rubrics%5B4%5D=opinions"
# print("Extracting from: ", p_url)
# reqs = requests.get(p_url, headers=headers)
# soup = BeautifulSoup(reqs.text, 'html.parser')
# print(soup)
# for i in soup.find_all('a',{'class':'c-card-list__link o-card__link'}):
#     link = i['href']
#     urls.append(link)
#     print(link)



# for link in soup.find_all('loc'):
#     print(link)
#     urls.append(link.text)
# urls = urls[2500:2600]
# print(urls)

# for i,url in enumerate(urls):
#     response = requests.get(url,headers=headers)
#     article = NewsPlease.from_html(response.text, url=url).__dict__
#     reqs = requests.get(url, headers=headers)
#     soup = BeautifulSoup(reqs.text, 'html.parser')
#     txt = ''
#     for s in soup.find_all('script',type="application/ld+json"):
#         txt+=s.string
        
#     date = re.findall('"datePublished": "(.*)",', txt)[0]
#     date = dateparser.parse(date).replace(tzinfo = None)
#     print(date)
    #print(i, article['date_publish'],url)


    