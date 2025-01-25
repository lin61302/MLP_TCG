import os
import pandas as pd
from pymongo import MongoClient
import pycountry
from cliff.api import Cliff
from datetime import datetime
from dateutil.relativedelta import relativedelta
import multiprocessing

class ClifftagAdminLocations:

    def __init__(self, mongo_uri, batch_size, sources, my_cliff):
        self.mongo_uri = mongo_uri
        self.batch_size = batch_size
        self.sources = sources
        self.db = MongoClient(mongo_uri).ml4p
        self.my_cliff = my_cliff 

    def pull_data(self, date):
        self.colname = f'articles-{date.year}-{date.month}'
        cursor = self.db[self.colname].find(
            {
                'source_domain': {'$in': self.sources},
                'include': True,
                'title_translated': {'$exists': True, '$ne': '', '$type': 'string'},
                'maintext_translated': {'$exists': True, '$ne': '', '$type': 'string'},
            }
        )
        docs = [doc for doc in cursor]
        return docs

    def split_list(self, list_to_split):
        length = len(list_to_split)
        wanted_parts = (length // self.batch_size) + 1

        return [list_to_split[i * length // wanted_parts: (i + 1) * length // wanted_parts] 
                for i in range(wanted_parts)]

    def fix_text(self, text):
        try:
            text = text.replace('\n', '')
            text = text.replace('“', "")
            text = text.replace('"', "")
            text = text.replace("'", "")
            text = text.replace("”", "")
        except Exception as e:
            print(f'Error in fixing/replacing text: {e}')
        return text

    def combine_text(self, title, text):
        combined_text = ''
        try:
            if title and text:
                combined_text += title + ". " + text
            elif text:
                combined_text += text
            elif title:
                combined_text += title
        except Exception as e:
            print(f'Error in combining text: {e}')
        return combined_text

    def final_text(self, batched_list):
        final_text_list = []
        for doc in batched_list:
            title = doc.get('title_translated', '')
            maintext = doc.get('maintext_translated', '')

            title_fixed = self.fix_text(title)
            maintext_fixed = self.fix_text(maintext)

            combined_text = self.combine_text(title_fixed, maintext_fixed)

            final_text_list.append({'_id': doc['_id'], 'text': combined_text, 'source_domain': doc['source_domain']})

        return final_text_list

    def get_location_level(self, feature_code):
        feature_code_mapping = {
            # Feature code mappings...
            # (same as your original mapping)
        }
        return feature_code_mapping.get(feature_code, "Other")

    def cliff_admin_location(self, doc):
        if 'text' not in doc or not doc['text']:
            print(f"Document {doc['_id']} missing 'text' field.")
            return None

        text = doc['text']
        dic_admin = []

        try:
            response = self.my_cliff.parse_text(text)
        except Exception as err:
            print(f"CLIFF API error for document {doc['_id']}: {err}")
            return None

        try:
            # Build a mapping from id to place info for focus places
            id_to_place = {}

            # Add countries to id_to_place
            for country in response['results']['places']['focus'].get('countries', []):
                id_to_place[str(country['id'])] = country

            # Add states to id_to_place
            for state in response['results']['places']['focus'].get('states', []):
                id_to_place[str(state['id'])] = state

            # Add cities to id_to_place
            for city in response['results']['places']['focus'].get('cities', []):
                id_to_place[str(city['id'])] = city

            for mention in response['results']['places']['mentions']:
                # Use the original category for the location level
                location_info = {
                    "location": mention['name'],
                    "location_level": mention['featureCode'],  # Use original category
                    "original_category": mention['featureCode'],
                    "lon": mention['lon'],
                    "lat": mention['lat'],
                    "confidence": mention.get('confidence', None),
                    "admin_hierarchy": []
                }

                # Track locations we've added to avoid duplicates
                seen_locations = set()

                # Add the detected location itself to the admin_hierarchy
                if mention['name'] not in seen_locations:
                    location_info['admin_hierarchy'].append({
                        "name": mention['name'],
                        "level": mention['featureCode'],  # Use original category here as well
                        "country_code": mention['countryCode']
                    })
                    seen_locations.add(mention['name'])

                # Build hierarchy using stateGeoNameId and countryGeoNameId
                # First, check for stateGeoNameId
                if 'stateGeoNameId' in mention and mention['stateGeoNameId']:
                    state_id = str(mention['stateGeoNameId'])
                    if state_id in id_to_place:
                        state = id_to_place[state_id]
                        state_name = state['name']
                        if state_name not in seen_locations:
                            location_info['admin_hierarchy'].append({
                                "name": state_name,
                                "level": state['featureCode'],
                                "country_code": state['countryCode']
                            })
                            seen_locations.add(state_name)
                # Next, check for countryGeoNameId
                if 'countryGeoNameId' in mention and mention['countryGeoNameId']:
                    country_id = str(mention['countryGeoNameId'])
                    if country_id in id_to_place:
                        country = id_to_place[country_id]
                        country_name = country['name']
                        if country_name not in seen_locations:
                            location_info['admin_hierarchy'].append({
                                "name": country_name,
                                "level": country['featureCode'],
                                "country_code": country['countryCode']
                            })
                            seen_locations.add(country_name)
                    else:
                        # If country not in id_to_place, get country name from pycountry
                        try:
                            country_name = pycountry.countries.get(alpha_2=mention['countryCode']).name
                        except AttributeError:
                            country_name = mention['countryCode']
                        if country_name not in seen_locations:
                            location_info['admin_hierarchy'].append({
                                "name": country_name,
                                "level": "PCLI",  # Keep original category for country
                                "country_code": mention['countryCode']
                            })
                            seen_locations.add(country_name)

                dic_admin.append(location_info)

        except Exception as err:
            print(f'Error in processing CLIFF response for document {doc["_id"]}: {err}')
            return None

        return dic_admin if dic_admin else None







    def update_info(self, final_list):
        for nn, _doc in enumerate(final_list):
            try:
                if 'cliff_admin_locations' not in _doc or _doc['cliff_admin_locations'] is None:
                    _doc['cliff_admin_locations'] = []

                self.db[self.colname].update_one(
                    {
                        '_id': _doc['_id']
                    },
                    {
                        '$set': {
                            'cliff_admin_locations': _doc['cliff_admin_locations']
                        }
                    }
                )
                # Log the update
                if _doc['cliff_admin_locations']:
                    print(f'Updated!!({nn + 1}/{len(final_list)}) - {self.colname} - Locations: {[loc["location"] for loc in _doc["cliff_admin_locations"]] if _doc["cliff_admin_locations"] else "None"}')
                else:
                    print(f'Updated with empty list!!({nn + 1}/{len(final_list)}) - {self.colname} - None')

            except Exception as err:
                print(f'FAILED updating!----- {err}')

    def run(self):
        dates = pd.date_range('2012-1-1', datetime.now() + relativedelta(months=1), freq='M')

        for date in dates:
            month_data = self.pull_data(date)
            lists_to_process = self.split_list(month_data) if len(month_data) > self.batch_size else [month_data]

            for batched_index, batched_list in enumerate(lists_to_process):
                final_text_list = self.final_text(batched_list=batched_list)

                final_list = []
                for i, doc in enumerate(final_text_list):
                    try:
                        admin_locations = self.cliff_admin_location(doc=doc)
                    except Exception as err:
                        admin_locations = None
                        print(f"Error processing document {doc['_id']}: {err}")

                    print(f'({batched_index + 1}/{len(lists_to_process)}) {self.colname} --- {i + 1}/{len(final_text_list)} --- Locations: {[loc["location"] for loc in admin_locations] if admin_locations else "None"}')

                    final_list.append({'_id': doc['_id'], 'cliff_admin_locations': admin_locations})

                proc = multiprocessing.Process(target=self.update_info(final_list=final_list))
                proc.start()

if __name__ == '__main__':
    uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
    my_cliff = Cliff('http://localhost:8080')
    country_list = ['COL']
    source_domains = MongoClient(uri).ml4p.sources.distinct('source_domain', filter={'include': True, 'primary_location': {'$in': country_list}})
    
    location_extractor = ClifftagAdminLocations(mongo_uri=uri, batch_size=128, sources=source_domains, my_cliff=my_cliff)
    location_extractor.run()
