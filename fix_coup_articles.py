
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
import math
import numpy as np

uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
db = MongoClient(uri).ml4p
today = pd.Timestamp.now()
load_dotenv()
#uri = os.getenv('DATABASE_URL')




# countries = [
#         ('Albania', 'ALB'), 
#         ('Benin', 'BEN'),
#         ('Colombia', 'COL'),
#         ('Ecuador', 'ECU'),
#         ('Ethiopia', 'ETH'),
#         ('Georgia', 'GEO'),
#         ('Kenya', 'KEN'),
#         ('Paraguay', 'PRY'),
#         ('Mali', 'MLI'),
#         ('Morocco', 'MAR'),
#         ('Nigeria', 'NGA'),
#         ('Serbia', 'SRB'),
#         ('Senegal', 'SEN'),
#         ('Tanzania', 'TZA'),
#         ('Uganda', 'UGA'),
#         ('Ukraine', 'UKR'),
#         ('Zimbabwe', 'ZWE'),
#         ('Mauritania', 'MRT'),
#         ('Zambia', 'ZMB'),
#         ('Kosovo', 'XKX'),
#         ('Niger', 'NER'),
#         ('Jamaica', 'JAM'),
#         ('Honduras', 'HND'),
#         ('Philippines', 'PHL'),
#         ('Ghana', 'GHA'),
#         ('Rwanda','RWA'),
#         ('Guatemala','GTM'),
#         ('Belarus','BLR'),
#         ('Cambodia','KHM'),
#         ('DR Congo','COD'),
#         ('Turkey','TUR'),
#         ('Bangladesh', 'BGD'),
#         ('El Salvador', 'SLV'),
#         ('South Africa', 'ZAF'),
#         ('Tunisia','TUN'),
#         ('Indonesia','IDN'),
#         ('Nicaragua','NIC'),
#         ('Angola','AGO'),
#         ('Armenia','ARM'),
#         ('Sri Lanka', 'LKA'),
#         ('Malaysia','MYS'),
#         ('Cameroon','CMR'),
#         ('Hungary','HUN'),
#         ('Malawi','MWI'),
#         ('Uzbekistan','UZB'),
#         ('India','IND'),
#         ('Mozambique','MOZ'),
#         ('Azerbaijan','AZE'),
#         ('Kyrgyzstan','KGZ'),
#         ('Moldova','MDA'),
#         ('Kazakhstan','KAZ'),
#         ('Peru','PER'),
#         ('Algeria','DZA'),
#         ('Macedonia','MKD'), 
#         ('South Sudan','SSD'),
#         ('Liberia','LBR'),
#         ('Pakistan','PAK'),
#         ('Nepal', 'NPL'),
#         ('Namibia','NAM'),
#         ('Burkina Faso', 'BFA'),

#     ]


from datetime import datetime, timedelta

# Assuming db is your MongoDB connection
# Function to generate collection names based on the date range
def generate_collection_names(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        yield f"articles-{current_date.year}-{current_date.month}"
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)

# Define the start and end date for the collections
start_date = datetime(2012, 1, 1)
end_date = datetime.now()

# Generate the collection names
collection_names = list(generate_collection_names(start_date, end_date))

# Iterate over countries and collection names to update the 'RAI' column
for colname in collection_names:
    # print(f"Processing collection: {colname}")
    
    # Check if the collection exists before querying
    if colname in db.list_collection_names():
        cur = db[colname].find(
            {
                'civic_new.event_type': 'coup'
            }
        )

        modified_count = 0

        # Process each document
        for doc in cur:
            model_outputs = doc['civic_new'].get('model_outputs', {})
            event_type = doc['civic_new'].get('event_type', '-999')

            # Find the max model output and its corresponding event (if model_outputs is not empty)
            if model_outputs:
                max_output_event = max(model_outputs, key=model_outputs.get)
                event_type = max_output_event  # Assuming you want to reset event_type to the max output event
            
            # Update the 'civic.event_type' in the database
            result = db[colname].update_one(
                {'_id': doc['_id']},
                {'$set': {'civic.event_type': event_type}}
            )
            
            # Increment the modified count if an update occurred
            if result.modified_count > 0:
                modified_count += 1

        # Print the number of modified articles for the current collection
        print(f"Modified {modified_count} articles in {colname}")
