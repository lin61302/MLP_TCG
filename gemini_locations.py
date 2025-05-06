import json
import time
import re
import unicodedata
from math import ceil
import pandas as pd
import pymongo
from datetime import datetime

import google.generativeai as genai

# Read the API key from a text file
with open("/home/diego/gemini_api_key.txt", "r", encoding="utf-8") as f:
    api_key = f.read().strip()

# Now configure Gemini using that key
genai.configure(api_key=api_key)

# Then select your model
model = genai.GenerativeModel("gemini-2.0-flash-lite")

##################################
# Retry logic for sending prompt #
##################################
MAX_RETRIES = 3
def safe_generate_content_with_retry(model, prompt):
    """
    Safely call Gemini's generate_content(prompt) with up to MAX_RETRIES attempts.
    If it fails (no text or an exception), returns None.
    """
    for attempt in range(MAX_RETRIES):
        try:
            response = model.generate_content(prompt)
            if response and response.text.strip():
                return response.text.strip()
            else:
                print(f"[safe_generate_content] Attempt {attempt+1}: got empty or no text.")
        except Exception as e:
            print(f"[safe_generate_content] Attempt {attempt + 1} failed: {e}")
        # Exponential backoff
        time.sleep(2 ** attempt)
    return None


###############################
# Extended textual scrubbing  #
###############################
def pre_clean_problem_text(text):
    """
    Extended cleaning to handle leftover advertisement strings, style/iframe blocks,
    partial code or curly braces, and other suspected patterns.
    """

    if not isinstance(text, str):
        return text

    # Remove multiple variants of advertisement or sponsored lines
    text = re.sub(
        r'(?i)(-+\s*advertisement\s*-+|advertorial|adverts?|publicit[eé]|sponsored\s+post)',
        '',
        text
    )

    # Remove <script> / <style> / <iframe> blocks
    text = re.sub(r'<script.*?>.*?</script>', '', text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'<style.*?>.*?</style>', '', text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'<iframe.*?>.*?</iframe>', '', text, flags=re.IGNORECASE | re.DOTALL)

    # If you want to remove ANY leftover html tags
    # text = re.sub(r'<[^>]*>', '', text, flags=re.IGNORECASE)

    # TOTALLY remove code in curly braces
    # text = re.sub(r'\{[^}]*\}', '', text)

    return text


##################################
# Mild text cleaning function    #
##################################
def moderate_clean_text(text):
    """
    Mild text cleaning, as in your script:
      1) Normalize Unicode (NFC).
      2) Remove control chars/newlines except normal whitespace.
      3) Keep extended Latin letters, digits, punctuation, basic symbols. Remove bizarre symbols.
      4) Collapse multiple spaces -> single.
      5) Strip leading/trailing spaces.
    """
    if not isinstance(text, str):
        return text

    # 1) Normalize to NFC
    text = unicodedata.normalize("NFC", text)

    # 2) Remove control chars except normal whitespace
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]+', ' ', text)
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')

    # 3) Keep letters (incl. extended-latin diacritics \u00C0-\u024F),
    #    digits, punctuation, basic symbols. Remove bizarre symbols.
    text = re.sub(r'[^a-zA-Z\u00C0-\u024F0-9\s\.,;:\-\'\"?!()&%$@#=_/\\]+', ' ', text)

    # 4) Collapse multiple spaces
    text = re.sub(r'\s+', ' ', text)

    # 5) Trim
    return text.strip()


##################################
# Write error responses to file  #
##################################
def write_error_to_file(collection_name, reason, prompt_text, response_text):
    """
    Write the prompt + entire raw response to a local file with collection info & timestamp if parsing fails.
    """
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    with open("gemini_error_log.txt", "a", encoding="utf-8") as f:
        f.write(f"===== Error in Collection: {collection_name} at {timestamp} =====\n")
        f.write(f"Reason: {reason}\n\n")
        f.write("=== PROMPT ===\n")
        f.write(prompt_text + "\n\n")
        f.write("=== RESPONSE ===\n")
        f.write(response_text if response_text else "[No Response or Empty]")
        f.write("\n\n")


##################################
# Main GeminiBatchGeoParser class
##################################
class GeminiBatchGeoParser:
    """
    Steps:
      1) Fetch from monthly collections e.g. articles-2012-1
      2) Batches of size default=15
      3) Up to 3 tries with parse checks
      4) If parse success, store 'gemini_locations' in DB ignoring top-level 'Unknown'
      5) Print first 3 updated docs per batch
      6) Now includes extra pre-clean step + standard mild text cleaning
      7) Writes prompt & response to gemini_error_log.txt if parse mismatch or parse error
    """

    def __init__(
        self,
        uri: str,
        db_name: str,
        countries: list,
        start_year: int = 2012,
        end_year: int = 2025,
        end_month: int = 4,
        batch_size: int = 10
    ):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[db_name]
        self.countries = countries
        self.start_year = start_year
        self.end_year = end_year
        self.end_month = end_month
        self.batch_size = batch_size

    def fetch_articles(self):
        loc = [
            doc['source_domain']
            for doc in self.db['sources'].find({
                'primary_location': {'$in': self.countries},
                'include': True
            })
        ]

        # loc = self.db.sources.distinct('source_domain', filter={'include' : True, 'major_international' : True})
        # loc += self.db.sources.distinct('source_domain', filter={'include' : True, 'major_regional' : True})

        env_query = {
            "source_domain": {'$in': loc},
            "environmental_binary.result": "Yes",
            "$or": [
                {"env_classifier.env_max": {"$nin": ["-999", "environmental -999", None]}},
                {"env_classifier.env_sec": {"$nin": ["-999", "environmental -999", None]}}
            ],
            "gemini_locations": {'$exists': False}
        }

        for year in range(self.start_year, self.end_year + 1):
            for month in range(1, 13):
                if year == self.end_year and month > self.end_month:
                    break
                collection_name = f"articles-{year}-{month}"
                collection = self.db[collection_name]

                cursor = collection.find(env_query, {
                    "_id": 1,
                    "title_translated": 1,
                    "maintext_translated": 1,
                    "source_domain": 1
                })
                docs = list(cursor)
                if docs:
                    yield collection_name, docs

    def build_prompt_for_batch(self, batch_df: pd.DataFrame):
        """
        Build a multi-article prompt with the new pre-clean step + mild cleaning.
        We'll also do the partial truncate ([:300], [:1200]) as usual.
        """
        prompt_header = "\n\nArticles for the Geoparsing:\n"
        articles_text = []
        for i, row in batch_df.iterrows():
            idx_in_batch = i % self.batch_size + 1

            raw_title = row.get("title_translated", "") or ""
            raw_main = row.get("maintext_translated", "") or ""

            # 1) Pre-clean to remove “- Advertisement -” lines, script blocks, etc.
            raw_title = pre_clean_problem_text(raw_title)
            raw_main = pre_clean_problem_text(raw_main)

            # 2) Then mild cleaning & truncation
            clean_title = moderate_clean_text(raw_title)[:300]
            clean_main = moderate_clean_text(raw_main)[:1200]

            entry = (
                f"{idx_in_batch}. Article {idx_in_batch} "
                f"Title: {clean_title} "
                f"Maintext: {clean_main}"
            )
            articles_text.append(entry)
        return prompt_header + "\n".join(articles_text)

    def geoparse_batch(self, collection_name: str, batch_df: pd.DataFrame):
        batch_size = len(batch_df)
        prompt_content = self.build_prompt_for_batch(batch_df)
        final_prompt = f"""
You are a geographic data extraction expert with advanced knowledge of global administrative divisions.
For each article, please identify every **distinct** location name and associate it with the correct subdivisions.

**Output** a JSON array of length {batch_size}, with exactly one element per article in the same order.
Each element is an object (dictionary) whose:
- Keys are distinct location names
- Values have:
  "location_level": "Country/ADMIN1/ADMIN2/Other/Unknown",
  "admin0": "Country or Unknown",
  "admin1": "Admin1 or Unknown",
  "admin2": "Unknown"

If no valid location is found, use an empty object {{}} for that array entry.

Here are the articles:

{prompt_content}
        """.strip()

        response_text = None
        for attempt in range(MAX_RETRIES):
            print(f"[geoparse_batch] Attempt {attempt+1} of {MAX_RETRIES} for {collection_name}")
            response_text = safe_generate_content_with_retry(model, final_prompt)
            if not response_text:
                # No text => try again
                time.sleep(2 ** attempt)
                continue

            try:
                cleaned_text = response_text.replace("```json", "").replace("```", "").strip()
                data = json.loads(cleaned_text)
                if not isinstance(data, list) or len(data) != batch_size:
                    print(f"⚠️ parse mismatch attempt {attempt+1}: array len != {batch_size} or not a list.")
                    # Log the mismatch with prompt & response
                    write_error_to_file(collection_name, 
                                        f"parse mismatch attempt {attempt+1}",
                                        final_prompt,
                                        response_text)
                    time.sleep(2 ** attempt)
                    continue
                # success parse => return data
                return data

            except (json.JSONDecodeError, ValueError) as e:
                print(f"⚠️ parse error attempt {attempt+1}: {e}")
                # Log the parse error with prompt & response
                write_error_to_file(collection_name,
                                    f"parse error attempt {attempt+1} => {e}",
                                    final_prompt,
                                    response_text)
                time.sleep(2 ** attempt)
                continue

        # If we exhaust all attempts, log the last known response or "No response"
        if response_text:
            write_error_to_file(collection_name,
                                "All attempts exhausted with parse mismatch or error",
                                final_prompt,
                                response_text)
        else:
            write_error_to_file(collection_name,
                                "All attempts exhausted => empty/timeout each time",
                                final_prompt,
                                "[No Response or Empty]")

        return None

    def update_db(self, collection_name: str, batch_docs: list, loc_data: list):
        collection = self.db[collection_name]
        printed_count = 0

        for doc, location_dict in zip(batch_docs, loc_data):
            doc_id = doc["_id"]
            if not isinstance(location_dict, dict):
                print(f"[No update] _id {doc_id}: not a dict -> {location_dict}")
                continue

            cleaned_dict = {}
            for place_name, admin_info in location_dict.items():
                if place_name.strip().lower() == "unknown":
                    continue
                if not isinstance(admin_info, dict):
                    continue

                cleaned_dict[place_name] = {
                    "location_level": admin_info.get("location_level", "Unknown"),
                    "admin0": admin_info.get("admin0", "Unknown"),
                    "admin1": admin_info.get("admin1", "Unknown"),
                    "admin2": admin_info.get("admin2", "Unknown"),
                }

            update_doc = {"gemini_locations": cleaned_dict}
            collection.update_one({"_id": doc_id}, {"$set": update_doc})

            if printed_count < 3:
                print(f"[Updated] _id {doc_id} => {update_doc}")
                printed_count += 1

    def run(self):
        for collection_name, docs in self.fetch_articles():
            total_docs = len(docs)
            total_batches = ceil(total_docs / self.batch_size)
            print("\n\n", end='')
            print(f"=== Processing {collection_name}: {total_docs} docs, {total_batches} batch(es) ===")

            for batch_start in range(0, total_docs, self.batch_size):
                batch_end = batch_start + self.batch_size
                batch_docs = docs[batch_start:batch_end]
                batch_df = pd.DataFrame(batch_docs)

                batch_num = (batch_start // self.batch_size) + 1
                print(f"--- Batch {batch_num}/{total_batches} with {len(batch_docs)} articles ---")

                loc_data = self.geoparse_batch(collection_name, batch_df)
                if loc_data is None:
                    print("Skipping this batch: parse error or no response after multiple attempts.")
                    continue

                self.update_db(collection_name, batch_docs, loc_data)


if __name__ == "__main__":
    # We'll iterate over decreasing batch sizes
    batch_sizes = [ 10, 5, 1]
    for bs in batch_sizes:
        print(f"\n\n=== Starting parser with batch_size={bs} ===")
        parser = GeminiBatchGeoParser(
            uri="mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true",
            db_name="ml4p",
            countries=[ 'COL','TUR','PER','UZB'] ,
                
            #done: "ENV_GTM", "ENV_NGA", 'ENV_SLV', 'ENV_PAN', 'ENV_INT', 'ENV_CRI', 'ENV_SLB','ENV_NIC','ENV_BEN','ENV_PAK','ENV_HND' 
            # 'SLB', 'NGA', 'HND','NIC','SLV','GTM','PAN','CRI', 'CMR','TUN','LKA','UGA','NPL', 
            # 'PAN','CRI'
            # processing 1: 'MAR','SSD','TZA','RWA','ZWE','COD','NER',
            #  processing 2: 'TLS', 'GEO', 'PRY', 'ECU', 'MLI', 'JAM', 'KAZ' ,'ARM','MOZ'
            # 'ETH','MRT','GHA','ALB', 'BEN', 'PAK',
            # ,'AGO'
            # IND
            # processing 1: 'PHL','BFA','AGO','AZE','MWI','BLR','BGD','HUN','XKX','MYS', fix: 'MOZ', 'ARM'
            #'IDN','PAN','MKD','KGZ','MDA','SEN','SRB','LBR','NAM'

            # 'ENV_AZE','ENV_KGZ','ENV_IDN','ENV_MDA','ENV_MKD','ENV_COD','ENV_KAZ','ENV_LKA','ENV_ECU','ENV_GTM'

            start_year=2012,
            end_year=2025,
            end_month=4,
            batch_size=bs
        )
        parser.run()
        print(f"=== Completed run with batch_size={bs} ===\n\n")

    