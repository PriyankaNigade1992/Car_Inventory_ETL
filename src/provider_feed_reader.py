import os
from glob import glob
import pandas as pd
from src.provider_schema import Mappings
import psycopg2
import hashlib
import src.data_processor as dp
from datetime import datetime

try:
    conn = psycopg2.connect(database="Rodo_Car_Inventories", user="postgres", password="Mercyhurst100%",
                            host="localhost", port="5432")
except:
    print("connection to the database failed")


# data_processor = dp.DataProcessor(conn)

class SchemaReader:
    def __init__(self):
        self.PROVIDERS_FILES_PATH = "feeds/"
        self.EXT = "*.csv"

    # Get all files and associate with its provider
    def get_providers_filepaths(self):
        all_csv_files = []
        for path, subdir, files in os.walk(self.PROVIDERS_FILES_PATH):
            for file in glob(os.path.join(path, self.EXT)):
                provider = file.split('/')[1]
                all_csv_files.append({'provider': provider, 'file': file})

        # print(all_csv_files)
        return all_csv_files

    # Read data from all the csv
    # Map the columns
    def read_feed_data(self):
        feeds = self.get_providers_filepaths()
        for feed in feeds:
            csv_data = pd.read_csv(feed['file'])

            print(f"{len(csv_data)} records for {feed['provider']}")

            # rename the column names as per the mapping set
            # csv_data.columns = csv_data.columns.to_series().map(Mappings.PROVIDERS_SCHEMA_COLUMNS[feed['provider']])
            csv_data.rename(Mappings.PROVIDERS_SCHEMA_COLUMNS[feed['provider']], axis=1, inplace=True)

            # create new csv file to check if the column mapping is correct
            # csv_data.to_csv('test' + '.csv')

            parsed_data = self.parse_data(csv_data, feed['provider'])

            # call the function from data_processor to check for valid data
            dp.DataProcessor(conn).process_data(parsed_data)

    # function to set values as per the condition
    def parse_data(self, csv_data: pd.DataFrame, provider: str):

        if provider == 'provider1' and 'is_new' in csv_data.columns:
            csv_data['is_new'] = csv_data['is_new'] == 'New'
            # if csv_data[csv_data['is_new']] == 'New':
            #     csv_data['is_new'] = True
            # else:
            #     csv_data['is_new'] = False

        if provider == 'provider2' and 'is_new' in csv_data.columns:
            csv_data['is_new'] = csv_data['is_new'] == 'N'
            # if csv_data['is_new'] == 'New':
            #     csv_data['is_new'] = True
            # else:
            #     csv_data['is_new'] = False

        #csv_data.to_csv('test2' + '.csv')

        if 'dealer_msrp' in csv_data.columns and 'is_new' in csv_data.columns and 'List Price' in csv_data.columns:
            csv_data['dealer_msrp'] = csv_data['dealer_msrp'] if [csv_data['is_new']] else csv_data['List Price']
            # if not csv_data['is_new']:
            #     csv_data['dealer_msrp'] = csv_data['List Price']
        if 'dealer_invoice' in csv_data.columns and 'is_new' in csv_data.columns and 'List Price' in csv_data.columns:
            csv_data['dealer_invoice'] = csv_data['dealer_invoice'] if [csv_data['is_new']] else csv_data['List Price']

        if 'dealer_certified' in csv_data.columns:
            csv_data['dealer_certified'] = csv_data['dealer_certified'] == 'Yes'

        csv_data['dealer_inventory_entry_date'] = pd.to_datetime(csv_data['dealer_inventory_entry_date'])

        # csv_data.dealer_inventory_entry_date.dt.strftime('%Y%m%d').astype(int)
        # csv_data.to_csv(provider + '.csv')

        # create hash based on all row values to identify any modification happened from previous updates
        # with this combine all row data and then create hash value
        csv_data['hash'] = csv_data.apply(lambda x: ''.join(x.astype(str)), axis=1).apply(self.create_hash)

        return csv_data

    # Function to create hash for each record
    def create_hash(self, str_value: str):
        return hashlib.sha3_256(str_value.encode()).hexdigest()
