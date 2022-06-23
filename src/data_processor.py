from pydantic import ValidationError
import psycopg2
from src.models.dealer_data import DealerData
import pandas as pd
from datetime import datetime
import numpy as np


class DataProcessor:
    def __init__(self, conn):
        self.conn = conn

    # function to validate the data
    def validate_and_process_data(self, row_data: dict, is_new: bool):
        try:
            for key, value in row_data.items():
                if value == 'nan':
                    row_data[key] = None

            if row_data['dealer_images'] is not None and '|' in row_data['dealer_images']:
                row_data['dealer_images'] = row_data['dealer_images'].split('|')
            elif row_data['dealer_images'] is not None and ',' in row_data['dealer_images']:
                row_data['dealer_images'] = row_data['dealer_images'].split(',')
            elif row_data['dealer_images'] is not None and '|' in row_data['dealer_images'] and ',' in row_data[
                'dealer_images']:
                row_data['dealer_images'] = list(row_data['dealer_images'])
            else:
                row_data['dealer_images'] = []

            if 'dealer_installed_option_codes' in row_data and row_data['dealer_installed_option_codes'] is not None:
                row_data['dealer_installed_option_codes'] = row_data['dealer_installed_option_codes'].split(',')
            if 'dealer_installed_option_descriptions' in row_data and row_data[
                'dealer_installed_option_descriptions'] is not None:
                row_data['dealer_installed_option_descriptions'] = row_data[
                    'dealer_installed_option_descriptions'].split(',')

            row_data['updated_at'] = datetime.now()
            dealer_data = DealerData(**row_data)

            if is_new:
                self.insert_inventory(dealer_data)
            else:
                print('Update existing record')

            # return dealer_data
        except ValidationError as e:
            print(e.json())
            # insert error into database table
            self.log_validation_errors(row_data['hash'], row_data['dealership_id'], row_data['vin'], e.json())

    # function to check if record already exists in the database
    def process_data(self, parsed_data: pd.DataFrame):
        try:
            # load dealer data from db to validate the hash, insert only when hash changed else update
            parsed_data = parsed_data.replace({np.nan: None})
            parsed_dict = parsed_data.to_dict('records')
            current_dealership_id = parsed_dict[0]['dealership_id']
            postgreSQL_select_Query = f"select * from dealer_data where dealership_id = '{current_dealership_id}'"

            cur = self.conn.cursor()
            cur.execute(postgreSQL_select_Query)
            dealer_records = cur.fetchall()

            for row_data in parsed_dict:
                if self.verify_hash_for_vin(dealer_records, row_data['hash'], row_data['vin']) == 'New':
                    self.validate_and_process_data(row_data, True)
                elif self.verify_hash_for_vin(dealer_records, row_data['hash'], row_data['vin']) == 'Modified':
                    self.validate_and_process_data(row_data, False)

        except ValidationError as e:
            print(e.json())
            # no need to raise exception

    # get hash column by vin
    def verify_hash_for_vin(self, dealer_records, hash_value, vin):
        mode = 'New'
        for inventory in dealer_records:
            if inventory[2] == vin:
                if inventory[0] == hash_value:
                    mode = 'UnChanged'
                else:
                    mode = 'Modified'

        return mode

    # Function to insert record in the dealer_data table
    def insert_inventory(self, dealer_data: DealerData):
        try:
            curser = self.conn.cursor()

            insert_row = f"""INSERT INTO dealer_data (hash, dealership_id, vin, mileage, is_new, 
                                stock_number, dealer_year, dealer_make, dealer_model, dealer_trim,dealer_model_number,
                                dealer_msrp,dealer_invoice,dealer_body,dealer_inventory_entry_date,
                                dealer_exterior_color_description,dealer_interior_color_description,
                                dealer_exterior_color_code,dealer_interior_color_code,dealer_transmission_name,
                                dealer_installed_option_codes,dealer_installed_option_descriptions,dealer_additional_specs,
                                dealer_doors,dealer_drive_type,updated_at,dealer_images,dealer_certified) 
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                        """

            curser.execute(insert_row,
                           (dealer_data.hash, dealer_data.dealership_id,
                            dealer_data.vin, dealer_data.mileage, dealer_data.is_new, dealer_data.stock_number,
                            dealer_data.dealer_year, dealer_data.dealer_make, dealer_data.dealer_model,
                            dealer_data.dealer_trim,
                            dealer_data.dealer_model_number, dealer_data.dealer_msrp, dealer_data.dealer_invoice,
                            dealer_data.dealer_body, dealer_data.dealer_inventory_entry_date,
                            dealer_data.dealer_exterior_color_description,
                            dealer_data.dealer_interior_color_description,
                            dealer_data.dealer_exterior_color_code, dealer_data.dealer_interior_color_code,
                            dealer_data.dealer_transmission_name, dealer_data.dealer_installed_option_codes,
                            dealer_data.dealer_installed_option_descriptions, dealer_data.dealer_additional_specs,
                            dealer_data.dealer_doors, dealer_data.dealer_drive_type, dealer_data.updated_at,
                            dealer_data.dealer_images, dealer_data.dealer_certified))

            # Commit new record in db
            self.conn.commit()
            print('Record inserted successfully')
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            raise Exception(f"{error}")

    # Function to insert validation error in log table
    def log_validation_errors(self, hash_value, dealership_id, vin, error_log):
        try:
            curser = self.conn.cursor()

            insert_row = f"""INSERT INTO dealer_data_log (hash, dealership_id, vin, error_log, created_at) 
                            VALUES (%s,%s,%s,%s,%s);
                        """
            curser.execute(insert_row, (hash_value, dealership_id, vin, error_log, datetime.now()))

            # Commit new record in db
            self.conn.commit()
            print('Log Record inserted successfully')
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
