pip install -r requirements.txt
# Car_Inventory_ETL
This repository contains source code for car inventory ETL process.

# Project overview
The purpose of this project was to extract data from csv files, transform it to predefined data structure, validate,  and then load into PosgreSQL Database.

# Data

Feed (csv files) stored in feeds folder contains detailed information about car availabilities at each dealership. Each feed provider has consistent data schema for each associated dealerships.

# PostgresSQL Database
Created database to store car inventories and log the errors. Below is brief description on tables:

1) CREATE TABLE dealer_data (
        hash text PRIMARY KEY,
        dealership_id text NOT NULL,
        vin text NOT NULL,
        mileage integer,
        is_new boolean,
        stock_number text,
        dealer_year integer,
        dealer_make text,
        dealer_model text,
        dealer_trim text,
        dealer_model_number text,
        dealer_msrp integer,
        dealer_invoice integer,
        dealer_body text,
        dealer_inventory_entry_date date,
        dealer_exterior_color_description text,
        dealer_interior_color_description text,
        dealer_exterior_color_code text,
        dealer_interior_color_code text,
        dealer_transmission_name text,
        dealer_installed_option_codes text[],
        dealer_installed_option_descriptions text[],
        dealer_additional_specs text,
        dealer_doors text,
        dealer_drive_type text,
        updated_at timestamp with time zone NOT NULL DEFAULT now(),
        dealer_images text[],
        dealer_certified boolean
    );


2) CREATE TABLE IF NOT EXISTS dealer_data_log (
        hash text,
        dealership_id text,
        vin text,
        error_log text,
        created_at timestamp with time zone NOT NULL DEFAULT now()
    );

# Project Files
- data/dealer_data.py - contains sql queries to create above tables
- src/models/dealer_data.py - contains model to validate data
- src/provider_schema.py - contains mapping of database table and feed csv file header
- src/provider_feed_reader - contains code to read the data from csv file into dataframe, transforming it into required format
- src/data_processor - contains code to validate the data and transform the data to database

# ETL Description
The ETL process was coded in Python language. List of required libraries is stored in requirements.txt

The process could be presented in following steps:

- Extract data - 
  First, data needs to be loaded. Each csv file is loaded in python dataframe one by one and processed respectively. 

- The mapping between the desired table structure and csv file headers was necessary since the data columns of the desired table structure differ from the csv file, and for each dealer there is a particular csv file format.

- Before transforming data to database, it is validated using `typing` and `pydantic` python libraries.

- Transform - if record already exists in the database, updated the record, otherwise inserted record in the database.

# How to run
Navigate to project directory
Connection string to connect to database:
`conn = psycopg2.connect(database="Rodo_Car_Inventories", user="postgres", password="*******",
                            host="localhost", port="5432")`

To create the tables run - python `data/dealer_data.py`
To start the application run - `python main.py`

# Testing
`SELECT * FROM dealer_data;`</br>
Application inserted 97 records from both the providers' dealership feed csv files, and it gave validation error
for one record due to missing value for 'vin' field.

Added error log in the `dealer_data_log` table </br>
`SELECT * FROM dealer_data_log;`
This returns all the failed transactions due to data validation error. 

Result of both the queries is stored in Output folder.


# Task 2

Wrote this below query to get the month wise number of inventories for each dealership <br><br>
`
select dealership_id, 
to_char(dealer_inventory_entry_date, 'YYYY-MM'),
count(*) as count
from dealer_data
group by dealership_id, to_char(dealer_inventory_entry_date, 'YYYY-MM')
`



