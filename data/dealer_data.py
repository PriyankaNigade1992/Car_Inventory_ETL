# Import necessary libraries
# pip install psycopg2-binary
import psycopg2

try:
    conn = psycopg2.connect(database="Rodo_Car_Inventories", user="postgres", password="postgres",
                            host="localhost", port="5432")
except:
    print("connection to the database failed")

cur = conn.cursor()
try:
    res = cur.execute("""
        CREATE TABLE IF NOT EXISTS dealer_data (
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
    """)
except Exception as err:
    print("Not able to create dealer_data table!", err)

try:
    res1 = cur.execute("""
        CREATE TABLE IF NOT EXISTS dealer_data_log (
        hash text,
        dealership_id text,
        vin text,
        error_log text,
        created_at timestamp with time zone NOT NULL DEFAULT now()
    );
    """)
    print(res1)
except Exception as err:
    print("Not able to create dealer_data_log table!", err)

conn.commit()  # <--- makes sure the change is shown in the database
conn.close()
cur.close()
