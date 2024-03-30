import pandas as pd
import psycopg2
from sqlalchemy import create_engine

import sys
from itertools import product


connection_string = "postgresql://postgres:12345@localhost:5432/Data_Warehouse"
try:
    db_connection = create_engine(connection_string)       
    print(f"successful connection")       
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")

# Section 1: One-Time ETL for Dimension Tables
# location dimension

zone_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
zone_df = pd.read_csv(zone_url)

zone_df= zone_df.rename(columns={'LocationID': 'pickup_location_id'})
zone_df['dropoff_location_id'] = zone_df.pickup_location_id
zone_df= zone_df.rename(columns={'Zone': 'pickup_zone'})
zone_df['dropoff_zone'] = zone_df.pickup_zone
zone_df = zone_df.rename(columns={'Borough': 'pickup_borough'})
zone_df['dropoff_borough'] = zone_df.pickup_borough

id_combinations = list(product(zone_df.pickup_location_id, zone_df.dropoff_location_id))

location_df = pd.DataFrame(id_combinations, columns=['pickup_location_id', 'dropoff_location_id'])
location_df = location_df.merge(zone_df[['pickup_location_id', 'pickup_zone','pickup_borough']], on='pickup_location_id', how='left')
location_df = location_df.merge(zone_df[['dropoff_location_id', 'dropoff_zone','dropoff_borough']], on='dropoff_location_id', how='left')
location_df['location_id'] =location_df['pickup_location_id'].astype(str) + '-' + location_df['dropoff_location_id'].astype(str)

location_df.to_sql('location_dim', con = db_connection, index = False, if_exists = 'append')
print('location_dim successful upload')
#print(location_df)

# Vendor Dimension/Ratecode Dimension/Payment Dimension/Trip Dimension
columns = ['vendor_id', 'vendor_name']
values = {
    'vendor_id': [1, 2, 99],
    'vendor_name': ['Creative Mobile Technologies', 'VeriFone Inc.', None]
}
vendor_df = pd.DataFrame(values, columns=columns)
vendor_df.to_sql('vendor_dim', con = db_connection, index = False, if_exists = 'append')
print('vendor_dim successful upload')

columns = ['ratecode_id', 'ratecode']
values = {
    'ratecode_id': [1, 2, 3, 4, 5, 6, 99],
    'ratecode': ['Standard rate','JFK','Newark','Nassau/Westchester','Negotiated fare','Group ride', None]
}
ratecode_df = pd.DataFrame(values, columns=columns)
ratecode_df.to_sql('ratecode_dim', con = db_connection, index = False, if_exists = 'append')
print('ratecode_dim successful upload')

columns = ['payment_type', 'payment_type_name']
values = {
    'payment_type': [1, 2, 3, 4, 5, 6, 99],
    'payment_type_name': ['Credit card','Cash','No charge','Dispute','Unknown','Voided trip', None]
}
payment_df = pd.DataFrame(values, columns=columns)
payment_df.to_sql('payment_dim', con = db_connection, index = False, if_exists = 'append')
print('payment_dim successful upload')

columns = ['trip_type', 'trip_type_name']
values = {
    'trip_type': [1, 2, 99],
    'trip_type_name': ['Street-hail','Dispatch', None]
}
trip_df = pd.DataFrame(values, columns=columns)
trip_df.to_sql('trip_dim', con = db_connection, index = False, if_exists = 'append')
print('trip_dim successful upload')



# Section 2: Loop ETL

yr = 2015
mon = 1

while( yr <= 2023):
    while ( mon <= 12):
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{yr:04d}-{mon:02d}.parquet'

        df = pd.read_parquet(url)   

        rows_to_remove = int(len(df) * 0.5)
        rows_to_remove_indices = df.sample(n=rows_to_remove, random_state=42).index        
        df = df.drop(rows_to_remove_indices)
        df = df.reset_index(drop=True)   
 
    #data cleaning
        df= df.rename(columns={'lpep_pickup_datetime': 'pickup_datetime'})
        df=df.rename(columns={'lpep_dropoff_datetime': 'dropoff_datetime'})
        df= df.rename(columns={'VendorID': 'vendor_id'})
        df= df.rename(columns={'RatecodeID': 'ratecode_id'})

        df['trip_duration'] = df['dropoff_datetime'] - df['pickup_datetime']
        df['trip_duration_minutes'] = df['trip_duration'].dt.total_seconds() / 60
        df['pickup_time'] = df['pickup_datetime'].dt.time
        df['dropoff_time'] = df['dropoff_datetime'].dt.time

        df['pickup_date'] = df['pickup_datetime'].dt.date
        df['dropoff_date'] = df['dropoff_datetime'].dt.date

        df['pickup_date'] = df['pickup_datetime'].dt.date
        df['pickup_month'] = df['pickup_datetime'].dt.month
        df['pickup_year'] = df['pickup_datetime'].dt.year
        
        df['dropoff_date'] = df['dropoff_datetime'].dt.date
        df['dropoff_month'] = df['dropoff_datetime'].dt.month
        df['dropoff_year'] = df['dropoff_datetime'].dt.year


        columns_to_check = ['total_amount', 'fare_amount', 'extra', 'mta_tax','improvement_surcharge', 'tip_amount', 'tolls_amount']
        rows_to_drop = df.index[(df[columns_to_check] < 0).any(axis=1)]
        df.drop(df[(df[columns_to_check] < 0).any(axis=1)].index, inplace=True)

        mask = (df['pickup_year'] == yr) & (df['pickup_month'] == mon)
        df = df[mask]

        #if df['payment_type'] 
        allowed_values = [1, 2, 3, 4, 5, 6, 99] 
        column_name = 'payment_type'
        df[column_name] = df[column_name].apply(lambda x: x if x in allowed_values else 99)
        #if df['vendor_id']
        allowed_values = [1, 2, 99]  
        column_name = 'vendor_id'
        df[column_name] = df[column_name].apply(lambda x: x if x in allowed_values else 99)
        #if df['ratecode_id']
        allowed_values = [1, 2, 3, 4, 5, 6, 99] 
        column_name = 'ratecode_id'
        df[column_name] = df[column_name].apply(lambda x: x if x in allowed_values else 99)
        #if df['trip_type']
        allowed_values = [1, 2, 99]  
        column_name = 'trip_type'
        df[column_name] = df[column_name].apply(lambda x: x if x in allowed_values else 99)

        df['datetime_id'] = df['pickup_date'].astype(str) + '-' + df['dropoff_date'].astype(str)
        df['location_id'] = df.PULocationID.astype(str) + '-' + df.DOLocationID.astype(str)

    
        # datetime_dim
        datetime_df = df[['pickup_date', 'pickup_month','pickup_year', 'dropoff_date', 'dropoff_month', 'dropoff_year']].copy()        
        datetime_df['datetime_id'] =  df['datetime_id']    
        datetime_df = datetime_df.drop_duplicates()    

        # fact table
        taxi_record = df[["vendor_id" , "store_and_fwd_flag" , "total_amount", "payment_type" ,"extra", "mta_tax","improvement_surcharge","tip_amount", "tolls_amount", "fare_amount","trip_duration_minutes", "trip_distance", "passenger_count" , "ratecode_id", "trip_type", "pickup_time","dropoff_time"]].copy()
        taxi_record['datetime_id'] = df['datetime_id']
        taxi_record['location_id'] = df['location_id']

    # Upload
        datetime_df.to_sql('datetime_dim', con=db_connection, index=False, if_exists='append')
        taxi_record.to_sql('trip_fact', con = db_connection, index = False, if_exists = 'append')
        print('successful upload')       
        
        mon +=1        

    mon = 1
    yr +=1

db_connection.dispose()
