import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# 1. Database Connection
connection_string = "postgresql://postgres:12345@localhost:5432/Data_Warehouse"
try:
    db_connection = create_engine(connection_string)  
    print(f"successful connection")       
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")


# 2. Fetch Latest Month

conn = db_connection.connect()
result = conn.execute("SELECT MAX(pickup_year), MAX(pickup_month) FROM datetime_dim")
row = result.fetchone() 

yr, mon = row

if (mon >= 12):
    yr += 1
    mon = 1
else:
    mon+=1

print(yr)
print(mon)

# 3. Data Extraction:
url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{yr:04d}-{mon:02d}.parquet'


df = pd.read_parquet(url)   

# 4. Data Cleaning and Transformation 
rows_to_remove = int(len(df) * 0.5)
rows_to_remove_indices = df.sample(n=rows_to_remove, random_state=42).index        
df = df.drop(rows_to_remove_indices)
df = df.reset_index(drop=True)   
 

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

    #payment_type
allowed_values = [1, 2, 3, 4, 5, 6, 99] 
column_name = 'payment_type'
df[column_name] = df[column_name].apply(lambda x: x if x in allowed_values else 99)
    #vendor_id
allowed_values = [1, 2, 99]  
column_name = 'vendor_id'
df[column_name] = df[column_name].apply(lambda x: x if x in allowed_values else 99)
    #ratecode_id
allowed_values = [1, 2, 3, 4, 5, 6, 99] 
column_name = 'ratecode_id'
df[column_name] = df[column_name].apply(lambda x: x if x in allowed_values else 99)
    #trip_type
allowed_values = [1, 2, 99]  
column_name = 'trip_type'
df[column_name] = df[column_name].apply(lambda x: x if x in allowed_values else 99)

df['datetime_id'] = df['pickup_date'].astype(str) + '-' + df['dropoff_date'].astype(str)
df['location_id'] = df.PULocationID.astype(str) + '-' + df.DOLocationID.astype(str)


# 5. Dimension and Fact Table Construction
    # datetime_dim
datetime_df = df[['pickup_date', 'pickup_month','pickup_year', 'dropoff_date', 'dropoff_month', 'dropoff_year']].copy()        
datetime_df['datetime_id'] =  df['datetime_id']    
datetime_df = datetime_df.drop_duplicates()    

    # fact table
taxi_record = df[["vendor_id" , "store_and_fwd_flag" , "total_amount", "payment_type" ,"extra", "mta_tax","improvement_surcharge","tip_amount", "tolls_amount", "fare_amount","trip_duration_minutes", "trip_distance", "passenger_count" , "ratecode_id", "trip_type", "pickup_time","dropoff_time"]].copy()
taxi_record['datetime_id'] = df['datetime_id']
taxi_record['location_id'] = df['location_id']

# 6. Data Upload
datetime_df.to_sql('datetime_dim', con=db_connection, index=False, if_exists='append')
taxi_record.to_sql('trip_fact', con = db_connection, index = False, if_exists = 'append')

print('successful upload')
print(taxi_record)  
print(datetime_df)

# 7. Cleanup
conn.close()