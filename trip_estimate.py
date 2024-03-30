import streamlit as st
import psycopg2
import pandas as pd
import sqlalchemy as db
from datetime import datetime
from shapely.geometry import Point, Polygon
from math import radians, sin, cos, sqrt, atan2
import requests
import pandas as pd
import joblib


# API endpoint
api_url = "https://data.cityofnewyork.us/resource/755u-8jsi.json"

# Make a GET request to the API
response = requests.get(api_url)
# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data)
else:
    print(f"Error: Unable to fetch data. Status Code: {response.status_code}")



def process_polygon_string(data_column):
    for index, coord in enumerate(data_column):
      # Extract coordinates from the MultiPolygon dictionary
      coordinates = coord['coordinates'][0][0]

      # Convert coordinates to tuples
      coordinates = [tuple(coord) for coord in coordinates]
      data_column[index] = coordinates

def find_zone(lon, lat):
  for index, coord in enumerate(df.the_geom):
    zone_polygon = Polygon(coord)
    address_point = Point(lon, lat)
    is_within_zone = address_point.within(zone_polygon)

    if is_within_zone:
      return df.zone[index]

  print("insufficient info about the location")
  return None


process_polygon_string(df.the_geom)

def address_coord(address):
  original_address = address
  modified_address = original_address.replace(" ", "+")
  modified_address = modified_address.replace(',', '')

  # convert into coordinate
  url = "https://api.radar.io/v1/geocode/forward?query="
  url = url + str(modified_address)

  headers = {
    "Authorization": "prj_live_sk_062a35a643453a894d9ef6ca758cfed5aac96ddd"
  }

  response = requests.get(url, headers=headers)
  response_data = response.json()

  if 'addresses' in response_data and len(response_data['addresses']) > 0:
      # Extract the coordinates from the first address in the list
      first_address = response_data['addresses'][0]

      latitude = first_address['latitude']
      longitude = first_address['longitude']

      print(f"Latitude: {latitude}, Longitude: {longitude}")
  else:
      print("No coordinates found in the response.")

  return longitude, latitude

model = joblib.load('Estimate.pkl')
engine = db.create_engine('postgresql://postgres:12345@127.0.0.1:5432/Data_Warehouse')

st.title('NYC Green Cab')

# Header
st.markdown(
    """
    <style>
        .header {
            display: flex;
            align-items: center;
            justify-content: center;
            background-color: #111111;
            color: #ffffff; /* Changed text color to light gray */
            padding: 10px;
            border-radius: 10px;
            margin-bottom: 30px;
        }
        .header h1 {
            margin: 0;
            font-size: 36px;
        }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown('<div class="header"><h1>Trip Estimator</h1></div>', unsafe_allow_html=True)

# Input fields
with st.form(key='trip_form'):
    st.markdown('### Enter Trip Details')
    col1, col2 = st.columns(2)
    with col1:
        pickup_location = st.text_input('Pickup Location', 'Empire State Building, New York')
        dropoff_location = st.text_input('Drop-off Location', 'Central Park, New York')
    with col2:
        pickup_time = st.date_input('Pickup Date')
        pickup_time = st.time_input('Pickup Time', datetime.now().time())

    submitted = st.form_submit_button('Estimate Fare & Time')

pickup_longitude, pickup_latitude = address_coord(pickup_location)
dropoff_longitude, dropoff_latitude = address_coord(dropoff_location)

pickup_zone= find_zone( pickup_longitude, pickup_latitude)
dropoff_zone= find_zone( dropoff_longitude, dropoff_latitude)

pickup_code = df[df['zone'] == pickup_zone].location_id.astype(int).iloc[0]
dropoff_code = df[df['zone'] == dropoff_zone].location_id.astype(int).iloc[0]

location_string1 = f"{pickup_code}-{dropoff_code}"
location_string2 = f"{dropoff_code}-{pickup_code}"


time = pickup_time
time = datetime.strptime('22:47:11.250400', '%H:%M:%S.%f')
total_hours = time.hour + time.minute / 60 
print("Total hours elapsed since midnight:", total_hours)

query = f'''SELECT AVG(trip_fact.trip_duration_minutes) \
FROM trip_fact \
JOIN location_dim l \
ON trip_fact.location_id = l.location_id \
WHERE (l.location_id = '{location_string1}') OR (l.location_id = '{location_string2}')'''



df_duration = pd.read_sql_query(query, engine)
avg_duration = df_duration['avg'].iloc[0]
print(avg_duration)

query = f'''SELECT AVG(trip_fact.total_amount) \
FROM trip_fact \
JOIN location_dim l \
ON trip_fact.location_id = l.location_id \
WHERE (l.location_id = '{location_string1}') OR (l.location_id = '{location_string2}')'''

df_price = pd.read_sql_query(query, engine)
avg_price = df_price['avg'].iloc[0]
print(avg_price)



# Display results
if submitted:
    st.markdown(
        """
        <style>
            .result {
                background-color: #f0f0f0;
                padding: 20px;
                border-radius: 10px;
            }
            .result h2 {
                margin-top: 0;
            }
        </style>
        """,
        unsafe_allow_html=True
    )

    st.markdown('<div class="result">', unsafe_allow_html=True)
    st.markdown('<h2>Estimated Trip Details</h2>', unsafe_allow_html=True)
    st.markdown(f'**Pickup zone:** {pickup_zone}')
    st.markdown(f'**Drop-off zone:** {dropoff_zone}')
    st.markdown(f'**Pickup Time:** {pickup_time}')
    st.markdown(f'**Estimated trip duration(min):** {avg_duration}')   
    st.markdown(f'**Estimated price(USD):** {avg_price}')    
    st.markdown('</div>', unsafe_allow_html=True)