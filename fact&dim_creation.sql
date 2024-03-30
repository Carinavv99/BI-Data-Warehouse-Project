CREATE TABLE datetime_dim (
    datetime_id VARCHAR primary key,
     pickup_date  TIMESTAMP,
     pickup_year INTEGER, 
     pickup_month INTEGER,  
     dropoff_date  TIMESTAMP,
     dropoff_year INTEGER, 
     dropoff_month INTEGER ,
     CONSTRAINT unique_datetime_id UNIQUE (datetime_id) 
);

CREATE TABLE location_dim(
	location_id  VARCHAR primary key,
	pickup_location_id  INTEGER,
    dropoff_location_id  INTEGER,
	pickup_zone VARCHAR(255),
	pickup_borough VARCHAR(255),
	dropoff_zone VARCHAR(255),
	dropoff_borough VARCHAR(255)
);


CREATE TABLE vendor_dim(
	vendor_id  INTEGER  primary key,
	vendor_name VARCHAR
);

CREATE TABLE ratecode_dim(
	ratecode_id  INTEGER  primary key,
	ratecode VARCHAR
);

CREATE TABLE payment_dim(
	payment_type  INTEGER  primary key,
	payment_type_name  VARCHAR
);

CREATE TABLE trip_dim(
	trip_type  INTEGER  primary key,
	trip_type_name VARCHAR
);

CREATE TABLE trip_fact (    
    datetime_id VARCHAR, 
    location_id VARCHAR,
    "vendor_id" INTEGER,
    "payment_type" INTEGER,
    "ratecode_id" INTEGER,
    "trip_type" INTEGER,
    "store_and_fwd_flag" VARCHAR(255),
    "total_amount" NUMERIC,   
    "extra" NUMERIC,
    "mta_tax" NUMERIC,
    "improvement_surcharge" NUMERIC,
    "tip_amount" NUMERIC,
    "tolls_amount" NUMERIC,
    "fare_amount" NUMERIC,
    "trip_duration_minutes"  NUMERIC,
    "trip_distance"  NUMERIC,
    "passenger_count" INTEGER,      
    "pickup_time"  TIME WITHOUT TIME ZONE,
    "dropoff_time" TIME WITHOUT TIME ZONE,
    FOREIGN KEY (datetime_id) REFERENCES datetime_dim(datetime_id),   
    FOREIGN KEY (location_id) REFERENCES location_dim(location_id),
    FOREIGN KEY (vendor_id) REFERENCES vendor_dim(vendor_id),   
    FOREIGN KEY (ratecode_id) REFERENCES ratecode_dim(ratecode_id),  
    FOREIGN KEY (payment_type) REFERENCES payment_dim(payment_type),   
    FOREIGN KEY (trip_type) REFERENCES trip_dim(trip_type)        
);

