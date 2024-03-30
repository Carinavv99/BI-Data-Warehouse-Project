SELECT 
    dt.pickup_year as trip_year,
    dt.pickup_month as trip_month,    
    SUM(tf.total_amount) AS total_transaction_amount
FROM 
    trip_fact tf JOIN datetime_dim dt
ON tf.datetime_id = dt.datetime_id
GROUP BY 
    CUBE (trip_year, trip_month)
ORDER BY 
    trip_year, trip_month;


SELECT 
    lt.pickup_borough as pickup_borough,
    lt.pickup_zone as pickup_zone,
    SUM(tf.total_amount) AS total_transaction_amount
FROM 
    trip_fact tf JOIN location_dim lt
ON tf.location_id = lt.location_id
GROUP BY 
    CUBE (pickup_borough,pickup_zone)
ORDER BY 
    pickup_borough, pickup_zone;



SELECT
	payment_type,
	COUNT(*) AS num_trips,
AVG(tip_amount) AS avg_tip_amount
FROM
	Trip_fact
GROUP BY
	payment_type
ORDER BY
	num_trips DESC;
