A = LOAD '/ds410/taxi/faredata/trip_fare_9.csv' USING PigStorage(',') AS (medallion:chararray, hack_license:chararray, vendor_id:chararray, pickup_datetime:chararray, payment_type:chararray, fare_amount:double, surcharge:double, mta_tax:double, tip_amount:double, tolls_amount:double, total_amount:double);
### Load based on how data appears on HDFS
describe A;

X = LIMIT A 50;

DUMP X;
## This is to check that the data is loaded correctly within Pig
