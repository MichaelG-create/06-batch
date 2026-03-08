## Q2:
```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

!wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet

df = spark.read \
    .option("header", "true") \
    .parquet('yellow_tripdata_2025-11.parquet')

df = df.repartition(4)
df.write.parquet('yellow/2025/11/')

```


## Q3: Count of trips on November 15th

```python
df.createOrReplaceTempView('trips_data')

spark.sql("""
    SELECT
        COUNT(trip_distance)
    FROM
        trips_data
    WHERE (to_date(tpep_pickup_datetime) = DATE('2025-11-15'))

""").show()

```

## Q4: Longest trip

```python
spark.sql("""
    SELECT
        MAX((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime))/3600) as max_duration
    FROM trips_data
""").show()

```

## Q6:

```python

!wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

df_taxi_zone_lookup = spark \
                    .read \
                    .option("header", "true") \
                    .csv("taxi_zone_lookup.csv")

print(df_taxi_zone_lookup.schema)

from pyspark.sql import types

schema =  types.StructType([
    types.StructField('LocationID', types.IntegerType(), True),
    types.StructField('Borough', types.StringType(), True),
    types.StructField('Zone', types.StringType(), True),
    types.StructField('service_zone', types.StringType(), True)
])

df_taxi_zone_lookup = spark \
                    .read \
                    .option("header", "true") \
                    .schema(schema) \
                    .csv("taxi_zone_lookup.csv")

df_taxi_zone_lookup.show(10)

df_taxi_zone_lookup.createOrReplaceTempView('taxi_zone_lookup')

spark.sql("""
WITH count_pickup_location AS (
    SELECT
        PULocationID,
        COUNT(*) as pickup_number
    FROM trips_data
    GROUP BY PULocationID
    )
SELECT 
    t.Borough,
    t.Zone,
    c.pickup_number
FROM count_pickup_location c
LEFT JOIN taxi_zone_lookup t
ON c.PULocationID = t.LocationID
ORDER BY c.pickup_number
LIMIT 10
""").show()


spark.stop()
```
