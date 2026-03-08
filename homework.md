# Module 6 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2025-11 data from the official website:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

'4.1.1'
![spark.version]({50622716-2CD2-4699-ADD2-3598155DC22E}.png)

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/06-batch/setup/pyspark.md)


## Question 2: Yellow November 2025

Read the November 2025 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- X 25MB
- 75MB
- 100MB
![parquet partitions files]({AF12F8DA-6327-42D5-ADCC-CAE4C69BB8B0}.png)

## Question 3: Count records

How many taxi trips were there on the 15th of November?

Consider only trips that started on the 15th of November.
![count_trips]({8D96D7E2-C785-4855-8D37-6D1AB6F27CB4}.png)

- 62,610
- 102,340
- X 162,604
- 225,768


## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?
![max_duration]({5E2A5266-9CD0-4AC8-96EE-C6DC574999D4}.png)
- 22.7
- 58.2
- X 90.6
- 134.5


## Question 5: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?
![local_host:4040]({F2665F21-ECE9-40CF-95E7-CBE71D5BB0AB}.png)
- 80
- 443
- X 4040
- 8080



## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?
![least_frequent_pickup_zone]({30DE6456-F4CF-4FFE-9A25-55E6B0E8895B}.png)
- X Governor's Island/Ellis Island/Liberty Island
- X Arden Heights
- Rikers Island
- Jamaica Bay
