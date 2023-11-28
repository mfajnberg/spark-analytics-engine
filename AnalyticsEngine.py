from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, collect_list
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from sklearn.linear_model import LinearRegression
import numpy as np

spark = SparkSession.builder.appName("SmartFactoryAnalyticsEngine").getOrCreate()

# For incoming JSON data
json_schema = StructType([
    StructField("MaschinenId", IntegerType(), True),
    StructField("Zeitstempel", StringType(), True),
    StructField("Temperatur", DoubleType(), True)
])

"""
Read streaming mock data from a socket, for simplicity's sake.
In a production environment, this would be replaced with various Kafka topics from the actual smart factory, 
e.g. one per machine-id and undergoing aggregation into micro-batches before any further processing.
"""
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

"""
`historic_data` is used for performing linear regression over a temperature data window.
In a production environment, this would be replaced with a connection to something like Hadoop HDFS via Kafka.
""" 
historic_data = {}
window_size = 25

"""
Here parallelization is achieved at the Spark DataFrame level (parallel processing of batches), 
however, linear regression is performed sequentially within each executor.

This should be sufficient, since the processed data easily fits into memory,
and good performance is ensured by the parallelized linear regression implementation of scikit-learn.
"""
def process_micro_batch(df, epoch_id):
    for row in df.orderBy("MaschinenId").collect():
        machine_id = row["MaschinenId"]
        machine_temperature = row["Temperatur"]
        historic_data.setdefault(machine_id, []).append(machine_temperature)
        if historic_data[machine_id]:
            data_window = historic_data[machine_id][-window_size:]
            X = np.arange(len(data_window)).reshape(-1, 1)
            y = np.array(data_window)
            model = LinearRegression().fit(X, y)
            slope = round(model.coef_[0], 2)
            print(f"Id {machine_id}: {data_window[-1]}°C")
            print(f"LR Slope: {slope} (rising)" if slope > 0 else (f"LR Slope: {slope} (falling)" if slope < 0 else "(stable)"))
            print(row["Zeitstempel"])
            # print("Intercept: ", model.intercept_)
            print("")

"""
For larger fully parallelized batch jobs within a production environment, 
a seperate batch processing data pipeline should be implemented.

Here, on the other hand, data from the stream is divided into micro-batches, 
and the processing logic is applied in parallel to each micro-batch.
"""
query = lines \
    .select(
        from_json(lines.value, json_schema).alias("data")
    ).select(
        col("data.MaschinenId").alias("MaschinenId"),
        col("data.Zeitstempel").alias("Zeitstempel"),
        col("data.Temperatur").alias("Temperatur")
    ) \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(process_micro_batch) \
    .start()
query.awaitTermination()

"""
What changes when this is deployed to a cluster?

Data Aggregation (If Applicable):
If your production environment involves aggregating data from multiple Kafka topics or sources, 
the distributed nature of a Spark cluster can handle this more efficiently.

=> Potential to add interfaces to distributed processing components, namely HDFS, Batch jobs and complex ML-Models

Resource Utilization:
In a cluster, you have the opportunity to allocate more resources (CPU, memory) to your Spark job, 
potentially improving overall performance.
"""