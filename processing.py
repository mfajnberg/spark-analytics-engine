from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

SPARK = SparkSession.builder.appName("SmartFactoryAnalyticsEngine").getOrCreate()

lines = SPARK \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

historic_data = {}
DATA_WINDOW_SIZE = 25

def process_micro_batch(df, epoch_id):
    for row in df.collect():
        machine_id = row["MaschinenId"]
        machine_temperature = row["Temperatur"]
        historic_data.setdefault(machine_id, []).append((len(historic_data[machine_id]), machine_temperature))
        if len(historic_data[machine_id]) >= DATA_WINDOW_SIZE:
            data_window = historic_data[machine_id][-DATA_WINDOW_SIZE:]
            spark_df = SPARK.createDataFrame(data_window, ["Index", "Temperatur"])
            
            vectorAssembler = VectorAssembler(inputCols = ["Index"], outputCol = "features")
            v_df = vectorAssembler.transform(spark_df)
            lr = LinearRegression(featuresCol = "features", labelCol = "Temperatur")
            lr_model = lr.fit(v_df)

            slope = round(lr_model.coefficients[0], 2)
            print(f"Machine {machine_id}")
            print(f"{machine_temperature}°C")
            print(f"LR Slope: {slope}")
            print(f"+++Rising+++" if slope > 0 else (f"---Falling---" if slope < 0 else "===Stable==="))
            print("")

JSON_SCHEMA = StructType([
    StructField("MaschinenId", IntegerType(), True),
    StructField("Zeitstempel", StringType(), True),
    StructField("Temperatur", DoubleType(), True)
])

QUERY = lines \
    .select(
        from_json(lines.value, JSON_SCHEMA).alias("data")
    ).select(
        col("data.MaschinenId").alias("MaschinenId"),
        col("data.Zeitstempel").alias("Zeitstempel"),
        col("data.Temperatur").alias("Temperatur")
    ) \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(process_micro_batch) \
    .start()
QUERY.awaitTermination()