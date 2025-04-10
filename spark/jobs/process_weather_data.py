from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--year", type=int, required=True)
parser.add_argument("--month", type=int, required=True)
args = parser.parse_args()

spark = SparkSession.builder \
    .appName(f"Process weather data for {args.year}-{args.month}") \
    .getOrCreate()

weather_code_df = spark.read.csv("gs://flight-delay-pred-data/raw/weather_code_meteostat.csv", header=True, inferSchema=True)

weather_df = spark.read.csv(f"gs://flight-delay-pred-data/raw/weather_data_{args.year}_{args.month}.csv", header=True, inferSchema=True)

RENAMED_COLS = {
    "temp": "Temperature",
    "dwpt": "DewPoint",
    "rhum": "Humidity",
    "prcp": "Precipitation",
    "wspd": "WindSpeed",
    "pres": "Pressure",
    "coco": "WeatherCode",
}

# Mean value imputation
DEFAULT_VALUES = {
    "Temperature": 15.2,
    "DewPoint": 8.0,
    "Humidity": 67.2,
    "Precipitation": 0.2,
    "WindSpeed": 12.8,
    "Pressure": 1014.8,
    "WeatherCode": 1,
}

weather_df = weather_df \
    .withColumn("time", F.to_timestamp_ntz(F.col('time'))) \
    .withColumnRenamed("time", "Timestamp")

window_spec = Window.partitionBy("IATA_code").orderBy("Timestamp").rowsBetween(Window.unboundedPreceding, 0)

weather_df_filled = weather_df.select(
    "Timestamp",
    *[
        F.coalesce(
            F.last(old_col, ignorenulls=True).over(window_spec),
            F.lit(DEFAULT_VALUES[new_col])
        ).alias(new_col)
        for old_col, new_col in RENAMED_COLS.items()
    ],
    F.col("IATA_code").alias("Airport")
)

result_df = weather_df_filled \
    .join(F.broadcast(weather_code_df), weather_df_filled["WeatherCode"] == weather_code_df["Code"], "left") \
    .select(weather_df_filled["*"], weather_code_df['WeatherCondition'], weather_code_df['ExtremeWeather'])

result_df.coalesce(1).write.parquet(f"gs://flight-delay-pred-data/staging/weather_data_{args.year}_{args.month}.parquet", mode="overwrite")