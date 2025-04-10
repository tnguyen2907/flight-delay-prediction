from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--year", type=int, required=True)
parser.add_argument("--month", type=int, required=True)
args = parser.parse_args()

spark = SparkSession.builder \
    .appName(f"Join weather and flight data for {args.year}-{args.month}") \
    .getOrCreate()



weather_df = spark.read.parquet(f"gs://flight-delay-pred-data/staging/weather_data_{args.year}_{args.month}.parquet")

flight_df = spark.read.parquet(f"gs://flight-delay-pred-data/staging/flight_data_{args.year}_{args.month}.parquet")

weather_df.createOrReplaceTempView("Weather")

flight_df.createOrReplaceTempView("Flight")

MIDNIGHT_TEMP = 17.2
MIDNIGHT_DWPT = 8
MIDNIGHT_RHUM = 60.9
MIDNIGHT_PRCP = 0.1
MIDNIGHT_WDSP = 13.8
MIDNIGHT_PRES = 1014.1

year = int(args.year)
month = int(args.month)
start = pd.to_datetime(f"{year}-{month}-01")
end = pd.to_datetime(f'{year}-{month}-01') + pd.DateOffset(months=1) - pd.Timedelta(seconds=1)

params_lst = [
    ("Temperature", MIDNIGHT_TEMP),
    ("DewPoint", MIDNIGHT_DWPT),
    ("Humidity", MIDNIGHT_RHUM),
    ("Precipitation", MIDNIGHT_PRCP),
    ("WindSpeed", MIDNIGHT_WDSP),
    ("Pressure", MIDNIGHT_PRES),
]

weather_col_lst = [param[0] for param in params_lst] + ["WeatherCode", "WeatherCondition", "ExtremeWeather"]

def generate_weather_sql(params_lst):
    sql = "    SELECT *,\n"
    for param, default_val in params_lst:
        for i in range(1, 3):
            sql += f"\tLAG(`{param}`, {i}, {default_val}) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `{param}Lag{i}`,\n"
            weather_col_lst.append(f"{param}Lag{i}")
            sql += f"\tLEAD(`{param}`, {i}, {default_val}) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `{param}Lead{i}`,\n"
            weather_col_lst.append(f"{param}Lead{i}")
        sql += "\n"
    # Remove the last comma
    sql = sql[:-3]
    sql += f"\n    FROM Weather\n    WHERE Timestamp BETWEEN '{start}' AND '{end}'"
    return sql

weather_sql = generate_weather_sql(params_lst)


flight_sql = """
    SELECT *, 
        TO_TIMESTAMP_NTZ(DATE_TRUNC('hour', `ScheduledDepartureTimestamp`)) AS `DepartureTimestamp`,
        TO_TIMESTAMP_NTZ(DATE_TRUNC('hour', `ScheduledArrivalTimestamp`)) AS `ArrivalTimestamp`
    FROM Flight
"""

sql = f"WITH WeatherData AS (\n{weather_sql}\n),\n"
sql += f"FlightData AS ({flight_sql})\n"
sql += "SELECT\n    f.*,\n"
for col in weather_col_lst:
    sql += f"    w1.`{col}` AS `Departure{col}`, w2.`{col}` AS `Arrival{col}`,"
    sql += "\n"
sql = sql[:-2]
sql += """
FROM FlightData f 
    LEFT JOIN WeatherData w1 
        ON f.`DepartureTimestamp` = w1.`Timestamp` AND f.`OriginAirport` = w1.`Airport`
    LEFT JOIN WeatherData w2 
        ON f.`ArrivalTimestamp` = w2.`Timestamp` AND f.`DestinationAirport` = w2.`Airport`
    WHERE w1.`Temperature` IS NOT NULL OR w2.`Temperature` IS NOT NULL
"""

joined_df = spark.sql(sql)

departure_weather_col_lst = [f"Departure{col}" for col in weather_col_lst]
arrival_weather_col_lst = [f"Arrival{col}" for col in weather_col_lst]

# Mean value imputation
DEFAULT_WEATHER_VALUES = {
    "Temperature": 15.2,
    "DewPoint": 8.0,
    "Humidity": 67.2,
    "Precipitation": 0.2,
    "WindSpeed": 12.8,
    "Pressure": 1014.8,
    "WeatherCode": 1,
    "WeatherCondition": "Clear",
    "ExtremeWeather": False
}

NEW_DEFAULT_WEATHER_VALUES = {}
for new_col in departure_weather_col_lst + arrival_weather_col_lst:
    for col, default_val in DEFAULT_WEATHER_VALUES.items():
        if col in new_col:
            NEW_DEFAULT_WEATHER_VALUES[new_col] = default_val


departure_window_spec = Window.partitionBy("DepartureTimestamp", "OriginState")
arrival_window_spec = Window.partitionBy("ArrivalTimestamp", "DestinationState")

#Fill NA values
joined_df_filled = joined_df
for col_name in departure_weather_col_lst:
    joined_df_filled = joined_df_filled.withColumn(
        col_name,
        F.coalesce(
            F.col(col_name),
            F.first(col_name, ignorenulls=True).over(departure_window_spec),
            F.lit(NEW_DEFAULT_WEATHER_VALUES[col_name])
        )
    )

for col_name in arrival_weather_col_lst:
    joined_df_filled = joined_df_filled.withColumn(
        col_name,
        F.coalesce(
            F.col(col_name),
            F.first(col_name, ignorenulls=True).over(arrival_window_spec),
            F.lit(NEW_DEFAULT_WEATHER_VALUES[col_name])
        )
    )

joined_df_filled = joined_df_filled.drop("DepartureTimestamp", "ArrivalTimestamp")

joined_df_filled.coalesce(1).write.parquet(f"gs://flight-delay-pred-data/processed/data_{args.year}_{args.month}.parquet", mode="overwrite")

