from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pandas.tseries.holiday import USFederalHolidayCalendar
from datetime import datetime
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--year", type=int, required=True)
parser.add_argument("--month", type=int, required=True)
args = parser.parse_args()

spark = SparkSession.builder \
    .appName(f"Process flight data for {args.year}-{args.month}") \
    .getOrCreate()

airline_name_df = spark.read.csv("gs://flight-delay-pred-data/raw/airline_lookup.csv", header=True, inferSchema=True)

flight_df = spark.read.csv(f"gs://flight-delay-pred-data/raw/flight_data_{args.year}_{args.month}.csv", header=True, inferSchema=True)

flight_df = flight_df \
    .join(airline_name_df, flight_df["IATA_CODE_Reporting_Airline"] == airline_name_df["Code"], "inner")

# Filter null values
non_null_renamed_cols = {
    "Year": "Year",
    "Quarter": "Quarter",
    "Month": "Month",
    "DayofMonth": "DayOfMonth",
    "DayOfWeek": "DayOfWeek",
    "FlightDate": "FlightDate",
    "IATA_CODE_Reporting_Airline": "IATACode",
    "Description": "AirlineName",
    "Origin": "OriginAirport",
    "OriginCityName": "OriginCity",
    "OriginState": "OriginState",
    "Dest": "DestinationAirport",
    "DestCityName": "DestinationCity",
    "DestState": "DestinationState",
    "CRSDepTime": "ScheduledDepartureTime",
    "DepTime": "ActualDepartureTime",
    "DepDelayMinutes": "DepartureDelayMinutes",
    "DepDel15": "DepartureDelayed",
    "TaxiOut": "TaxiOutTime",
    "TaxiIn": "TaxiInTime",
    "CRSArrTime": "ScheduledArrivalTime",
    "ArrTime": "ActualArrivalTime",
    "ArrDelayMinutes": "ArrivalDelayMinutes",
    "ArrDel15": "ArrivalDelayed",
    "CRSElapsedTime": "ScheduledElapsedTime",
    "ActualElapsedTime": "ActualElapsedTime",
    "AirTime": "AirTime",
    "Cancelled": "Cancelled",
}

null_renamed_cols = {
    "CarrierDelay": "CarrierDelayMinutes",
    "WeatherDelay": "WeatherDelayMinutes",
    "NASDelay": "NASDelayMinutes",
    "SecurityDelay": "SecurityDelayMinutes",
    "LateAircraftDelay": "LateAircraftDelayMinutes",
}

renamed_cols = {**non_null_renamed_cols, **null_renamed_cols}

accepted_iata_codes = ["AA", "B6", "DL", "F9", "AS", "9E", "UA", "NK", "OO", "MQ", "OH", "YX", "WN", "HA", "G4"]

flight_df = flight_df \
    .dropna(subset=list(non_null_renamed_cols.keys())) \
    .select(*[F.col(c).alias(new_c) for c, new_c in renamed_cols.items()]) \
    .filter(F.col("IATACode").isin(accepted_iata_codes))

holidays = USFederalHolidayCalendar().holidays(start=datetime(2000, 1, 1), end=datetime(2030, 1, 1)).strftime("%Y-%m-%d").tolist()

flight_df = flight_df.withColumn("Holiday", F.col("FlightDate").cast("string").isin(holidays))

timestamp_cols = ["ScheduledDepartureTime", "ActualDepartureTime", "ScheduledArrivalTime", "ActualArrivalTime"]
for timestamp_col in timestamp_cols:
    flight_df = flight_df \
        .withColumn(
            timestamp_col + "stamp",
            F.make_timestamp_ntz(
                F.col("Year"), 
                F.col("Month"), 
                F.col("DayOfMonth"), 
                F.floor(F.col(timestamp_col) / 100),
                F.col(timestamp_col) % 100, 
                F.lit(0)
            )
        )

flight_df = flight_df \
    .withColumn("DepartureDelayed", F.col("DepartureDelayed").cast("boolean")) \
    .withColumn("ArrivalDelayed", F.col("ArrivalDelayed").cast("boolean")) \
    .withColumn("Cancelled", F.col("Cancelled").cast("boolean"))


flight_df.coalesce(1).write.parquet(f"gs://flight-delay-pred-data/staging/flight_data_{args.year}_{args.month}.parquet", mode="overwrite")

