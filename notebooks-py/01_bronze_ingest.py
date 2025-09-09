# Databricks notebook: Bronze ingestion - read raw CSV/JSON and write Delta to BRONZE
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession

dbutils.widgets.text("raw_base_csv", "/Workspace/Repos/data/raw/csv")
dbutils.widgets.text("raw_base_json", "/Workspace/Repos/data/raw/json")

raw_base_csv  = dbutils.widgets.get("raw_base_csv")
raw_base_json = dbutils.widgets.get("raw_base_json")

def bronze_path(name): 
    return spark.conf.get("f1.paths.bronze") + f"/{name}"

def with_ingestion(df):
    return df.withColumn("ingestion_timestamp", F.current_timestamp())

# Circuits (CSV)
circuits_schema = StructType([
    StructField("circuitId", IntegerType(), True),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True),
])
circuits_df = (spark.read.option("header", True)
                .schema(circuits_schema)
                .csv(f"{raw_base_csv}/circuits.csv"))
with_ingestion(circuits_df)    .write.format("delta").mode("overwrite")    .option("mergeSchema", "true")    .save(bronze_path("circuits"))

# Races (CSV)
races_schema = StructType([
    StructField("raceId", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
])
races_df = (spark.read.option("header", True)
            .schema(races_schema)
            .csv(f"{raw_base_csv}/races.csv"))
with_ingestion(races_df)    .write.format("delta").mode("overwrite")    .option("mergeSchema", "true")    .save(bronze_path("races"))

# Constructors (JSON)
constructors_df = (spark.read.json(f"{raw_base_json}/constructors.json"))
with_ingestion(constructors_df)    .write.format("delta").mode("overwrite")    .option("mergeSchema", "true")    .save(bronze_path("constructors"))

# Drivers (JSON - nested name)
drivers_df = spark.read.json(f"{raw_base_json}/drivers.json")
drivers_df = (drivers_df
    .withColumn("forename", F.col("name.forename"))
    .withColumn("surname", F.col("name.surname"))
    .drop("name"))
with_ingestion(drivers_df)    .write.format("delta").mode("overwrite")    .option("mergeSchema", "true")    .save(bronze_path("drivers"))

# Results (JSON)
results_df = spark.read.json(f"{raw_base_json}/results.json")
with_ingestion(results_df)    .write.format("delta").mode("overwrite")    .option("mergeSchema", "true")    .save(bronze_path("results"))

# Pit Stops (JSON)
pit_df = spark.read.json(f"{raw_base_json}/pit_stops.json")
with_ingestion(pit_df)    .write.format("delta").mode("overwrite")    .option("mergeSchema", "true")    .save(bronze_path("pit_stops"))

# Lap Times (CSV in folder)
lap_df = spark.read.option("header", True).csv(f"{raw_base_csv}/lap_times")
with_ingestion(lap_df)    .write.format("delta").mode("overwrite")    .option("mergeSchema", "true")    .save(bronze_path("lap_times"))

# Qualifying (CSV in folder for this sample; in real projects often JSON)
qual_df = spark.read.option("header", True).csv(f"{raw_base_csv}/qualifying")
with_ingestion(qual_df)    .write.format("delta").mode("overwrite")    .option("mergeSchema", "true")    .save(bronze_path("qualifying"))

print("Bronze ingestion complete.")
