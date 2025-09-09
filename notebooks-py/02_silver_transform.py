# Databricks notebook: Silver transformations - standardize schemas, types, and keys
from pyspark.sql import functions as F, types as T

def b(name): return spark.read.format("delta").load(spark.conf.get("f1.paths.bronze") + f"/{name}")
def s_path(name): return spark.conf.get("f1.paths.silver") + f"/{name}"

# Circuits
circuits = (b("circuits")
    .select(
        F.col("circuitId").alias("circuit_id").cast("int"),
        F.col("circuitRef").alias("circuit_ref"),
        "name","location","country",
        F.col("lat").alias("latitude").cast("double"),
        F.col("lng").alias("longitude").cast("double"),
        F.col("alt").alias("altitude").cast("int"),
        "url",
        "ingestion_timestamp"
    ))
circuits.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(s_path("circuits"))

# Races
races = (b("races")
    .withColumn("race_date", F.to_date("date"))
    .select(
        F.col("raceId").alias("race_id").cast("int"),
        F.col("year").alias("race_year").cast("int"),
        "round",
        F.col("circuitId").alias("circuit_id").cast("int"),
        "name","race_date","ingestion_timestamp"
    ))
races.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(s_path("races"))

# Constructors
constructors = (b("constructors")
    .select(
        F.col("constructorId").alias("constructor_id").cast("int"),
        F.col("constructorRef").alias("constructor_ref"),
        "name","nationality","ingestion_timestamp"
    ))
constructors.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(s_path("constructors"))

# Drivers
drivers = (b("drivers")
    .select(
        F.col("driverId").alias("driver_id").cast("int"),
        F.col("driverRef").alias("driver_ref"),
        "number","code","forename","surname",
        F.to_date("dob").alias("dob"),
        "nationality","ingestion_timestamp"
    ))
drivers.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(s_path("drivers"))

# Results
results = (b("results")
    .select(
        F.col("resultId").alias("result_id").cast("int"),
        F.col("raceId").alias("race_id").cast("int"),
        F.col("driverId").alias("driver_id").cast("int"),
        F.col("constructorId").alias("constructor_id").cast("int"),
        F.col("grid").cast("int"),
        F.col("position").cast("int"),
        F.col("points").cast("double"),
        "time","ingestion_timestamp"
    ))
results.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(s_path("results"))

# Pit Stops
pit = (b("pit_stops")
    .select(
        F.col("raceId").alias("race_id").cast("int"),
        F.col("driverId").alias("driver_id").cast("int"),
        F.col("stop").cast("int"),
        F.col("lap").cast("int"),
        F.col("time").alias("pit_time"),
        "ingestion_timestamp"
    ))
pit.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(s_path("pit_stops"))

# Lap Times
laps = (b("lap_times")
    .select(
        F.col("raceId").alias("race_id").cast("int"),
        F.col("driverId").alias("driver_id").cast("int"),
        F.col("lap").cast("int"),
        F.col("position").cast("int"),
        "time",
        F.col("milliseconds").cast("int"),
        "ingestion_timestamp"
    ))
laps.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(s_path("lap_times"))

# Qualifying
qual = (b("qualifying")
    .select(
        F.col("qualifyId").alias("qualify_id").cast("int"),
        F.col("raceId").alias("race_id").cast("int"),
        F.col("driverId").alias("driver_id").cast("int"),
        "q1","q2","q3","ingestion_timestamp"
    ))
qual.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(s_path("qualifying"))

print("Silver transformations complete.")
