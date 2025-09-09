# Databricks notebook: Gold curation - create analytics-ready Delta tables
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def s(name): 
    return spark.read.format("delta").load(spark.conf.get("f1.paths.silver") + f"/{name}")

def g_path(name): 
    return spark.conf.get("f1.paths.gold") + f"/{name}"

# Race Results (join multiple silver tables)
race_results = (
    s("results").alias("res")
    .join(s("drivers").alias("drv"), ["driver_id"], "left")
    .join(s("constructors").alias("con"), ["constructor_id"], "left")
    .join(s("races").alias("rac"), ["race_id"], "left")
    .join(s("circuits").alias("cir"), ["circuit_id"], "left")
    .select(
        F.col("rac.race_year").alias("race_year"),
        F.col("rac.race_id").alias("race_id"),
        F.col("rac.round").alias("round"),
        F.col("rac.name").alias("race_name"),
        F.col("rac.race_date").alias("race_date"),
        F.col("cir.circuit_ref").alias("circuit_ref"),
        F.col("cir.location").alias("location"),
        F.col("cir.country").alias("country"),
        F.col("drv.driver_id"),
        F.col("drv.driver_ref").alias("driver_ref"),
        F.col("drv.forename").alias("forename"),
        F.col("drv.surname").alias("surname"),
        F.col("drv.code").alias("driver_code"),
        F.col("con.constructor_id"),
        F.col("con.constructor_ref").alias("constructor_ref"),
        F.col("con.name").alias("constructor_name"),
        F.col("con.nationality").alias("constructor_nationality"),
        F.col("res.grid").alias("grid"),
        F.col("res.position").alias("position"),
        F.col("res.points").alias("points"),
        F.col("res.time").alias("race_time")
    )
)

(race_results.write.format("delta")
    .mode("overwrite").option("overwriteSchema","true").save(g_path("race_results")))

# Driver standings (per season)
driver_standings = (
    race_results
    .groupBy("race_year","driver_id","driver_ref","forename","surname","driver_code")
    .agg(F.sum("points").alias("points"))
    .withColumn("rank", F.dense_rank().over(Window.partitionBy("race_year").orderBy(F.desc("points"))))
)
(driver_standings.write.format("delta")
    .mode("overwrite").option("overwriteSchema","true").save(g_path("driver_standings")))

# Constructor standings (per season)
constructor_standings = (
    race_results
    .groupBy("race_year","constructor_id","constructor_ref","constructor_name")
    .agg(F.sum("points").alias("points"))
    .withColumn("rank", F.dense_rank().over(Window.partitionBy("race_year").orderBy(F.desc("points"))))
)
(constructor_standings.write.format("delta")
    .mode("overwrite").option("overwriteSchema","true").save(g_path("constructor_standings")))

print("Gold curation complete.")
