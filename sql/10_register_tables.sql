-- Register silver & gold Delta directories as external tables (replace <silver-base>/<gold-base>)
-- Tip: In Databricks SQL, first SET variables or paste the base paths.

-- SILVER
CREATE TABLE IF NOT EXISTS f1_silver.circuits     USING DELTA LOCATION '<silver-base>/circuits';
CREATE TABLE IF NOT EXISTS f1_silver.races        USING DELTA LOCATION '<silver-base>/races';
CREATE TABLE IF NOT EXISTS f1_silver.constructors USING DELTA LOCATION '<silver-base>/constructors';
CREATE TABLE IF NOT EXISTS f1_silver.drivers      USING DELTA LOCATION '<silver-base>/drivers';
CREATE TABLE IF NOT EXISTS f1_silver.results      USING DELTA LOCATION '<silver-base>/results';
CREATE TABLE IF NOT EXISTS f1_silver.pit_stops    USING DELTA LOCATION '<silver-base>/pit_stops';
CREATE TABLE IF NOT EXISTS f1_silver.lap_times    USING DELTA LOCATION '<silver-base>/lap_times';
CREATE TABLE IF NOT EXISTS f1_silver.qualifying   USING DELTA LOCATION '<silver-base>/qualifying';

-- GOLD
CREATE TABLE IF NOT EXISTS f1_gold.race_results           USING DELTA LOCATION '<gold-base>/race_results';
CREATE TABLE IF NOT EXISTS f1_gold.driver_standings       USING DELTA LOCATION '<gold-base>/driver_standings';
CREATE TABLE IF NOT EXISTS f1_gold.constructor_standings  USING DELTA LOCATION '<gold-base>/constructor_standings';
