-- Create databases with external locations (adjust for your environment)
-- Option A: Hive Metastore external locations
CREATE DATABASE IF NOT EXISTS f1_bronze;
CREATE DATABASE IF NOT EXISTS f1_silver;
CREATE DATABASE IF NOT EXISTS f1_gold;

-- After running notebooks, you can register Delta locations as tables, e.g.:
-- CREATE TABLE IF NOT EXISTS f1_silver.circuits USING DELTA LOCATION '<silver-base>/circuits';
