from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, row_number,abs, lit,month, year, round ,count, avg, sum as _sum, max as _max, countDistinct
import logging
import sys
from datetime import datetime


# Configure logging
def setup_logging():
    logger = logging.getLogger('machine_data_etl')
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    log_filename = f'machine_data_etl_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

logger = setup_logging()


def create_spark_session():
    spark = SparkSession.builder \
            .appName("ETLToSnowflake") \
            .config("spark.executor.cores", "1")\
            .config("spark.cores.max", "1") \
            .config("spark.default.parallelism", "1") \
            .config("spark.jars.packages", 
            "net.snowflake:spark-snowflake_2.12:2.11.3-spark_3.3," + 
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .config("spark.maxRemoteBlockSizeFetchToMem", "1g") \
            .getOrCreate()
    logger.info("Spark Session created successfully")
    return spark

# Extract data from Cassandra
def extract_cassandra_data(spark):
    try:
        logger.info("Starting data extraction from Cassandra")
        df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="machines_data_api", keyspace="spark_streams_machines_api") \
            .load()
        logger.info(f"Successfully extracted {df.count()} records from Cassandra")
        return df
    except Exception as e:
        logger.error(f"Error during Cassandra data extraction: {e}")
        raise

# Transform data
def transform_data(df):
    try:
        logger.info("Starting data transformation")
        
        # Constants
        MAX_TOOL_LIFETIME = 100

        # Calculate defect_rate
        defect_rate = df.groupBy("product_id").agg(
            (count(when(col("failure_type") == "Overstrain Failure", 1)) / count("*") * 100).alias("defect_rate")
        )

        # Calculate tool_wear_per_unit
        tool_wear_per_unit = df.groupBy("type").agg(
            round((_sum("tool_wear") / countDistinct("product_id")), 2).alias("tool_wear_per_unit")
        )

        # Calculate tool_lifetime_efficiency
        tool_lifetime_efficiency = df.groupBy("product_id").agg(
            ((lit(MAX_TOOL_LIFETIME) - _max("tool_wear")) / lit(MAX_TOOL_LIFETIME) * 100).alias("tool_lifetime_efficiency")
        )

        # Derive metrics
        df = df.withColumn(
            "deviation_from_target_process_temperature", abs(col("process_temperature") - col("air_temperature"))
        ).withColumn(
            "tool_efficiency", 1 - (col("tool_wear") / MAX_TOOL_LIFETIME)
        )

        # Join calculated metrics back into the main DataFrame
        df = df.join(defect_rate, "product_id", "left") \
            .join(tool_wear_per_unit, "type", "left") \
            .join(tool_lifetime_efficiency, "product_id", "left") 
            # .join(avg_failure_rate, "product_id", "left") \
            # .join(production_yield, "type", "left")

        logger.info(f"Transformation complete. Total records: {df.count()}")
        return df
    except Exception as e:
        logger.error(f"Error during data transformation: {e}")
        raise


def transform_failure_dim(df):
    try:
        logger.info("Transforming data for failure_dim")
        failure_dim = df.select(
            col("failure_type").alias("failure_type"),col("failure_type_id")
        ).distinct()
        logger.info(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~failure_dim transformation complete with {failure_dim.count()} records")
        return failure_dim
    except Exception as e:
        logger.error(f"Error during failure_dim transformation: {e}")
        raise

def transform_type_dim(df):
    try:
        logger.info("Transforming data for type_dim")
        failure_dim = df.select(
            col("type"),col("type_id")
        ).distinct()
        logger.info(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~type_dim transformation complete with {failure_dim.count()} records")
        return failure_dim
    except Exception as e:
        logger.error(f"Error during type_dim transformation: {e}")
        raise

def transform_product_dim(df):
    try:
        logger.info("Transforming data for product_dim")
        product_dim = df.select(
            col("product_id"),
            col("product_name"),  # Assuming these columns exist in your source
            col("product_category"),
            col("manufacturer")
        ).distinct()
        logger.info(f"product_dim transformation complete with {product_dim.count()} records")
        return product_dim
    except Exception as e:
        logger.error(f"Error during product_dim transformation: {e}")
        raise

def transform_dimension_time(df):
    try:
        logger.info("Transforming data for date_dim")
        df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))
        window_spec = Window.orderBy("date", "day_of_week", "quarter", "timestamp")
        # Create dimension table with explicitly generated date_id
        dim_time = df.select(
            col("date"),
            col("day_of_week"), 
            col("quarter"),
            col("timestamp").cast("string").alias("timestamp")
        ).distinct() \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("year", year(col("timestamp")))\
        .withColumn("date_id", row_number().over(window_spec)) 

        logger.info(f"~~~~~~~~date_dim transformation complete with {dim_time.count()} records")
        return dim_time
    except Exception as e:
        logger.error(f"Error during date_dim transformation: {e}")
        raise



def load_to_snowflake(df, table_name, spark):
    try:
        logger.info(f"Loading data into Snowflake table: {table_name}")
        snowflake_options = {
            "sfURL": "https://snitlbk-nj25850.snowflakecomputing.com",
            "sfDatabase": "MACHINES_FAILURES_DATAWAREHOUSE",
            "sfSchema": "DW_Schema",
            "sfWarehouse": "compute_wh",
            "sfRole": "accountadmin",
            "sfUser": "ZED",
            "sfPassword": "Zjrr1.23"
        }
        df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", table_name) \
            .mode("overwrite") \
            .save()
        logger.info(f"Data successfully loaded into {table_name}")
    except Exception as e:
        logger.error(f"Error loading data into Snowflake table {table_name}: {e}")
        raise

def transform_fact_table(transformed_data, date_dim):
    try:
        logger.info("Transforming data for fact_table")
        
        # Join with date dimension to get date_id
        fact_table = transformed_data.join(
            date_dim, 
            (transformed_data.timestamp == date_dim.timestamp),
            "left"
        ).select(
            col("udi").alias("udi"),
            col("failure_type_id").alias("failure_id"),
            col("product_id"),
            col("type_id"),
            
            # Use the joined date_id
            date_dim.date_id.alias("date_id"),
            
            col("process_temperature"),
            col("tool_wear"),
            col("air_temperature"),
            col("rotational_speed"),
            col("torque"),
            col("target"),
            
            # Calculated Measurements (which were already added in transform_data)
            col("deviation_from_target_process_temperature"),
            col("tool_efficiency"),
            col("defect_rate"),
            col("tool_wear_per_unit"),
            # col("avg_failure_rate"),
            col("tool_lifetime_efficiency")
            # col("production_yield")
        )
        
        logger.info(f"fact_table transformation complete with {fact_table.count()} records")
        return fact_table
    except Exception as e:
        logger.error(f"Error during fact_table transformation: {e}")
        raise

# Modify the main function to pass date_dim
def main():
    try:
        logger.info("Starting ETL process")
        spark = create_spark_session()
        raw_data = extract_cassandra_data(spark)
        
        # Transform raw data with all metrics
        transformed_data = transform_data(raw_data)
        
        # Transform and Load Dimension Tables
        failure_dim = transform_failure_dim(transformed_data)
        load_to_snowflake(failure_dim, "failure_dim", spark)

        type_dim = transform_type_dim(transformed_data)
        load_to_snowflake(type_dim, "type_dim", spark)

        product_dim = transform_product_dim(transformed_data)
        load_to_snowflake(product_dim, "product_dim", spark)
        
        # Transform and Load Date Dimension
        date_dim = transform_dimension_time(transformed_data)
        load_to_snowflake(date_dim, "date_dim", spark)
        
        # Transform and Load Fact Table - pass date_dim
        fact_table = transform_fact_table(transformed_data, date_dim)
        load_to_snowflake(fact_table, "fact_table", spark)

        logger.info("ETL Process Completed Successfully")
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise




# Main Function
# def main():
#     try:
#         logger.info("Starting ETL process")
#         spark = create_spark_session()
#         raw_data = extract_cassandra_data(spark)

#         # Transform and Load Dimension Tables
#         failure_dim = transform_failure_dim(raw_data)
#         load_to_snowflake(failure_dim, "failure_dim", spark)

#         type_dim = transform_type_dim(raw_data)
#         load_to_snowflake(type_dim, "type_dim", spark)

#         product_dim = transform_product_dim(raw_data)
#         load_to_snowflake(product_dim, "product_dim", spark)

#         logger.info("ETL Process Completed Successfully")
#     except Exception as e:
#         logger.error(f"ETL process failed: {e}")


if __name__ == "__main__":
    main()
