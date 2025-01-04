import logging
import sys
from typing import Optional

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('spark_streaming.log')
    ]
)
logger = logging.getLogger(__name__)

def create_spark_connection() -> Optional[SparkSession]:
    """
    Creates and configures Spark Session with Kafka and Cassandra connectors.
    
    Returns:
        SparkSession or None if connection fails
    """
    try:
        spark = SparkSession.builder \
            .appName("SparkStructuredStreaming") \
            .config("spark.executor.cores", "1")\
            .config("spark.cores.max", "1") \
            .config("spark.default.parallelism", "1") \
            .config("spark.jars.packages", 
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .config("spark.maxRemoteBlockSizeFetchToMem", "1g") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        logger.info('Spark session created successfully')
        return spark
    
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        return None

def connect_to_kafka(spark_session: SparkSession) -> Optional[DataFrame]:
    """
    Creates Kafka streaming DataFrame.
    
    Args:
        spark_session: Active SparkSession
    
    Returns:
        DataFrame or None if connection fails
    """
    try:
        df = spark_session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "Machines_Data_API") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        logger.info("Kafka streaming DataFrame created successfully")
        return df
    
    except Exception as e:
        logger.error(f"Failed to create Kafka DataFrame: {e}")
        return None

def get_machines_schema() -> StructType:
    """
    Defines the schema for incoming machine data.
    
    Returns:
        StructType describing the data schema
    """
    return StructType([
        StructField("udi", IntegerType(), True),
        StructField("failure_type", StringType(), True),
        StructField("target", IntegerType(), True),
        StructField("process_temperature", DoubleType(), True),
        StructField("product_id", StringType(), True),
        StructField("rotational_speed", IntegerType(), True),
        StructField("air_temperature", DoubleType(), True),
        StructField("tool_wear", IntegerType(), True),
        StructField("torque", DoubleType(), True),
        StructField("type", StringType(), True),
        StructField("date", StringType(), True),
        StructField("day_of_week", StringType(), True),
        StructField("failure_type_id", DoubleType(), True),
        StructField("manufacturer", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("quarter", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("type_id", IntegerType(), True)
    ])

def create_selection_df_from_kafka(spark_df: DataFrame) -> Optional[DataFrame]:
    """
    Transforms Kafka DataFrame to structured DataFrame.
    
    Args:
        spark_df: Raw Kafka DataFrame
    
    Returns:
        Structured DataFrame or None
    """
    try:
        schema = get_machines_schema()
        sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")
        
        logger.info("Successfully transformed Kafka DataFrame")
        return sel
    
    except Exception as e:
        logger.error(f"Failed to transform DataFrame: {e}")
        return None

def create_cassandra_connection() -> Optional[Cluster]:
    """
    Creates Cassandra cluster connection.
    
    Returns:
        Cassandra session or None
    """
    try:
        cluster = Cluster(['cassandra'])
        session = cluster.connect()
        logger.info("Cassandra connection established")
        return session
    
    except Exception as e:
        logger.error(f"Cassandra connection failed: {e}")
        return None

def create_keyspace_and_table(session):
    """
    Creates Cassandra keyspace and table if not exists.
    
    Args:
        session: Cassandra session
    """
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams_machines_api
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        
        session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams_machines_api.machines_data_api (
            udi INT PRIMARY KEY,
            failure_type TEXT,
            target INT,
            process_temperature DOUBLE,
            product_id TEXT,
            rotational_speed INT,
            air_temperature DOUBLE,
            tool_wear INT,
            torque DOUBLE,
            type TEXT,
            date TEXT,
            day_of_week TEXT,
            failure_type_id DOUBLE,
            manufacturer TEXT,
            product_category TEXT,
            product_name TEXT,
            quarter INT,
            timestamp TEXT,
            type_id INT
        );""")
        
        logger.info("Keyspace and table created successfully")
    
    except Exception as e:
        logger.error(f"Failed to create keyspace/table: {e}")
        sys.exit(1)

def log_batch_statistics():
    """
    Logs statistics about each micro-batch.

    Args:
        batch_df: The DataFrame of the current batch.
        batch_id: The ID of the batch.
    """
    logger.info("Streaming query started. Awaiting termination...Batch ID\n:")

def main():
    """
    Main orchestration function for Spark Streaming pipeline.
    """
    # Create Spark connection
    spark_conn = create_spark_connection()
    if not spark_conn:
        logger.error("Failed to establish Spark connection. Exiting.")
        sys.exit(1)

    # Connect to Kafka
    spark_df = connect_to_kafka(spark_conn)
    if not spark_df:
        logger.error("Failed to connect to Kafka. Exiting.")
        sys.exit(1)

    # Transform DataFrame
    selection_df = create_selection_df_from_kafka(spark_df)
    if not selection_df:
        logger.error("DataFrame transformation failed. Exiting.")
        sys.exit(1)

    # Create Cassandra connection
    session = create_cassandra_connection()
    if not session:
        logger.error("Cassandra connection failed. Exiting.")
        sys.exit(1)

    # Create keyspace and table
    create_keyspace_and_table(session)

    try:
        # Start streaming query
        streaming_query = (selection_df.writeStream
            .foreach(log_batch_statistics) # Log batch info
            .format("org.apache.spark.sql.cassandra")
            .option('checkpointLocation', '/tmp/checkpoint1')
            .option('keyspace', 'spark_streams_machines_api')
            .option('table', 'machines_data_api')
            .start())

        logger.info("Streaming query started. Awaiting termination...")
        streaming_query.awaitTermination()

    except Exception as e:
        logger.error(f"Streaming query failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()