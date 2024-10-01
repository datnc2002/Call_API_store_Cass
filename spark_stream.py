import logging
from datetime import datetime

# from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from uuid import uuid4
from pyspark.sql.functions import lit

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            weather TEXT,
            temperature TEXT,
            temperature_min TEXT,
            temperature_max TEXT,
            pressure TEXT,
            humidity TEXT,
            wind_speed TEXT,
            clouds TEXT,
            location TEXT);
    """)
    print("Table created successfully!")

def insert_data(session, batch_id, **kwargs):
    print("Inserting data ...")
    
    # id = uuid4()
    weather = kwargs.get('weather')
    temperature = kwargs.get('temperature')
    temperature_min = kwargs.get('temperature_min')
    temperature_max = kwargs.get('temperature_max')
    pressure = kwargs.get('pressure')
    humidity = kwargs.get('humidity')
    wind_speed = kwargs.get('wind_speed')
    clouds = kwargs.get('clouds')
    location = kwargs.get('location')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, weather, temperature, temperature_min,
                        temperature_max, pressure, humidity, wind_speed, clouds, location)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (id, weather, temperature, temperature_min,
                        temperature_max, pressure, humidity, wind_speed, clouds, location))
        logging.info(f"Data inserted for {id}")
        # current_id += 1
    except Exception as e:
        logging.error(f"Could not insert data due to {e}")

def create_spark_connection():
    s_conn = None
    
    try:
        # s_conn = SparkSession.builder \
        #     .appName("SparkDataStreaming") \
        #     .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
        #                                     "com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.4.1,"
        #                                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        #     .config('spark.cassandra.connection.host','172.18.0.3') \
        #     .config('spark.kafka.bootstrap.servers','172.18.0.3:29092') \
        #     .getOrCreate()

        s_conn = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                            "com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.4.1,"
                                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host','cassandra') \
            .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def create_cassandra_connection():
    try:
        cluster = Cluster(['cassandra'])
        # cluster = Cluster(['172.18.0.3'])

        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None
    
def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        # spark_df = spark_conn.readStream \
        #     .format('kafka') \
        #     .option('kafka.bootstrap.servers', '172.18.0.3:29092') \
        #     .option('subscribe', 'user_created') \
        #     .option('startingOffsets', 'earliest') \
        #     .load()

        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'user_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully!")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created due to: {e}")
    
    return spark_df

def create_selection_df_from_kafka(spark_df):
    # global current_id
    schema = StructType([
        StructField("weather", StringType(), False),
        StructField("temperature", StringType(), False),
        StructField("temperature_min", StringType(), False),
        StructField("temperature_max", StringType(), False),
        StructField("pressure", StringType(), False),
        StructField("humidity", StringType(), False),
        StructField("wind_speed", StringType(), False),
        StructField("clouds", StringType(), False),
        StructField("location", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value as STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    
    sel_with_id = sel.withColumn("id", lit(str(uuid4())))
    # sel_with_id = sel.withColumn("id", lit(str(current_id)))

    sel_with_id.writeStream \
        .format('console') \
        .outputMode('append') \
        .start()
    
    # current_id += 1

    return sel_with_id

if __name__ == "__main__":
    # current_id = 1
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()
        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Steaming is being started ...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .outputMode("append")
                               .trigger(processingTime='2 second')
                               .start())
            streaming_query.awaitTermination()


    # spark_conn = create_spark_connection()

    # if spark_conn is not None:
    #     spark_df = connect_to_kafka(spark_conn)
    #     selection_df = create_selection_df_from_kafka(spark_df)
    #     # Dùng foreachBatch để xử lý và ghi vào Cassandra
    #     def write_to_cassandra(batch_df, batch_id):
    #         session = create_cassandra_connection()
    #         if session is not None:
    #             create_keyspace(session)
    #             create_table(session)
                
    #             # Ghi DataFrame vào Cassandra
    #             batch_df.write \
    #                 .format("org.apache.spark.sql.cassandra") \
    #                 .options(keyspace="spark_streams", table="created_users") \
    #                 .mode("append") \
    #                 .save()
                
    #             logging.info(f"Batch {batch_id} written to Cassandra.")

    #     logging.info("Streaming is being started ...")

    #     streaming_query = (selection_df.writeStream
    #                        .foreachBatch(write_to_cassandra)
    #                        .start())

    #     streaming_query.awaitTermination()
