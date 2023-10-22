from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json
from pyspark.sql.types import StructType,StructField, StringType
from connectors.sink.database import PostgreSQLWriter
from connectors.source.kafka import KafkaReader

# Create spark session
sparkSession = SparkSession\
    .builder\
    .appName("ClickEventFilter")\
    .getOrCreate()

# Read from Kafka source
# Note: Multiple Kafka topic reads can be useful for merging two different topic data.
kafkaReader = KafkaReader(sparkSession, 
                 servers=["localhost:9094"], 
                 topics=["clicks"],
                 readOffset="earliest")

df = kafkaReader.readStream()

df_out = df.selectExpr("CAST(value AS STRING)")

schema = StructType([  
    StructField("id",StringType(),True), 
    StructField("ip",StringType(),True), 
    StructField("dow",StringType(),True),
  ])

# Tranform
df_sql = df_out.select(
    from_json(col("value"), schema)
    .alias("data")
    ).select("data.*") # Flat json to columns


# Write to PostgresDB
query = PostgreSQLWriter().writeStream(df_sql)

query.awaitTermination()