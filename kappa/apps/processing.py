from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,LongType,IntegerType,FloatType,StringType
from pyspark.sql.functions import split,sum,from_json,col
import sys

def main():

    message_schema = StructType([
        StructField("grp_identifiant", StringType(), False),
        StructField("grp_nom", StringType(), False),
        StructField("grp_statut", IntegerType(), False),
        StructField("grp_disponible", IntegerType(), False),
        StructField("grp_exploitation", IntegerType(), False),
        StructField("grp_complet", IntegerType(), False),
        StructField("grp_horodatage", StringType(), False),
        StructField("idobj", StringType(), False),
        StructField("location", StructType([
            StructField("lon", DoubleType(), False),
            StructField("lat", DoubleType(), False)
        ]), False),
        StructField("disponibilite", IntegerType(), False)
    ])

    spark = SparkSession.builder \
        .appName("Spark-Kafka-Cassandra") \
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")

    topic = sys.argv[1]

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", topic) \
        .option("delimeter",",") \
        .option("startingOffsets", "latest") \
        .load()
    '''
    df1 = df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), message_schema).alias("data")) \
        .select("data.*")
    '''
    df1 = df.select(
        col("timestamp"),
        from_json(col("value").cast("string"), message_schema).alias("data")
    ).select("timestamp", "data.*")

    df1.printSchema()

    df1.filter(col("grp_statut") > 0)
    df1 = df1.withColumn("dispo_pourcentage", (max(0,col("grp_disponible")) / col("grp_exploitation")) * 100)

    df1.printSchema()

    def writeToCassandra(writeDF, epochId):
        writeDF.write \
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="transactions1", keyspace="demo")\
            .save()

    df1.writeStream \
        .option("spark.cassandra.connection.host","cassandra1:9042")\
        .foreachBatch(writeToCassandra) \
        .outputMode("update") \
        .start()\
        .awaitTermination()
   

if __name__ == "__main__":
    main()














