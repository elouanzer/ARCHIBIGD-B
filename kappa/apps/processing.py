from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,LongType,IntegerType,FloatType,StringType,DoubleType
from pyspark.sql.functions import split,sum,from_json,col, date_format, to_timestamp, greatest, lit, hour, dayofmonth,current_timestamp, expr
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
        col("Timestamp"),
        from_json(col("value").cast("string"), message_schema).alias("data")
        ).select(
            to_timestamp(date_format(col("timestamp"), "yyyy-MM-dd HH:mm"), "yyyy-MM-dd HH:mm").alias("date"),
            "data.grp_identifiant",
            "data.grp_nom",
            "data.grp_statut",
            "data.grp_disponible",
            "data.grp_exploitation",
            "data.grp_complet",
            "data.grp_horodatage",
            "data.idobj",
            col("data.location.lon").alias("longitude"),
            col("data.location.lat").alias("latitude"),
            "data.disponibilite"
        )


    df1.printSchema()
    # dataframe ajouté en parquet pour un traitement en batch
    df_batch = df1.withColumn("jour", dayofmonth(col("date")))\
        .withColumn("heure", hour(col("date")))
    
    # dataframe ajouté a cassandra en stream
    df1 = df1.filter(col("grp_statut") > 0)
    df1 = df1.withColumn("grp_horodatage", to_timestamp(col("grp_horodatage"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
    df1 = df1.withColumn("dispo_pourcentage", 100*(greatest(col("grp_disponible"), lit(0.0)) / col("grp_exploitation")))
    df1 = df1.withColumn("valide", (current_timestamp() - col("grp_horodatage") <= expr("INTERVAL 15 MINUTES")))
    df1.printSchema()

    def writeToCassandra(writeDF, epochId):
        writeDF.write \
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="parking_data", keyspace="projet")\
            .save()
    
    def writeToParquet(writeDF, epochId):
        print(f"Processing epoch {epochId}")
        writeDF.show(5)  # afficher quelques lignes du batch
        writeDF.write\
        .format('parquet')\
        .partitionBy("heure", "jour")\
        .mode("append")\
        .parquet("parquet_data_file")
        print(f"Epoch {epochId} written successfully")


    query1 = df1.writeStream \
        .option("spark.cassandra.connection.host", "cassandra1:9042")\
        .foreachBatch(writeToCassandra) \
        .outputMode("append") \
        .start()

    query2 = df_batch.writeStream \
        .foreachBatch(writeToParquet) \
        .outputMode("append") \
        .trigger(processingTime="600 seconds")\
        .option('checkpointLocation', "./checkpoint")\
        .start()

    query1.awaitTermination()
    query2.awaitTermination()

   

if __name__ == "__main__":
    main()














