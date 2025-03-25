from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,LongType,IntegerType,FloatType,StringType,DoubleType
from pyspark.sql.functions import split,sum,from_json,col, date_format, to_timestamp, greatest, lit, hour, dayofmonth,month
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
    .appName("KafkaToParquet") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()



    spark.sparkContext.setLogLevel("ERROR")

    #172.18.0.7:9092

    #topic = sys.argv[1]

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", 'parking') \
        .option("startingOffsets", "latest") \
        .load()
   
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
    
    # Ajouter des colonnes pour partitionner les données
    df1 = df1.withColumn("jour", dayofmonth(col("date")))\
             .withColumn("heure", hour(col("date")))
             


    def writeToParquet(writeDF, epochId):
        print(f"Processing epoch {epochId}")
        writeDF.show(5)  # Afficher quelques lignes du batch
        writeDF.write\
        .format('parquet')\
        .partitionBy("heure","jour")\
        .mode("append")\
        .parquet("parquet_data_file")
        print(f"Epoch {epochId} written successfully")


    df1.writeStream \
        .foreachBatch(writeToParquet) \
        .outputMode("append") \
        .trigger(processingTime="600 seconds")\
        .option('checkpointLocation',"./checkpoint")\
        .start()\
        .awaitTermination()
   

if __name__ == "__main__":
    main()

