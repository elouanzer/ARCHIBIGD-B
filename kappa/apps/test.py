from pyspark.sql import SparkSession

def test_kafka_connection():
    # Crée une session Spark
    spark = SparkSession.builder \
        .appName("Test Kafka Connection") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    # Configuration de l'adresse de Kafka et du topic
    kafka_bootstrap_servers = "kafka:29092"  # Adresse de Kafka dans Docker (remplacez par l'adresse correcte si nécessaire)
    topic = "parking"  # Remplacez avec le nom de votre topic Kafka

    # Lecture en continu des données depuis Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .load()

    # Sélectionner les valeurs du message (clé et valeur)
    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # Affichage des résultats dans la console
    query = df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    # Attente de la fin du streaming
    query.awaitTermination()

if __name__ == "__main__":
    test_kafka_connection()
