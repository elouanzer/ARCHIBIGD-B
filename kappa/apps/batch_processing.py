from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, avg, countDistinct
from cassandra.cluster import Cluster
import os

def list_valid_dirs(base_path, prefix):
    return [os.path.join(base_path, d) for d in os.listdir(base_path) if d.startswith(prefix)]

def load_and_aggregate_parquet(base_path):
    spark = SparkSession.builder.appName("ParquetAggregator").getOrCreate()
    
    heure_dirs = list_valid_dirs(base_path, "heure=")
    results = []
    
    for heure_dir in heure_dirs:
        heure_value = heure_dir.split("=")[-1]
        jour_dirs = list_valid_dirs(heure_dir, "jour=")
        
        for jour_dir in jour_dirs:
            jour_value = jour_dir.split("=")[-1]
            parquet_files = [os.path.join(jour_dir, f) for f in os.listdir(jour_dir) if f.endswith(".parquet")]
            
            if parquet_files:
                df = spark.read.parquet(*parquet_files)
                aggregated_df = (df.groupBy("grp_nom", "grp_identifiant")
                                   .agg(avg("disponibilite").alias("moyenne_disponibilite")))
                
                aggregated_df = aggregated_df.withColumn("heure", lit(heure_value))
                aggregated_df = aggregated_df.withColumn("jour", lit(jour_value))
                results.append(aggregated_df)
    
    if results:
        final_df = results[0]
        for df in results[1:]:
            final_df = final_df.unionByName(df)
        return final_df
    else:
        print("Aucun fichier Parquet trouvé.")
        return None

def create_cassandra_table():
    clstr = Cluster(['172.18.0.4'])
    session = clstr.connect()
    
    session.execute('''CREATE KEYSPACE IF NOT EXISTS projet 
                       WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1};''')
    
    session.execute('''CREATE TABLE IF NOT EXISTS projet.parking_batch_1 (
                       heure int,
                       jour int,
                       grp_identifiant int,
                       grp_nom text,
                       moyenne_disponibilite double,
                       PRIMARY KEY ((heure, jour), grp_identifiant)
                       );''')

def write_to_cassandra(df):
    df.write \
      .format("org.apache.spark.sql.cassandra") \
      .mode("append") \
      .options(table="parking_batch_1", keyspace="projet") \
      .save()

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Spark-Kafka-Cassandra") \
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    base_directory = "parquet_data_file"
    create_cassandra_table()
    final_df = load_and_aggregate_parquet(base_directory)
    
    if final_df:
        print(f"Nombre total de lignes : {final_df.count()}")
        print(f"Nombre de valeurs uniques de heure : {final_df.select(countDistinct('heure')).collect()[0][0]}")
        final_df.show()
        write_to_cassandra(final_df)
    
    spark.stop()