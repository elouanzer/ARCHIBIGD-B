spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 --conf spark.cassandra.connection.host=cassandra1 --conf spark.cassandra.connection.port=9042 processing.py parking