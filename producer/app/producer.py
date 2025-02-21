import json
import time
import urllib.request
import json
from datetime import datetime
from kafka import KafkaProducer, KafkaClient 
from kafka.admin import KafkaAdminClient, NewTopic

def main():
    """_summary_
    
    Returns:
        _type_: _description_
    """    
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_parkings-publics-nantes-disponibilites/records?limit=-1"

    # topic = sys.argv[1]
    topic = 'parking'
    
    kafka_client = KafkaClient(bootstrap_servers='localhost:9092')
    
    admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
    server_topics = admin.list_topics()

    topic = 'parking'
    num_partition = 1

    print(server_topics)
    # création du topic si celui-ci n'est pas déjà créé
    if topic not in server_topics:
        try:
            print("create new topic :", topic)

            topic1 = NewTopic(name=topic,
                             num_partitions=num_partition,
                             replication_factor=1)
            admin.create_topics([topic1])
        except Exception:
            print("error")
            pass
    else:
        print(topic,"est déjà créé")

    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    while True:
        response = urllib.request.urlopen(url)
        json_file = json.loads(response.read().decode())
        parkings = json_file['results']
        for p in parkings: 
            producer.send(topic, json.dumps(p).encode())
        print("{} Produced {} station records".format(datetime.fromtimestamp(time.time()), len(parkings)))
        time.sleep(600)

if __name__ == "__main__":
    main()
