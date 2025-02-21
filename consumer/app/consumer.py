import time
import json
from datetime import datetime
from kafka import KafkaAdminClient, KafkaConsumer

def consumer_from_kafka(topic,stations):

    parkings={}
    parking_name={}

    consumer = KafkaConsumer("parking", bootstrap_servers='localhost:9092',auto_offset_reset="earliest", enable_auto_commit=True, auto_commit_interval_ms=1000)
    consumer.subscribe(["parking"])

    for message in consumer:
        parking = json.loads(message.value.decode())
        #print(station)
        #print("da",datetime.utcfromtimestamp(station["last_update"]/1000).strftime('%Y-%m-%d %H:%M:%S'))
        parking_id = parking['grp_identifiant']
        parking_name = parking["grp_nom"]
        availability = parking["disponibilite"]

        if parking_name not in parkings:
            parkings[parking_name] = availability
            #print("st",station_number, city_stations[station_number])

        count_diff = availability - parkings[parking_name]
        if count_diff != 0:
            #print(available_bike_stands,city_stations[station_number])
            parkings[parking_name] = availability
            if count_diff > 0:
                print('+', count_diff, parking["grp_nom"])
            else :
                print(count_diff, parking["grp_nom"])
        #else:
            #print("0",station["address"])

    consumer.close()



def main():
    print("consumer : ")
    parkings = {}
    topic = 'parking'
    try:
        admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
    except Exception:
        pass

    consumer_from_kafka(topic,parkings)

if __name__ == "__main__":
    main()
    
