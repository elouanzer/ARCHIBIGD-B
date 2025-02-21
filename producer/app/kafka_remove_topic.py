from kafka.admin import KafkaAdminClient


def main():

    admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
    server_topics = admin.list_topics()

    topic = "topic1"
    num_partition = 1

    print(server_topics)
    
    if topic in server_topics:
        try:
            print("delete topic :", topic)

            admin.delete_topics(topics=[topic])
        except Exception:
            print("error")
            pass
    else:
        print("le topic ",topic,"n'existait pas")

if __name__ == "__main__":
    main()
