from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, TopicAlreadyExistsError, UnknownTopicOrPartitionError
import pandas as pd
import json


class KafkaAPI:
    def __init__(self, bootstrap_servers='localhost:9092', client_id='test', group_id='test-consumer-group'):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.group_id = group_id
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id
        )
        self.producer = KafkaProducer(bootstrap_servers=[self.bootstrap_servers])
        self.consumer = None

    def create_topic(self, topic_name, num_partitions=1, replication_factor=1):
        topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
        self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{topic_name}' created.")

    def delete_topic(self, topic_name):
        self.admin_client.delete_topics(topics=[topic_name])
        print(f"Topic '{topic_name}' deleted.")

    def insert_to_topic(self, df, topic_name):
        for index, row in df.iterrows():
            data = row.to_json().encode('utf-8')
            key = str(index).encode('utf-8')
            self.producer.send(topic=topic_name, key=key, value=data)
        print(f"Data inserted into topic '{topic_name}'.")

    def read_from_topic(self, topic_name, timeout_ms=10000):
        try:
            consumer = KafkaConsumer(
                topic_name,
                group_id=self.group_id,
                bootstrap_servers=[self.bootstrap_servers],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=timeout_ms  # Timeout for stopping the consumer after a period of inactivity
            )
            data = []
            for message in consumer:
                data.append(message.value)
            df = pd.DataFrame(data)
            return df
        except KafkaError as e:
            print(f"Failed to read from topic '{topic_name}': {e}")
            return pd.DataFrame()


# Пример использования
if __name__ == "__main__":
    kafka_api = KafkaAPI()

    # # Создание топика
    # kafka_api.create_topic('Spotify_data')
    #
    # # Запись данных в топик
    # df = pd.read_excel('Spotify_data.xlsx')
    # kafka_api.insert_to_topic(df, 'Spotify_data')

    # Чтение данных из топика
    # df_from_kafka = kafka_api.read_from_topic('Spotify_data')
    # print(df_from_kafka)

    # Удаление топика
    kafka_api.delete_topic('Spotify_data')
