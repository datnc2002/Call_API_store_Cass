from kafka import KafkaConsumer
import json

def consume_data():
    consumer = KafkaConsumer(
        'user_created',
        group_id='user_group',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        bootstrap_servers=['localhost:9092']
    )

    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        print(f"Received message: {data}")

if __name__ == "__main__":
    consume_data()
