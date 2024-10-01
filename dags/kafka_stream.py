import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 9, 25, 10, 00)
}

def get_data():
    import requests

    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": "Hanoi",
        "appid": "1af9056958b2d0d54180b6a72ed0729f"
    }
    res = requests.get(url, params=params)
    res = res.json()
    
    return res

def format_data(res):
    data = {}
    coord = res["coord"]
    data["weather"] = res["weather"][0]["main"] + ": " + res["weather"][0]["description"]
    data["temperature"] = f"%.2f" % (float(res["main"]["temp"]) - 273)
    data["temperature_min"] = f"%.2f" % (float(res["main"]["temp_min"]) - 273)
    data["temperature_max"] = f"%.2f" % (float(res["main"]["temp_max"]) - 273)
    data["pressure"] = str(res["main"]["pressure"]) + " hPa"
    data["humidity"] = str(res["main"]["humidity"]) + " %"
    data["wind_speed"] = str(res["wind"]["speed"])
    first_key = next(iter(res["clouds"].keys()))
    value = res["clouds"][first_key]
    data["clouds"] = first_key + ": " + str(value)
    data["location"] = res["name"] + " - " + res["sys"]["country"]
    
    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    res = get_data()
    res = format_data(res)

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    # producer.send('user_created', json.dumps(res).encode('utf-8'))
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('user_created', json.dumps(res).encode('utf-8'))

        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

# if __name__=='__main__':
#     stream_data()

with DAG('user_automation',
            default_args=default_args,
            schedule_interval='@daily',
            catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data',
        python_callable=stream_data
    )