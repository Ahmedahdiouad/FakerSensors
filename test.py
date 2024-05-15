import paho.mqtt.client as mqtt
import random
import json
import threading
from datetime import datetime
import time

# MQTT Brokers Settings
brokers = [
    {"broker": "test.mosquitto.org", "port": 1883, "topics": {
        "Temperature": "Factory/Machine1/1/Temperature",
        "Humidity": "Factory/Machine1/2/Humidity",
        "Flow": "Factory/Machine1/3/Flow",
        "Position": "Factory/Machine1/4/Position",
        "PaintLevel": "Factory/Machine1/5/PaintLevel",
        "SurfaceQuality": "Factory/Machine1/6/SurfaceQuality"
    }},
    {"broker": "test.mosquitto.org", "port": 1883, "topics": ["machines_1", "machines_2", "machines_3", "machines_4"]}
]

# Function to publish data to MQTT broker
def publish_to_broker(client, broker, topic, message):
    if isinstance(topic, str):
        client.publish(topic, message, qos=2)
    else:
        client.publish(topic, json.dumps(message))

# Function to generate and publish fake sensor values to MQTT brokers
def publish_fake_sensor_values():
    threading.Timer(1.0, publish_fake_sensor_values).start()

    for broker_info in brokers:
        client = mqtt.Client()
        client.connect(broker_info["broker"], broker_info["port"])

        for sensor, value in generate_sensor_data().items():
            topic = broker_info["topics"][sensor] if isinstance(broker_info["topics"], dict) else random.choice(broker_info["topics"])
            message = {
                "Sensor_ID": f"Type-{list(broker_info['topics'].keys()).index(sensor) + 1}",
                "Date": datetime.today().strftime("%d-%b-%Y %H:%M:%S:%f"),
                sensor: value
            }
            publish_to_broker(client, broker_info["broker"], topic, message)

# Function to generate fake sensor data
def generate_sensor_data():
    return {
        "Temperature": round(random.uniform(30, 60), 2),
        "Humidity": round(random.uniform(50, 90), 2),
        "Flow": round(random.uniform(0, 50), 2),
        "Position": random.randint(0, 1),
        "PaintLevel": round(random.uniform(0, 1000), 2),
        "SurfaceQuality": random.randint(0, 1)
    }

# Start publishing fake sensor values to MQTT brokers
publish_fake_sensor_values()
