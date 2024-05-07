from faker import Faker
import paho.mqtt.client as mqtt
import random
from faker.providers import color
import threading
import json
from datetime import datetime

# MQTT Settings
MQTT_Broker = "test.mosquitto.org"
MQTT_Port = 1883

# Topics for each sensor
MQTT_Topics = {
    "Temperature": "Factory/Machine1/1/Temperature",
    "Humidity": "Factory/Machine1/2/Humidity",
    "Flow": "Factory/Machine1/3/Flow",
    "Position": "Factory/Machine1/4/Position",
    "PaintLevel": "Factory/Machine1/5/PaintLevel",
    "SurfaceQuality": "Factory/Machine1/6/SurfaceQuality"
}

# Subscribe to all Sensors at Base Topic
mqttc = mqtt.Client()
mqttc.connect(MQTT_Broker, MQTT_Port)


def publish_To_Topic(topic, message):
    mqttc.publish(topic, message)
    print("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))
    print("")


def publish_Fake_Sensor_Values_to_MQTT():
    threading.Timer(1.0, publish_Fake_Sensor_Values_to_MQTT).start()

    # Generate fake sensor values
    Humidity_Fake_Value = float("{0:.2f}".format(random.uniform(50, 90.01)))
    Temperature_Fake_Value = float("{0:.2f}".format(random.uniform(30, 60.01)))
    Flow_Fake_Value = float("{0:.2f}".format(random.uniform(0, 50.01)))
    Position_Fake_Value = int(random.randint(0, 1))
    PaintLevel_Fake_Value = float("{0:.2f}".format(random.uniform(0, 1000.01)))
    SurfaceQuality_Fake_Value = int(random.randint(0, 1))

    # Prepare data for each sensor and publish to its specific topic
    for sensor, value in {
        "Temperature": Temperature_Fake_Value,
        "Humidity": Humidity_Fake_Value,
        "Flow": Flow_Fake_Value,
        "Position": Position_Fake_Value,
        "PaintLevel": PaintLevel_Fake_Value,
        "SurfaceQuality": SurfaceQuality_Fake_Value
    }.items():
        sensor_topic = MQTT_Topics[sensor]
        sensor_data_json = json.dumps({
            "Sensor_ID": f"Type-{list(MQTT_Topics.keys()).index(sensor) + 1}",
            "Date": datetime.today().strftime("%d-%b-%Y %H:%M:%S:%f"),
            sensor: value
        })
        publish_To_Topic(sensor_topic, sensor_data_json)


publish_Fake_Sensor_Values_to_MQTT()
