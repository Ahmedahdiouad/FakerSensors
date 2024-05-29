import paho.mqtt.client as mqtt
import random
import threading
import json
from datetime import datetime

# MQTT Settings
MQTT_Broker = "test.mosquitto.org"
MQTT_Port = 1883

# Topics for each sensor
MQTT_Topics = {
    "Temperature":"Factory/Machine1/prossice/Temperature",
    "Humidity": "Factory/Machine1/event/Humidity",
    "Flow": "Factory/Machine1/prossice/Flow",
    "Position": "Factory/Machine1/event/Position",
    "PaintLevel": "Factory/Machine1/prossice/PaintLevel",
    "SurfaceQuality": "Factory/Machine1/event/SurfaceQuality",
    "Test1": "Factory/Machine2/event/Test1",
    "Test2": "Factory/MachineN/event/Test2",
    "debit": "Factory/Machine1/event/debit"
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
    debit_Fake_Value = float("{0:.2f}".format(random.uniform(30, 60.01)))
    Test1_Fake_Value = float("{0:.2f}".format(random.uniform(30, 60.01)))
    Test2_Fake_Value = float("{0:.2f}".format(random.uniform(30, 60.01)))
    
    

    # Prepare data for each sensor and publish to its specific topic
    for sensor, value in {
        "Temperature": Temperature_Fake_Value,
        "Humidity": Humidity_Fake_Value,
        "Flow": Flow_Fake_Value,
        "Position": Position_Fake_Value,
        "PaintLevel": PaintLevel_Fake_Value,
        "SurfaceQuality": SurfaceQuality_Fake_Value,
        "debit": debit_Fake_Value,
        "Test1": Test1_Fake_Value,
        "Test2": Test2_Fake_Value,
        
        }.items():

        sensor_topic = MQTT_Topics[sensor]
        sensor_data_json = json.dumps({
            "Sensor_ID": f"Type-{list(MQTT_Topics.keys()).index(sensor) + 1}",
            "Date": datetime.today().strftime("%d-%b-%Y %H:%M:%S:%f"),
            sensor: value
        })
        publish_To_Topic(sensor_topic, sensor_data_json)


publish_Fake_Sensor_Values_to_MQTT()
