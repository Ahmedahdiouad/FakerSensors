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
MQTT_Topic_Temperature = "Factory/Machine1/1/Temperature"
MQTT_Topic_Humidity = "Factory/Machine1/2/Humidity"
MQTT_Topic_Flow = "Factory/Machine1/3/Flow"
MQTT_Topic_Position = "Factory/Machine1/4/Position"
MQTT_Topic_PaintLevel = "Factory/Machine1/5/PaintLevel"
MQTT_Topic_SurfaceQuality = "Factory/Machine1/6/SurfaceQuality"

# Subscribe to all Sensors at Base Topic
mqttc = mqtt.Client()
mqttc.connect(MQTT_Broker, MQTT_Port)


def publish_To_Topic(topic, message):
    mqttc.publish(topic, message)
    print("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))
    print("")


# FAKE SENSOR

toggle = 0


def publish_Fake_Sensor_Values_to_MQTT():
    threading.Timer(1.0, publish_Fake_Sensor_Values_to_MQTT).start()
    global toggle
    

    if toggle == 0:
        Humidity_Fake_Value = float("{0:.2f}".format(random.uniform(50, 90.01)))

        Humidity_Data = {}
        Humidity_Data['Sensor_ID'] = "Type-1"
        Humidity_Data['Date'] = datetime.today().strftime("%d-%b-%Y %H:%M:%S:%f")
        Humidity_Data['Humidity'] = Humidity_Fake_Value
        Humidity_json_data = json.dumps(Humidity_Data)

        print("Publishing fake Humidity Value: " + str(Humidity_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_Humidity, Humidity_json_data)
        toggle = 1

    elif toggle == 1:
        Temperature_Fake_Value = float("{0:.2f}".format(random.uniform(30, 60.01)))

        Temperature_Data = {}
        Temperature_Data['Sensor_ID'] = "Type-2"
        Temperature_Data['Date'] = datetime.today().strftime("%d-%b-%Y %H:%M:%S:%f")
        Temperature_Data['Temperature'] = Temperature_Fake_Value
        Temperature_json_data = json.dumps(Temperature_Data)

        print("Publishing fake Temperature Value: " + str(Temperature_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_Temperature, Temperature_json_data)
        toggle = 2

    elif toggle == 2:
        Flow_Fake_Value = float("{0:.2f}".format(random.uniform(0, 50.01)))

        Flow_Data = {}
        Flow_Data['Sensor_ID'] = "Type-3"
        Flow_Data['Date'] = datetime.today().strftime("%d-%b-%Y %H:%M:%S:%f")
        Flow_Data['Flow'] = Flow_Fake_Value
        Flow_json_data = json.dumps(Flow_Data)

        print("Publishing fake Flow Value: " + str(Flow_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_Flow, Flow_json_data)
        toggle = 3

    elif toggle == 3:
        Position_Fake_Value = int(random.randint(0,1))

        Position_Data = {}
        Position_Data['Sensor_ID'] = "Type-4"
        Position_Data['Date'] = datetime.today().strftime("%d-%b-%Y %H:%M:%S:%f")
        Position_Data['Position'] = Position_Fake_Value
        Position_json_data = json.dumps(Position_Data)

        print("Publishing fake Position Value: " + str(Position_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_Position, Position_json_data)
        toggle = 4

    elif toggle == 4:
        PaintLevel_Fake_Value = float("{0:.2f}".format(random.uniform(0, 1000.01)))

        PaintLevel_Data = {}
        PaintLevel_Data['Sensor_ID'] = "Type-5"
        PaintLevel_Data['Date'] = datetime.today().strftime("%d-%b-%Y %H:%M:%S:%f")
        PaintLevel_Data['PaintLevel'] = PaintLevel_Fake_Value
        PaintLevel_json_data = json.dumps(PaintLevel_Data)

        print("Publishing fake PaintLevel Value: " + str(PaintLevel_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_PaintLevel, PaintLevel_json_data)
        toggle = 5

    elif toggle == 5:
        SurfaceQuality_Fake_Value = int(random.randint(0,1))

        SurfaceQuality_Data = {}
        SurfaceQuality_Data['Sensor_ID'] = "Type-6"
        SurfaceQuality_Data['Date'] = datetime.today().strftime("%d-%b-%Y %H:%M:%S:%f")
        SurfaceQuality_Data['SurfaceQuality'] = SurfaceQuality_Fake_Value
        SurfaceQuality_json_data = json.dumps(SurfaceQuality_Data)

        print("Publishing fake SurfaceQuality Value: " + str(SurfaceQuality_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_SurfaceQuality, SurfaceQuality_json_data)
        toggle = 0


publish_Fake_Sensor_Values_to_MQTT()
