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
MQTT_Topic_A = "Factory/Machine1/1/A"
MQTT_Topic_B = "Factory/Machine1/2/B"
MQTT_Topic_W = "Factory/Machine1/3/W"
MQTT_Topic_X = "Factory/Machine1/4/X"
MQTT_Topic_Y = "Factory/Machine1/5/Y"
MQTT_Topic_Z = "Factory/Machine1/6/Z"

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
    fake = Faker()

    if toggle == 0:
        B_Fake_Value = float("{0:.2f}".format(random.uniform(50, 90.01)))

        B_Data = {}
        B_Data['Sensor_ID'] = "Type-1"
        B_Data['Date'] = datetime.today().strftime("%d-%b-%Y %H:%M:%S:%f")
        B_Data['B'] = B_Fake_Value
        B_json_data = json.dumps(B_Data)

        print("Publishing fake B Value: " + str(B_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_B, B_json_data)
        toggle = 1

    elif toggle == 1:
        A_Fake_Value = float("{0:.2f}".format(random.uniform(30, 60.01)))

        A_Data = {}
        A_Data['Sensor_ID'] = "Type-2"
        A_Data['Date'] = datetime.today().strftime("%d-%b-%Y %H:%M:%S:%f")
        A_Data['A'] = A_Fake_Value
        A_json_data = json.dumps(A_Data)

        print("Publishing fake A Value: " + str(A_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_A, A_json_data)
        toggle = 2

    elif toggle == 2:
        W_Fake_Value = float("{0:.2f}".format(random.uniform(0, 50.01)))

        W_Data = {}
        W_Data['Sensor_ID'] = "Type-3"
        W_Data['Date'] = datetime.today().strftime("%d-%b-%Y %H:%M:%S:%f")
        W_Data['W'] = W_Fake_Value
        W_json_data = json.dumps(W_Data)

        print("Publishing fake W Value: " + str(W_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_W, W_json_data)
        toggle = 3

    elif toggle == 3:
        X_Fake_Value = int(random.randint(0,1))

        X_Data = {}
        X_Data['Sensor_ID'] = "Type-4"
        X_Data['Date'] = datetime.today().strftime("%d-%b-%Y %H:%M:%S:%f")
        X_Data['X'] = X_Fake_Value
        x_json_data = json.dumps(X_Data)

        print("Publishing fake X Value: " + str(X_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_X, x_json_data)
        toggle = 4

    elif toggle == 4:
        Y_Fake_Value = float("{0:.2f}".format(random.uniform(0, 1000.01)))

        Y_Data = {}
        Y_Data['Sensor_ID'] = "Type-5"
        Y_Data['Date'] = datetime.today().strftime("%d-%b-%Y %H:%M:%S:%f")
        Y_Data['Y'] = Y_Fake_Value
        y_json_data = json.dumps(Y_Data)

        print("Publishing fake Y Value: " + str(Y_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_Y, y_json_data)
        toggle = 5

    elif toggle == 5:
        Z_Fake_Value = int(random.randint(0,1))

        Z_Data = {}
        Z_Data['Sensor_ID'] = "Type-6"
        Z_Data['Date'] = datetime.today().strftime("%d-%b-%Y %H:%M:%S:%f")
        Z_Data['Z'] = Z_Fake_Value
        z_json_data = json.dumps(Z_Data)

        print("Publishing fake Z Value: " + str(Z_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_Z, z_json_data)
        toggle = 0


publish_Fake_Sensor_Values_to_MQTT()
