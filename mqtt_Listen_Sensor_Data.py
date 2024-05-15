
import paho.mqtt.client as mqtt
from store_Sensor_Data_to_DB import sensor_Data_Handler

# MQTT Settings
MQTT_Broker = "test.mosquitto.org"
MQTT_Port = 1883
MQTT_Topic = "Factory/Machine1/#"

# Function to subscribe to all sensors at base topic
def on_connect(client, userdata, flags, rc):
    print("connect")
    client.subscribe(MQTT_Topic, 0)

# Function to save data into database table
def on_message(mosq, obj, msg):
    # Print received data details
    print("MQTT Data Received...")
    print("MQTT Topic:", msg.topic)
    print("Data:", msg.payload.decode())  # Decode payload for better handling

    sensor_Data_Handler(msg.topic, msg.payload)



# Create MQTT client instancez
mqttc = mqtt.Client()

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect

# Connect to MQTT broker
mqttc.connect(MQTT_Broker, int(MQTT_Port))

# Continuously monitor for messages
mqttc.loop_forever()
