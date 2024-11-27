import paho.mqtt.client as mqtt
import requests

# Broker details
BROKER_URL = "1637ad6943df4bfc899071bf9ea27207.s1.eu.hivemq.cloud"  # Replace with your HiveMQ cluster URL
PORT = 8883  # Use port 1883 for secured connections
USERNAME = "samir_123"  # Replace with your HiveMQ username
PASSWORD = "Iamthefirstuser"  # Replace with your HiveMQ password
CLIENT_ID = "subscriber_client_12345"  # Unique ID for this client
TOPIC = "#"  # Topic to subscribe to
QOS = 1  # Quality of Service level

# Define the callback function for receiving messages


# Define the callback function for receiving messages
def on_message(client, userdata, msg):
    message = msg.payload.decode('utf-8')
    print(f"Received message from topic '{msg.topic}': {message}")

    # Send message to NiFi
    nifi_url = "http://localhost:8081/contentListener"  # NiFi URL for ListenHTTP
    try:
        response = requests.post(nifi_url, data=message)
        if response.status_code == 200:
            print("Message sent to NiFi successfully.")
        else:
            print(f"Failed to send message to NiFi: {response.status_code}")
    except requests.RequestException as e:
        print(f"Error sending message to NiFi: {e}")


# Define the MQTT client and authentication
client = mqtt.Client(CLIENT_ID)
client.username_pw_set(USERNAME, PASSWORD)

# Set the callback function for message reception
client.on_message = on_message

# Connect to HiveMQ
client.tls_set()  # Use SSL for secured connection
client.connect(BROKER_URL, PORT)
client.loop_start()

# Subscribe to the topic
client.subscribe(TOPIC, qos=QOS)
print(f"Subscribed to topic '{TOPIC}' and waiting for messages...")

# Keep the client connected to receive messages
try:
    while True:
        pass  # Keep the script running to receive messages
except KeyboardInterrupt:
    print("Stopped receiving messages.")
    client.loop_stop()
    client.disconnect()
