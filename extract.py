import paho.mqtt.client as mqtt
import time
import random
import json

# Broker details
BROKER_URL = "1637ad6943df4bfc899071bf9ea27207.s1.eu.hivemq.cloud"  # Replace with your HiveMQ cluster URL
PORT = 8883  # Use port 1883 for unsecured or 8883 for secured connections
USERNAME = "samir_123"  # Replace with your HiveMQ username
PASSWORD = "Iamthefirstuser"  # Replace with your HiveMQ password
CLIENT_ID = "publisher_client_12345"  # Unique ID for this client
TOPIC = "industrial/machine/data"  # Topic to publish to
QOS = 1  # Quality of Service level

# Define the MQTT client and authentication
client = mqtt.Client(CLIENT_ID)
client.username_pw_set(USERNAME, PASSWORD)

# Connect to HiveMQ
client.tls_set()  # Use SSL for secured connection
client.connect(BROKER_URL, PORT)
client.loop_start()


# Define a function to publish random data
def publish_data():
    while True:
        # Sample data structure to publish
        data = {
            "machine_id": f"M-{random.randint(1, 100)}",
            "temperature": round(random.uniform(20.0, 100.0), 2),
            "vibration": round(random.uniform(0.0, 5.0), 2),
            "runtime_hours": random.randint(0, 2000),
            "status": random.choice(["active", "idle", "maintenance_required"]),
            "timestamp": int(time.time()),
            "location": random.choice(["Factory A", "Factory B", "Warehouse 1", "Warehouse 2"]),
            "operator_id": f"O-{random.randint(1, 50)}",
            "power_consumption_kw": round(random.uniform(0.5, 10.0), 2),
            "last_maintenance_date": int(time.time() - random.randint(0, 60 * 60 * 24 * 365))
            # Last maintenance up to a year ago
        }

        # Publish the JSON-encoded data
        client.publish(TOPIC, json.dumps(data), qos=QOS)
        print(f"Published message to topic '{TOPIC}': {data}")

        time.sleep(1)  # Publish every second for this example


# Run the publisher
try:
    publish_data()
except KeyboardInterrupt:
    print("Stopped publishing messages.")
    client.loop_stop()
    client.disconnect()
