import json
import logging
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import psutil
import time
import uuid
import socket

# MQTT Settings
mqtt_broker = "127.0.0.1"
mqtt_topic = "Resource/NodeMCU/Monitoring/#"
mqtt_username = "admin"
mqtt_password = "admin"

# Generate a unique MQTT client ID using UUID and hostname
hostname = socket.gethostname()
mqtt_client_id = f"RaspberryPi-{hostname}-{uuid.uuid4()}"

# Kafka Settings
kafka_topic = "logs"
kafka_broker = "192.168.1.105:9092"
producer = KafkaProducer(
    bootstrap_servers=kafka_broker.split(","),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ResourceMonitor")

def publish_log_to_kafka(level, msg, data=None):
    log_entry = {
        'level': level,
        'message': msg,
        'timestamp': time.time()
    }
    if data:
        log_entry['data'] = data
    producer.send(kafka_topic, log_entry)

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    msg = f"Connected to MQTT broker with result code {rc}"
    logger.info(msg)
    publish_log_to_kafka("INFO", msg)
    client.subscribe(mqtt_topic)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())

        # Gather Raspberry Pi resource data
        pi_data = {
            'cpu_usage': psutil.cpu_percent(interval=1),
            'memory_usage': psutil.virtual_memory().percent,
            'disk_usage': psutil.disk_usage('/').percent,
            'network_stats': psutil.net_io_counters()._asdict(),
        }

        # Combine NodeMCU and Raspberry Pi data
        combined_data = {**payload, **pi_data}

        # Publish combined data to Kafka
        producer.send(kafka_topic, combined_data)

    except Exception as e:
        msg = f"Error processing message: {e}"
        logger.error(msg)
        publish_log_to_kafka("ERROR", msg)

# MQTT Client Setup
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, mqtt_client_id)
client.username_pw_set(mqtt_username, mqtt_password)
client.on_connect = on_connect
client.on_message = on_message

# Connect to MQTT Broker
client.connect(mqtt_broker, 1883, 60)
client.loop_start()

# Keep the script running
while True:
    time.sleep(1)
