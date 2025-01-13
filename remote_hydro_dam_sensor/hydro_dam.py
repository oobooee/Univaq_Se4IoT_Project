import math
import os
import random
import time
import json
import threading
import paho.mqtt.client as mqtt  # type: ignore
import pika  # type: ignore

# MQTT Server Parameters
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = 1883
MQTT_USER = "mg"
MQTT_PASSWORD = "mg"
DAM_FOLDER = os.getenv("DAM_FOLDER")
CLIENT_ID = os.getenv("CLIENT_ID")
# RabbitMQ Parameters
rabbitmq_host = os.getenv("RABBIT_HOST")
queue_name = f'{CLIENT_ID}_Turbin_data'
exchange_value = f'{DAM_FOLDER}_Exchange'
routing_key_value = f'{CLIENT_ID}_Turbin_key'
RABB_USER = "mg"
RABB_PASSWORD = "mg"

# Characteristics of the dam

MAX_HEIGHT = int(os.getenv("MAX_HEIGHT"))
MAX_VOLUME = int(os.getenv("MAX_VOLUME"))
INITIAL_FILL_PERCENTAGE = float(os.getenv("INITIAL_FILL_PERCENTAGE"))
MIN_INFLOW = 0
MAX_INFLOW = int(os.getenv("MAX_INFLOW"))
MIN_OUTFLOW = 0
MAX_OUTFLOW = int(os.getenv("MAX_OUTFLOW"))
EFFICIENCY = 0.99
GRAVITY = 9.81
DENSITY_WATER = 1000
INTERVAL_SECONDS = 5
total_energy_mwh = 0

# Initial lake parameters
current_volume = MAX_VOLUME * INITIAL_FILL_PERCENTAGE
water_level = MAX_HEIGHT * INITIAL_FILL_PERCENTAGE

current_inflow = MIN_INFLOW + (MAX_INFLOW - MIN_INFLOW) * INITIAL_FILL_PERCENTAGE
current_outflow = MIN_OUTFLOW + (MAX_OUTFLOW - MIN_OUTFLOW) * INITIAL_FILL_PERCENTAGE
ENERGY_REQUEST_STEP = 1

# Global states
is_mqtt_connected = False
is_rabbit_connected = False
connection = None
channel = None
mqtt_lock = threading.Lock()
rabbit_lock = threading.Lock()

def on_connect(client, userdata, flags, rc):
    global is_mqtt_connected
    if rc == 0:
        print("Connected to MQTT Broker!")
        is_mqtt_connected = True
    else:
        print(f"Failed to connect to MQTT Broker, return code {rc}")
        is_mqtt_connected = False


def on_disconnect(client, userdata, rc):
    global is_mqtt_connected
    print("Disconnected from MQTT Broker")
    is_mqtt_connected = False


def reconnect_mqtt():
    global is_mqtt_connected
    while not is_mqtt_connected:
        try:
            with mqtt_lock:
                client.connect(MQTT_BROKER, MQTT_PORT, 60)
                client.loop_start()
                print("Reconnected to MQTT Broker")
                break
        except Exception as e:
            print("MQTT connection failed, retrying in 10 seconds:", str(e))
            time.sleep(10)

client = mqtt.Client(CLIENT_ID)
client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
threading.Thread(target=reconnect_mqtt, daemon=True).start()

def connect_to_rabbitmq():
    global connection, channel, is_rabbit_connected
    while not is_rabbit_connected:
        try:
            with rabbit_lock:
                credential = pika.PlainCredentials(RABB_USER, RABB_PASSWORD)
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=rabbitmq_host, credentials=credential)
                )
                channel = connection.channel()
                channel.queue_declare(queue=queue_name, durable=True)
                channel.exchange_declare(exchange=exchange_value, exchange_type='direct', durable=True)
                channel.queue_bind(exchange=exchange_value, queue=queue_name, routing_key=routing_key_value)
                print("Connected to RabbitMQ!")
                is_rabbit_connected = True
                break
        except pika.exceptions.AMQPConnectionError as e:
            print("RabbitMQ connection failed, retrying in 10 seconds:", str(e))
            time.sleep(10)

threading.Thread(target=connect_to_rabbitmq, daemon=True).start()

def publish_to_rabbitmq(message):
    global is_rabbit_connected
    if is_rabbit_connected:
        try:
            channel.basic_publish(
                exchange=exchange_value,
                routing_key=routing_key_value,
                body=json.dumps(message),
            )
            print(f"Message sent to RabbitMQ : \n{message}")
        except Exception as e:
            print("RabbitMQ publish error:", str(e))
            is_rabbit_connected = False
            threading.Thread(target=connect_to_rabbitmq, daemon=True).start()

def check_connections():
    if not is_mqtt_connected:
        threading.Thread(target=reconnect_mqtt, daemon=True).start()

    if not is_rabbit_connected:
        threading.Thread(target=connect_to_rabbitmq, daemon=True).start()

# Simulation functions

def simulate_inflow():
    global current_inflow, water_level
    variation = random.uniform(-1, 1)
    month = time.localtime().tm_mon
    if 3 <= month <= 5:
        seasonal_effect = 0.3
    elif 6 <= month <= 8:
        seasonal_effect = -0.3
    elif 9 <= month <= 11:
        seasonal_effect = 0.3
    else:
        seasonal_effect = 0

    current_inflow += variation + seasonal_effect
    current_inflow = max(MIN_INFLOW, min(current_inflow, MAX_INFLOW))
    if water_level > MAX_HEIGHT * 0.99:
        current_inflow = 0
    elif water_level > MAX_HEIGHT * 0.90:
        reduction_factor = 1 - (water_level - MAX_HEIGHT * 0.90) / (MAX_HEIGHT * 0.09)
        current_inflow *= reduction_factor

    return current_inflow

def simulate_outflow(energy_request, power_generated):
    global current_outflow, water_level
    variation = random.uniform(-5, 5)
    if energy_request <= power_generated:
        current_outflow += variation
    else:
        current_outflow += variation + 10

    if water_level > MAX_HEIGHT * 0.8:
        current_outflow += 3
    elif water_level < MAX_HEIGHT * 0.3:
        current_outflow -= 3

    current_outflow = max(MIN_OUTFLOW, min(current_outflow, MAX_OUTFLOW))
    return current_outflow

def update_water_level_and_volume(inflow, outflow):
    global water_level, current_volume
    volume_change = (inflow - outflow) * INTERVAL_SECONDS
    current_volume = max(0, min(MAX_VOLUME, current_volume + volume_change))
    water_level = (current_volume / MAX_VOLUME) * MAX_HEIGHT

def update_energy_request():
    global current_energy_request
    month = time.localtime().tm_mon
    hour = time.localtime().tm_hour

    seasonal_factor = (month - 6) / 12
    seasonal_variation = seasonal_factor * MAX_GENERATION_CAPACITY * 0.5

    if 6 <= hour <= 20:
        time_factor = 0.7 + random.uniform(-0.01, 0.01)
    else:
        time_factor = 0.3 + random.uniform(-0.01, 0.01)

    daily_variation = (MAX_GENERATION_CAPACITY / 4) * math.sin(2 * math.pi * hour / 24)
 
    new_energy_request = (
        MAX_GENERATION_CAPACITY * time_factor
        + seasonal_variation
        + daily_variation
    )

    current_energy_request = max(0, min(new_energy_request, MAX_GENERATION_CAPACITY))
    return current_energy_request


def calculate_efficiency(outflow):
    global EFFICIENCY
    voltage = 25
    ampere = 10
    value_out_of_range = None
    if random.uniform(0, 1) > 0.999:
        if random.uniform(0, 1) > 0.5:
            voltage = random.uniform(17, 20)
            value_out_of_range = f"⚠️ Tensione fuori scala: {voltage:.2f}V"
        else:
            ampere = random.uniform(3, 5)
            value_out_of_range = f"⚠️ Corrente fuori scala: {ampere:.2f}A"
    if value_out_of_range:
        EFFICIENCY = max(0.2, EFFICIENCY - 0.01)
    feed_power_percentage = (outflow - MIN_OUTFLOW) / max((MAX_OUTFLOW - MIN_OUTFLOW), 1)
    external_power = feed_power_percentage * 15000
    return round(EFFICIENCY, 2), voltage, ampere, round(external_power, 2), value_out_of_range

def calculate_power(outflow, height):
    return EFFICIENCY * DENSITY_WATER * outflow * GRAVITY * height / 1_000_000

MAX_GENERATION_CAPACITY = calculate_power(MAX_OUTFLOW, MAX_HEIGHT)
current_energy_request = MAX_GENERATION_CAPACITY / 2

while True:
    check_connections()
    inflow = simulate_inflow()
    power_generated = calculate_power(current_outflow, water_level)
    outflow = simulate_outflow(current_energy_request, power_generated)
    energy_request = update_energy_request()
    update_water_level_and_volume(inflow, outflow)

    energy_this_interval = power_generated * (INTERVAL_SECONDS / 3600)
    total_energy_mwh += energy_this_interval
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    EFFICIENCY, voltage, ampere, external_power, alert = calculate_efficiency(outflow)

    summary = {
        "SensorId": CLIENT_ID,
        "Timestamp": timestamp,
        "LakeDepth": round(water_level, 2),
        "LakeVol": round(current_volume, 2),
        "Water_IN": round(inflow, 2),
        "Water_OUT": round(outflow, 2),
        "InstantPower": round(power_generated, 2),
        "TotalEnergy": round(total_energy_mwh, 2),
        "EnergyRequest": round(energy_request, 2),
        "LakeDepthMin": 0.1,
        "LakeDepthMax": MAX_HEIGHT,
        "LakeVolMin": 0.1,
        "LakeVolMax": MAX_VOLUME,
        "Water_IN_Min": MIN_INFLOW,
        "Water_IN_Max": MAX_INFLOW,
        "Water_OUT_Min": MIN_OUTFLOW,
        "Water_OUT_Max": MAX_OUTFLOW,
        "InstantPowerMin": 0.1,
        "InstantPowerMax": MAX_GENERATION_CAPACITY,
        "EnergyRequestMin": 0.1,
        "EnergyRequestMax": MAX_GENERATION_CAPACITY
    }
    status = {
        "SensorId": CLIENT_ID,
        "Timestamp": timestamp,
        "EFFICIENCY": EFFICIENCY,
        "Voltage": round(voltage, 2),
        "Ampere": round(ampere, 2),
        "Power": round(external_power, 2)
    }
    if is_mqtt_connected:
        try: 
            client.publish(f"{DAM_FOLDER}/{CLIENT_ID}/LakeDepth", str(round(water_level, 2)))
            client.publish(f"{DAM_FOLDER}/{CLIENT_ID}/LakeVolume", str(round(current_volume, 2)))
            client.publish(f"{DAM_FOLDER}/{CLIENT_ID}/Water_IN", str(round(inflow, 2)))
            client.publish(f"{DAM_FOLDER}/{CLIENT_ID}/Water_OUT", str(round(outflow, 2)))
            client.publish(f"{DAM_FOLDER}/{CLIENT_ID}/InstantPower", str(round(power_generated, 2)))
            client.publish(f"{DAM_FOLDER}/{CLIENT_ID}/TotalEnergy", str(round(total_energy_mwh, 2)))
            client.publish(f"{DAM_FOLDER}/{CLIENT_ID}/EnergyRequest", str(round(energy_request, 2)))
            client.publish(f"{DAM_FOLDER}/{CLIENT_ID}/Status", json.dumps(status))
            client.publish(f"{DAM_FOLDER}/{CLIENT_ID}/Summary", json.dumps(summary))
            print(f"Message sent to MQTT: \n{summary}")

        except Exception as e:
            print("MQTT publish error:", str(e))
            threading.Thread(target=reconnect_mqtt, daemon=True).start()

    if is_rabbit_connected:
        publish_to_rabbitmq(status)

    if not is_mqtt_connected and not is_rabbit_connected:
        print("Both MQTT and RabbitMQ are disconnected, skipping publish.")

    time.sleep(INTERVAL_SECONDS)