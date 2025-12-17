import os
import time
import random
import json
import paho.mqtt.client as mqtt

broker = os.getenv("BROKER_HOST", "mosquitto")
port = int(os.getenv("BROKER_PORT", "1883"))
topic = os.getenv("MQTT_TOPIC", "weather")

station_id = os.getenv("STATION_ID", "WS-XX")
interval = int(os.getenv("INTERVAL", "5"))

def main():
    client = mqtt.Client()
    client.connect(broker, port, 60)

    silent_until = 0  

    while True:
        now = time.time()

        if silent_until > now:
            time.sleep(1)
            continue

        if random.random() < 0.005:
            print(f"[{station_id}] Simulierter Ausfall (60s, keine Publishes)")
            silent_until = now + 60
            time.sleep(1)
            continue

        temperature = -999 if random.random() < 0.01 else round(random.uniform(15, 30), 1)
        data = {
            "stationId": station_id,
            "temperature": temperature,
            "humidity": round(random.uniform(30, 60), 1),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        client.publish(topic, json.dumps(data))
        print(f"[{station_id}] Published: {data}")
        time.sleep(interval)

if __name__ == "__main__":
    if os.getenv("DISABLE_MQTT") == "1":
        print("MQTT disabled for tests (DISABLE_MQTT=1).")
    else:
        main(

#Test