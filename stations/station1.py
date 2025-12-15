import os
import time
import random
import json
import paho.mqtt.client as mqtt

broker = "mosquitto"
port = 1883
topic = "weather"

station_id = os.getenv("STATION_ID", "WS-XX")
interval = int(os.getenv("INTERVAL", "5"))
random
client = mqtt.Client()
client.connect(broker, port, 60)


# ... oben gleich ...
silent_until = 0  # epoch seconds

while True:
    now = time.time()

    # Wenn gerade Ausfallphase: nichts publishen
    if silent_until > now:
        time.sleep(1)
        continue

    # Ausfall starten (kleine Chance)
    if random.random() < 0.005:
        print(f"[{station_id}] Simulierter Ausfall (60s, keine Publishes)")
        silent_until = now + 60
        time.sleep(1)
        continue

    # normale Messwerte
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

