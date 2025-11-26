# MQTT Traffic Inspector 🕵️

A lightweight tool built on **paho-mqtt** and **apscheduler** to inspect, analyze, and persist MQTT topic statistics.  
It is designed to identify spamming topics and high-traffic anomalies by tracking the count and payload size of every message on the broker.

---

## Usage

The MQTT Traffic Inspector is built and run as a Docker container, simplifying deployment and ensuring all dependencies are met.

### Build the container

```bash
docker build -t mqtt-traffic-inspector .
```

### Execution Examples
#### Minimal
Run the container using a publicly available broker for quick testing.
Data will be saved to the ./data folder in your current directory.
```bash 
docker run \
  -e MQTT_BROKER_HOST=test.mosquitto.org \
  -v $(pwd)/data:/data \
  mqtt-traffic-inspector
```

#### With custom ttl
Control how often the gathered statistics are flushed from memory and written to the CSV file by setting SAVE_INTERVAL_MINUTES.
```bash
docker run \
  -e MQTT_BROKER_HOST=test.mosquitto.org \
  -e SAVE_INTERVAL_MINUTES=5 \
  -v $(pwd)/data:/data \
  mqtt-traffic-inspector
```

#### Full customization
Use all available environment variables, connecting to a specific broker and naming the output file.
```bash
docker run \
  -e MQTT_BROKER_HOST=broker.hivemq.com \
  -e MQTT_BROKER_PORT=1883 \
  -e MQTT_CLIENT_ID=my-analyzer \
  -e CSV_FILE=/data/stats.csv \
  -e SAVE_INTERVAL_MINUTES=10 \
  -v $(pwd)/data:/data \
  mqtt-traffic-inspector
```
