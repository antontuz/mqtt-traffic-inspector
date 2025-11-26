import paho.mqtt.client as mqtt
import csv
import os
import uuid
import threading
import sys
from typing import Dict
from dataclasses import dataclass
from apscheduler.schedulers.blocking import BlockingScheduler

@dataclass
class TopicStats:
    count: int = 0
    max_size: int = 0
    min_size: float = float('inf')
    total_size: int = 0

    @property
    def average_size(self) -> int:
        return int(self.total_size / self.count) if self.count > 0 else 0

    def update(self, payload_len: int):
        self.count += 1
        self.total_size += payload_len
        self.max_size = max(self.max_size, payload_len)
        self.min_size = min(self.min_size, payload_len)

class MqttAnalyzer:
    def __init__(self, host: str, port: int, client_id: str, csv_file: str):
        # Configuration
        self.BROKER_HOST = host
        self.BROKER_PORT = port
        self.CLIENT_ID = client_id
        self.CSV_FILE = csv_file
        
        self.stats_store: Dict[str, TopicStats] = {}
        self.store_lock = threading.Lock()
        
        self.mqtt_client = mqtt.Client(client_id=self.CLIENT_ID, clean_session=True)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc):
        print(f"Connected with result code: {rc}")
        client.subscribe("#", qos=0)

    def on_message(self, client, userdata, msg):
        payload_len = len(msg.payload)
        topic = msg.topic

        with self.store_lock:
            if topic not in self.stats_store:
                self.stats_store[topic] = TopicStats()
            self.stats_store[topic].update(payload_len)

    def load_csv(self):
        if not os.path.exists(self.CSV_FILE):
            return

        print("Loading existing data...")
        with open(self.CSV_FILE, newline='', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=';')
            with self.store_lock:
                for row in reader:
                    if len(row) < 5: continue
                    try:
                        topic, count, max_s, min_s, total_s = row
                        self.stats_store[topic] = TopicStats(
                            count=int(count),
                            max_size=int(max_s),
                            min_size=float(min_s),
                            total_size=int(total_s)
                        )
                    except ValueError:
                        print(f"Skipping malformed row: {row}")

    def save_csv(self):
        print("Saving stats to disk...")
        with self.store_lock:
            snapshot = list(self.stats_store.items())
        
        with open(self.CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            for topic, stats in snapshot:
                writer.writerow([
                    topic,
                    stats.count,
                    stats.max_size,
                    stats.min_size if stats.min_size != float('inf') else 0,
                    stats.total_size
                ])

    def start(self):
        self.load_csv()

        mqtt_thread = threading.Thread(target=self._run_mqtt_loop, daemon=True)
        mqtt_thread.start()

        print(f"Analyzer started. Listening on {self.BROKER_HOST}...")
        
        scheduler = BlockingScheduler()
        save_interval = int(os.getenv("SAVE_INTERVAL_MINUTES", "1"))
        scheduler.add_job(self.save_csv, 'interval', minutes=save_interval)
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            print("\nShutting down analyzer...")
            sys.exit(0)
            
    def _run_mqtt_loop(self):
        try:
            self.mqtt_client.connect(self.BROKER_HOST, self.BROKER_PORT, 60)
            self.mqtt_client.loop_forever()
        except Exception as e:
            print(f"MQTT Error: {e}")
            sys.exit(1)


if __name__ == "__main__":
    mqtt_host = os.getenv("MQTT_BROKER_HOST")
    if not mqtt_host:
        print("ERROR: MQTT_BROKER_HOST environment variable is required")
        sys.exit(1)
    
    analyzer = MqttAnalyzer(
        host=mqtt_host,
        port=int(os.getenv("MQTT_BROKER_PORT", "1883")),
        client_id=os.getenv("MQTT_CLIENT_ID", f"mqtt-analyzer-{uuid.uuid4().hex[:8]}"),
        csv_file=os.getenv("CSV_FILE", "/data/analyzed.csv")
    )
    analyzer.start()