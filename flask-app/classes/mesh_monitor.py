import sqlite3
import json
import time
import subprocess
import paho.mqtt.client as mqtt
import logging
import os
import re
import threading
from pathlib import Path
from typing import Dict, Set, Any
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_mesh_message_advanced(raw_message):
    """
    Parse mesh messages with ANSI escape codes into structured parts.
    """
    # Remove ANSI escape codes
    ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    clean_message = ansi_escape.sub("", raw_message)

    # Try different patterns
    patterns = [
        # Pattern 1: Sender (Status): Message
        r"^(.+?)\s*(\([^)]+\)):\s*(.+)$",
        # Pattern 2: Sender: Message (no status)
        r"^(.+?):\s*(.+)$",
        # Pattern 3: Just message content
        r"^(.+)$",
    ]

    for pattern in patterns:
        match = re.match(pattern, clean_message)
        if match:
            if pattern == patterns[0]:  # Sender (Status): Message
                return {
                    "sender": match.group(1).strip(),
                    "status": match.group(2).strip(),
                    "message": match.group(3).strip(),
                    "raw": raw_message,
                    "clean": clean_message,
                }
            elif pattern == patterns[1]:  # Sender: Message
                return {
                    "sender": match.group(1).strip(),
                    "status": None,
                    "message": match.group(2).strip(),
                    "raw": raw_message,
                    "clean": clean_message,
                }
            else:  # Just message
                return {
                    "sender": None,
                    "status": None,
                    "message": match.group(1).strip(),
                    "raw": raw_message,
                    "clean": clean_message,
                }

    # Fallback
    return {
        "sender": None,
        "status": None,
        "message": clean_message,
        "raw": raw_message,
        "clean": clean_message,
    }


class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    public_key TEXT PRIMARY KEY,
                    name TEXT,
                    adv_name TEXT,
                    type INTEGER,
                    adv_lat REAL,
                    adv_lon REAL,
                    out_path_len INTEGER,
                    last_advert INTEGER,
                    is_home INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

    def update_node(self, node_data: Dict, is_home: bool = False):
        with sqlite3.connect(self.db_path) as conn:
            pk = node_data.get("public_key")
            if not pk:
                return False

            cursor = conn.execute("SELECT 1 FROM nodes WHERE public_key = ?", (pk,))
            is_new = cursor.fetchone() is None

            conn.execute(
                """
                INSERT OR REPLACE INTO nodes 
                (public_key, name, adv_name, type, adv_lat, adv_lon, out_path_len, last_advert, is_home, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
                (
                    pk,
                    node_data.get("name"),
                    node_data.get("adv_name"),
                    node_data.get("type", 0),
                    node_data.get("adv_lat"),
                    node_data.get("adv_lon"),
                    node_data.get("out_path_len"),
                    node_data.get("last_advert"),
                    1 if is_home else 0,
                ),
            )
            return is_new

    def get_all_nodes(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            # Home node first, then sort by last_advert (the 'last heard' fix)
            rows = conn.execute(
                "SELECT * FROM nodes ORDER BY is_home DESC, last_advert DESC"
            ).fetchall()
            return [dict(row) for row in rows]


class MeshDevice:
    """Thread-safe handler for the serial device"""

    def __init__(self, serial_device: str):
        self.serial_device = serial_device
        self.lock = threading.Lock()

    def run_meshcli(self, args: list):
        if not os.path.exists(self.serial_device):
            return None
        with self.lock:
            cmd = ["uv", "run", "meshcli", "-s", self.serial_device] + args
            try:
                return subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            except Exception as e:
                logger.error(f"MeshCLI error: {e}")
                return None


class MqttHandler:
    def __init__(self, config):
        self.config = config
        self.client = mqtt.Client()
        self.mqtt_connected = False
        self.published_messages = 0
        self.failed_messages = 0
        self.last_publish_status = None
        self.setup()

    def setup(self):
        try:
            if self.config.get("username"):
                self.client.username_pw_set(
                    self.config["username"], self.config.get("password")
                )
            self.client.on_connect = self._on_connect
            self.client.connect(self.config["host"], self.config.get("port", 1883))
            self.client.loop_start()
        except Exception as e:
            logger.error(f"MQTT Init fail: {e}")

    def _on_connect(self, client, userdata, flags, rc):
        self.mqtt_connected = rc == 0

    def send_status(self, status: str, retain: bool = False):
        topic = self.config.get("status_topic", "mesh/status")
        payload = json.dumps(
            {"status": status, "timestamp": datetime.now().isoformat()}
        )
        self.client.publish(topic, payload, qos=1, retain=retain)

    def publish_node(self, node):
        topic = self.config.get("node_topic", "mesh/nodes/new")
        self.client.publish(topic, json.dumps(node))

    def publish_message(self, msg):
        topic = self.config.get("message_topic", "mesh/messages")
        self.client.publish(topic, json.dumps(msg))


class MeshMonitor:
    def __init__(self, mqtt_config: Dict[str, Any], db_path: str, _unused: str):
        self.db = DatabaseManager(db_path)
        self.device = MeshDevice(os.environ.get("MESH_SERIAL_DEVICE", "/dev/ttyACM0"))
        self.mqtt = MqttHandler(mqtt_config)
        self.serial_enabled = (
            os.environ.get("MESH_SERIAL_ENABLED", "true").lower() == "true"
        )
        self._stop_event = threading.Event()

    def monitor_loop(self, _unused_cb, interval: int = 30):
        if not self.serial_enabled:
            return

        # Message polling thread
        threading.Thread(target=self._msg_worker, daemon=True).start()
        # Discovery thread
        threading.Thread(
            target=self._discovery_worker, args=(interval,), daemon=True
        ).start()

        while not self._stop_event.is_set():
            time.sleep(1)

    def _msg_worker(self):
        while not self._stop_event.is_set():
            res = self.device.run_meshcli(["sync_msgs"])
            if res and res.stdout.strip():
                for line in res.stdout.splitlines():
                    parsed = parse_mesh_message_advanced(line)
                    if parsed["message"]:
                        self.mqtt.publish_message(parsed)
            time.sleep(5)

    def _discovery_worker(self, interval):
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        while not self._stop_event.is_set():
            self.mqtt.send_status("updating_nodes")

            # Replicate get_nodes.sh
            res = self.device.run_meshcli(["infos"])
            if res and res.returncode == 0:
                try:
                    self.db.update_node(json.loads(res.stdout), is_home=True)
                except:
                    pass

            res = self.device.run_meshcli(["list"])
            if res:
                for line in res.stdout.splitlines():
                    name = ansi_escape.sub("", line).strip()
                    if not name or "contacts" in name:
                        continue

                    info = self.device.run_meshcli(["contact_info", name])
                    if info and info.returncode == 0:
                        try:
                            data = json.loads(info.stdout)
                            if self.db.update_node(data):
                                self.mqtt.publish_node(data)
                        except:
                            pass

            self.mqtt.send_status("updated")
            time.sleep(max(120, interval))

    def get_connection_stats(self):
        return {
            "connected": self.mqtt.mqtt_connected,
            "published_messages": self.mqtt.published_messages,
            "failed_messages": self.mqtt.failed_messages,
            "last_publish_status": self.mqtt.last_publish_status,
        }

    def cleanup(self):
        self._stop_event.set()
        self.mqtt.client.loop_stop()
        self.mqtt.client.disconnect()
