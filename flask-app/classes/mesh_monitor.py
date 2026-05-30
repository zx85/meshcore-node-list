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
from datetime import datetime, timedelta
import traceback

from meshcore_cli.meshcore_cli import MeshCore

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sender TEXT,
                    status TEXT,
                    message TEXT,
                    raw TEXT,
                    clean TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

    def update_node(self, node_data: Dict, is_home: bool = False):
        with sqlite3.connect(self.db_path) as conn:
            pk = node_data.get("public_key")
            if not pk:
                return False

            cursor = conn.execute("SELECT 1 FROM nodes WHERE public_key = ?", (pk,))
            is_new = cursor.fetchone() is None

            # Use ON CONFLICT to ensure we don't overwrite the 'is_home' flag once set
            query = """
                INSERT INTO nodes 
                    (public_key, name, adv_name, type, adv_lat, adv_lon, out_path_len, last_advert, is_home, last_updated)
                VALUES 
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(public_key) DO UPDATE SET
                    name=excluded.name,
                    adv_name=excluded.adv_name,
                    type=excluded.type,
                    adv_lat=excluded.adv_lat,
                    adv_lon=excluded.adv_lon,
                    out_path_len=excluded.out_path_len,
                    last_advert=excluded.last_advert,
                    is_home=MAX(nodes.is_home, excluded.is_home),
                    last_updated=CURRENT_TIMESTAMP
            """
            conn.execute(
                query,
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

    def store_message(self, msg_data: Dict):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO messages (sender, status, message, raw, clean)
                VALUES (?, ?, ?, ?, ?)
            """,
                (
                    msg_data.get("sender"),
                    msg_data.get("status"),
                    msg_data.get("message"),
                    msg_data.get("raw"),
                    msg_data.get("clean"),
                ),
            )


class MeshDevice:
    """Thread-safe handler for the serial device"""

    def __init__(self, serial_device: str):
        self.serial_device = serial_device
        self.lock = threading.Lock()
        self._app = None

    def _get_app(self):
        """Lazy initialization of the MeshCore library"""
        if self._app is None:
            try:
                logger.info(f"Initializing MeshCore on {self.serial_device}...")
                # Pass the device path as a positional argument
                self._app = MeshCore(self.serial_device)
            except Exception as e:
                logger.error(
                    f"Failed to initialize MeshCore: {e}\n{traceback.format_exc()}"
                )
        return self._app

    def get_info(self):
        with self.lock:
            app = self._get_app()
            if app:
                try:
                    return app.get_info()
                except Exception as e:
                    logger.error(f"Error in get_info: {e}")
            return None

    def get_contacts(self):
        with self.lock:
            app = self._get_app()
            if app:
                try:
                    return app.get_contacts()
                except Exception as e:
                    logger.error(f"Error in get_contacts: {e}")
            return []

    def get_contact_info(self, name: str):
        with self.lock:
            app = self._get_app()
            if app:
                try:
                    return app.get_contact_info(name)
                except Exception as e:
                    logger.error(f"Error in get_contact_info for '{name}': {e}")
            return None

    def sync_msgs(self):
        with self.lock:
            app = self._get_app()
            if app:
                try:
                    return app.sync_messages()
                except Exception as e:
                    logger.error(f"Error in sync_msgs: {e}")
            return []

    def sync_clock(self):
        with self.lock:
            app = self._get_app()
            if app:
                try:
                    return app.sync_clock()
                except Exception as e:
                    logger.error(f"Error in sync_clock: {e}")
            return False

    def reboot(self):
        with self.lock:
            app = self._get_app()
            if app:
                try:
                    return app.reboot()
                except Exception as e:
                    logger.error(f"Error in reboot: {e}")
            return False

    def run_meshcli(self, args: list):
        """Legacy support for direct shell commands if needed, though mostly deprecated now"""
        if not os.path.exists(self.serial_device):
            return None
        with self.lock:
            cmd = ["uv", "run", "meshcli", "-s", self.serial_device] + args
            logger.debug(f"Running command: {' '.join(cmd)}")
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                if result.returncode != 0:
                    logger.debug(
                        f"Command failed (RC {result.returncode}). STDERR: {result.stderr.strip()}"
                    )
                if result.stdout:
                    logger.debug(
                        f"Raw output (first 100 chars): {result.stdout.strip()[:100]}"
                    )
                return result
            except Exception as e:
                logger.error(f"MeshCLI execution error: {e}")
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
            if not self.config.get("host"):
                logger.info("No MQTT host provided; MQTT publishing is disabled.")
                return

            if self.config.get("username"):
                self.client.username_pw_set(
                    self.config["username"], self.config.get("password")
                )
            self.client.on_connect = self._on_connect
            self.client.connect(self.config["host"], self.config.get("port", 1883))
            self.client.loop_start()
            logger.info(f"MQTT handler initialized for {self.config['host']}")
        except Exception as e:
            logger.error(f"MQTT Init fail: {e}")

    def _on_connect(self, client, userdata, flags, rc):
        self.mqtt_connected = rc == 0

    def send_status(self, status: str, retain: bool = False):
        if not self.mqtt_connected:
            return
        topic = self.config.get("status_topic", "mesh/status")
        payload = json.dumps(
            {"status": status, "timestamp": datetime.now().isoformat()}
        )
        self.client.publish(topic, payload, qos=1, retain=retain)

    def publish_node(self, node):
        if not self.mqtt_connected:
            return
        topic = self.config.get("node_topic", "mesh/nodes/new")
        self.client.publish(topic, json.dumps(node))

    def publish_message(self, msg):
        if not self.mqtt_connected:
            return
        topic = self.config.get("message_topic", "mesh/messages")
        self.client.publish(topic, json.dumps(msg))


class MeshMonitor:
    def __init__(self, mqtt_config: Dict[str, Any], db_path: str, _unused: str):
        # Clean up path to handle potential literal quotes from docker env
        db_path_clean = db_path.strip('"')
        self.db = DatabaseManager(db_path_clean)

        serial_dev = os.environ.get("MESH_SERIAL_DEVICE", "/dev/ttyACM0").strip('"')
        self.device = MeshDevice(serial_dev)
        self.mqtt = MqttHandler(mqtt_config)

        # Robust boolean check stripping quotes
        serial_env = os.environ.get("MESH_SERIAL_ENABLED", "true").strip('"').lower()
        self.serial_enabled = serial_env == "true"
        self._stop_event = threading.Event()

    def monitor_loop(self, _unused_cb, interval: int = 30):
        logger.info(f"Monitor loop called. Serial enabled: {self.serial_enabled}")
        if not self.serial_enabled:
            logger.warning("Monitor loop exiting: Serial monitoring is disabled.")
            return

        logger.info("Starting background worker threads...")
        threading.Thread(target=self._msg_worker, daemon=True).start()
        threading.Thread(
            target=self._discovery_worker, args=(interval,), daemon=True
        ).start()
        threading.Thread(target=self._reboot_worker, daemon=True).start()

        while not self._stop_event.is_set():
            time.sleep(1)

    def _reboot_worker(self):
        """Replicates the specific scheduled reboot times from crontab (04:32, 10:32, 16:32, 22:32)"""
        try:
            logger.info("Reboot worker thread started.")
            # Brief initial delay to let the application settle
            time.sleep(60)

            while not self._stop_event.is_set():
                now = datetime.now()
                scheduled_hours = [4, 10, 16, 22]
                target_time = None

                # Find the next scheduled reboot time for today
                for hour in scheduled_hours:
                    candidate = now.replace(
                        hour=hour, minute=32, second=0, microsecond=0
                    )
                    if candidate > now:
                        target_time = candidate
                        break

                # If no more reboots today, target 04:32 tomorrow
                if not target_time:
                    target_time = (now + timedelta(days=1)).replace(
                        hour=4, minute=32, second=0, microsecond=0
                    )

                wait_seconds = int((target_time - now).total_seconds())
                logger.info(
                    f"Next reboot scheduled for {target_time.strftime('%Y-%m-%d %H:%M:%S')}. Sleeping for {wait_seconds}s."
                )

                # Interruptible sleep until target time
                stop_sleeping = False
                for _ in range(wait_seconds):
                    if self._stop_event.is_set():
                        stop_sleeping = True
                        break
                    time.sleep(1)

                if stop_sleeping:
                    break

                logger.info("Starting scheduled node reboot...")

                # Use the library reboot method
                if self.device.reboot():
                    logger.info("Reboot command sent. Waiting 30s for recovery...")
                    # 2. Wait for node to come back up
                    time.sleep(30)
                    # 3. Sync the clock
                    logger.info("Syncing node clock after reboot...")
                    self.device.sync_clock()
                    logger.info("Node clock synced. Reboot cycle complete.")
        except Exception as e:
            logger.error(f"Reboot worker crashed: {e}")

    def _msg_worker(self):
        logger.info("Message polling worker thread started.")
        while not self._stop_event.is_set():
            msgs = self.device.sync_msgs()
            for msg_text in msgs:
                parsed = parse_mesh_message_advanced(msg_text)
                if parsed["message"]:
                    self.db.store_message(parsed)
                    self.mqtt.publish_message(parsed)
                    logger.debug(f"Published message: {parsed['clean']}")
            time.sleep(5)

    def _discovery_worker(self, interval):
        while not self._stop_event.is_set():
            start_time = time.time()
            nodes_processed = 0
            new_nodes_announced = 0

            logger.info("Starting node discovery cycle...")
            self.mqtt.send_status("updating_nodes")

            logger.debug("Fetching local node info...")
            node_data = self.device.get_info()
            if node_data:
                try:
                    if self.db.update_node(node_data, is_home=True):
                        logger.info(
                            f"Updated home node: {node_data.get('name') or node_data.get('adv_name')}"
                        )
                    nodes_processed += 1
                except Exception as e:
                    logger.error(f"Error updating home node in DB: {e}")
            else:
                logger.warning("Failed to retrieve local node info via library.")

            logger.info("Scanning contacts list...")
            contacts = self.device.get_contacts()
            if contacts:
                for name in contacts:
                    if not name or name.startswith("Error:"):
                        continue

                    logger.info(f"Checking node: '{name}'")
                    # Use library for contact info
                    node_info = self.device.get_contact_info(name)

                    # Replicate fallback logic if first call fails
                    if not node_info:
                        fallback_name = f"{name} "
                        logger.debug(
                            f"Direct info fetch failed for '{name}', retrying with '{fallback_name}'"
                        )
                        node_info = self.device.get_contact_info(fallback_name)

                    if node_info:
                        try:
                            if self.db.update_node(node_info):
                                self.mqtt.publish_node(node_info)
                                new_nodes_announced += 1
                                logger.info(
                                    f"New node discovered and announced: {node_info.get('adv_name', 'Unknown')} ({node_info.get('public_key', '')[:8]}...)"
                                )
                            nodes_processed += 1
                        except Exception as e:
                            logger.error(f"DB Error processing node '{name}': {e}")
                    else:
                        logger.warning(f"Failed to get valid data for node: '{name}'")
            else:
                logger.debug("No contacts found on device.")

            self.mqtt.send_status("updated")
            end_time = time.time()
            duration = end_time - start_time
            logger.info(
                f"Node discovery cycle completed. Processed {nodes_processed} nodes, announced {new_nodes_announced} new nodes in {duration:.2f} seconds."
            )

            # Ensure we sleep for at least the interval, accounting for execution time
            sleep_duration = max(120, interval) - duration
            if sleep_duration > 0:
                logger.debug(
                    f"Discovery worker sleeping for {sleep_duration:.2f} seconds."
                )
                time.sleep(sleep_duration)
            else:
                logger.warning(
                    f"Node discovery took longer than the interval ({duration:.2f}s vs {interval}s). Skipping sleep."
                )

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
