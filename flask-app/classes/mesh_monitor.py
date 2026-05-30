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
import asyncio
from meshcore.meshcore import MeshCore
from meshcore.connection_manager import EventType

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
        self._meshcore = None
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self._loop_thread.start()

    def _run_event_loop(self):
        """Runs the dedicated asyncio event loop for meshcore."""
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _run_async(self, coro):
        """Helper to run async coroutines from sync threads."""
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    async def _ensure_connected(self):
        """Initializes the MeshCore serial connection if it hasn't been started."""
        if self._meshcore is None:
            try:
                logger.info(f"Connecting to MeshCore device on {self.serial_device}...")
                self._meshcore = await MeshCore.create_serial(
                    self.serial_device, 115200, debug=False
                )
            except Exception as e:
                logger.error(f"Failed to create serial connection: {e}")
                raise

    async def _get_info_coro(self):
        await self._ensure_connected()
        result = await self._meshcore.commands.send_device_query()
        if result.type != EventType.ERROR:
            return result.payload
        logger.error(f"get_info failed: {result.payload}")
        return None

    def get_info(self):
        with self.lock:
            return self._run_async(self._get_info_coro())

    async def _get_contacts_coro(self):
        await self._ensure_connected()
        result = await self._meshcore.commands.get_contacts()
        if result.type != EventType.ERROR:
            return result.payload  # Usually a list of contact names/identifiers
        logger.error(f"get_contacts failed: {result.payload}")
        return []

    def get_contacts(self):
        with self.lock:
            return self._run_async(self._get_contacts_coro())

    def subscribe(self, event_type, callback):
        with self.lock:
            return self._run_async(self._subscribe_coro(event_type, callback))

    async def _sync_clock_coro(self):
        await self._ensure_connected()
        result = await self._meshcore.commands.sync_clock()
        return result.type != EventType.ERROR

    def sync_clock(self):
        with self.lock:
            return self._run_async(self._sync_clock_coro())

    async def _reboot_coro(self):
        await self._ensure_connected()
        result = await self._meshcore.commands.reboot()
        return result.type != EventType.ERROR

    def reboot(self):
        with self.lock:
            return self._run_async(self._reboot_coro())

    async def _subscribe_coro(self, event_type, callback):
        await self._ensure_connected()
        return self._meshcore.subscribe(event_type, callback)


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

        if self.serial_enabled:
            self._setup_event_listeners()

    def _setup_event_listeners(self):
        """Register real-time listeners for mesh events"""
        logger.info("Setting up real-time mesh event listeners...")

        async def on_message(event):
            try:
                data = event.payload
                logger.info(f"Event: New message from {data.get('pubkey_prefix')}")

                # Parse and format for DB storage
                msg_text = data.get("text", "")
                parsed = parse_mesh_message_advanced(msg_text)

                msg_record = {
                    "sender": data.get("pubkey_prefix"),
                    "status": parsed.get("status"),
                    "message": parsed.get("message"),
                    "raw": json.dumps(data),
                    "clean": parsed.get("clean"),
                }

                # Store in SQLite and publish via MQTT
                self.db.store_message(msg_record)
                self.mqtt.publish_message(msg_record)
            except Exception as e:
                logger.error(f"Error in on_message listener: {e}")

        async def on_advert(event):
            try:
                node_info = event.payload
                logger.debug(
                    f"Event: Advert from {node_info.get('adv_name', 'Unknown')}"
                )

                # Update node record in database
                if self.db.update_node(node_info):
                    # If this is a new discovery, publish to MQTT
                    self.mqtt.publish_node(node_info)
            except Exception as e:
                logger.error(f"Error in on_advert listener: {e}")

        # Hook into the MeshCore event system
        self.device.subscribe(EventType.CONTACT_MSG_RECV, on_message)
        self.device.subscribe(EventType.ADVERTISEMENT, on_advert)

    def monitor_loop(self, _unused_cb, interval: int = 30):
        logger.info(f"Monitor loop called. Serial enabled: {self.serial_enabled}")
        if not self.serial_enabled:
            logger.warning("Monitor loop exiting: Serial monitoring is disabled.")
            return

        logger.info("Starting background worker threads...")
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
                for idx, node_info in contacts.items():
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
