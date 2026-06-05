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
                    active INTEGER DEFAULT 1,
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
            # Ensure 'active' column exists for existing databases
            try:
                conn.execute("ALTER TABLE nodes ADD COLUMN active INTEGER DEFAULT 1")
            except sqlite3.OperationalError:
                pass  # Column already exists

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
                    (public_key, name, adv_name, type, adv_lat, adv_lon, out_path_len, last_advert, is_home, active, last_updated)
                VALUES 
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
                ON CONFLICT(public_key) DO UPDATE SET
                    name=excluded.name,
                    adv_name=excluded.adv_name,
                    type=excluded.type,
                    adv_lat=excluded.adv_lat,
                    adv_lon=excluded.adv_lon,
                    out_path_len=excluded.out_path_len,
                    last_advert=excluded.last_advert,
                    is_home=MAX(nodes.is_home, excluded.is_home),
                    active=1,
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

    def get_node_by_pubkey_prefix(self, pubkey_prefix: str):
        """
        Retrieves a node's data by matching its public_key with a given prefix.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM nodes WHERE public_key LIKE ? || '%' LIMIT 1",
                (pubkey_prefix,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def mark_node_inactive(self, public_key: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE nodes SET active = 0, last_updated = CURRENT_TIMESTAMP WHERE public_key = ?",
                (public_key,),
            )

    def get_all_nodes(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            # Home node first, then sort by last_advert (the 'last heard' fix)
            rows = conn.execute(
                "SELECT * FROM nodes WHERE active = 1 ORDER BY is_home DESC, last_advert DESC"
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
        self._subscriptions = (
            []
        )  # Track event subscriptions for persistence across reconnections
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
                    # Setting debug=True here will dump raw serial traffic to the logs
                    self.serial_device,
                    115200,
                    debug=False,
                )
                # Enable auto-fetching of messages as requested
                logger.info("Enabling auto message fetching...")
                await self._meshcore.start_auto_message_fetching()

                # Re-apply all registered event subscriptions to the new connection
                for event_type, callback in self._subscriptions:
                    logger.info(f"Re-applying persistent subscription for {event_type}")
                    self._meshcore.subscribe(event_type, callback)
            except Exception as e:
                logger.error(f"Failed to create serial connection: {e}")
                raise

    async def _get_info_coro(self):
        await self._ensure_connected()
        try:
            result = await self._meshcore.commands.send_device_query()
            if result.type != EventType.ERROR:
                return result.payload

            # If we get a timeout, the serial connection might be stale
            if (
                isinstance(result.payload, dict)
                and result.payload.get("reason") == "no_event_received"
            ):
                logger.error("Serial timeout (no_event_received) in get_info.")

            logger.error(f"get_info failed: {result.payload}")
        except Exception as e:
            logger.error(f"Exception in _get_info_coro: {e}")
        return None

    def get_info(self):
        with self.lock:
            return self._run_async(self._get_info_coro())

    async def _get_contacts_coro(self):
        await self._ensure_connected()
        try:
            result = await self._meshcore.commands.get_contacts()
            if result.type != EventType.ERROR:
                return result.payload

            if (
                isinstance(result.payload, dict)
                and result.payload.get("reason") == "no_event_received"
            ):
                logger.error("Serial timeout (no_event_received) in get_contacts.")

            logger.error(f"get_contacts failed: {result.payload}")
        except Exception as e:
            logger.error(f"Exception in _get_contacts_coro: {e}")
        return []

    def get_contacts(self):
        with self.lock:
            return self._run_async(self._get_contacts_coro())

    async def _remove_contact_coro(self, public_key):
        await self._ensure_connected()
        try:
            # Attempt to delete the contact from hardware memory
            result = await self._meshcore.commands.remove_contact(public_key)
            return result.type != EventType.ERROR
        except Exception as e:
            logger.error(f"Failed to delete contact {public_key} from device: {e}")
            return False

    def remove_contact(self, public_key):
        """Deletes a contact from the mesh device hardware."""
        with self.lock:
            return self._run_async(self._remove_contact_coro(public_key))

    def subscribe(self, event_type, callback):
        with self.lock:
            return self._run_async(self._subscribe_coro(event_type, callback))

    def disconnect(self):
        """Public sync method to disconnect the device."""
        with self.lock:
            return self._run_async(self._disconnect_coro())

    async def _disconnect_coro(self):
        if self._meshcore:
            logger.info("Disconnecting MeshCore serial device...")
            try:
                await self._meshcore.disconnect()
            except Exception:
                pass
            self._meshcore = None

    async def _sync_clock_coro(self):
        await self._ensure_connected()
        try:
            result = await self._meshcore.commands.set_time(int(time.time()))
            if (
                result.type == EventType.ERROR
                and isinstance(result.payload, dict)
                and result.payload.get("reason") == "no_event_received"
            ):
                logger.warning("Clock sync timed out (no_event_received).")
        except Exception as e:
            logger.error(f"Exception in _sync_clock_coro: {e}")
            return False
        return result.type != EventType.ERROR

    def sync_clock(self):
        with self.lock:
            return self._run_async(self._sync_clock_coro())

    async def _reboot_coro(self):
        await self._ensure_connected()
        try:
            result = await self._meshcore.commands.reboot()
            # Always disconnect after a reboot command regardless of result
            # because the hardware is about to vanish from the bus.
            logger.info("Reboot command sent; proactively disconnecting port.")
            await self._disconnect_coro()
            return result.type != EventType.ERROR
        except Exception as e:
            logger.debug(
                f"Exception during reboot command (expected if device reset fast): {e}"
            )
            await self._disconnect_coro()
            return False

    def reboot(self):
        with self.lock:
            return self._run_async(self._reboot_coro())

    async def _subscribe_coro(self, event_type, callback):
        if (event_type, callback) in self._subscriptions:
            return True

        # Store the subscription so it survives future connection resets
        self._subscriptions.append((event_type, callback))

        was_connected = self._meshcore is not None
        await self._ensure_connected()

        # If we were already connected, _ensure_connected did nothing, so apply now.
        # Otherwise, _ensure_connected already re-applied the list.
        if was_connected:
            return self._meshcore.subscribe(event_type, callback)
        return True


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
            self.client.on_disconnect = self._on_disconnect
            self.client.connect(self.config["host"], self.config.get("port", 1883))
            self.client.loop_start()
            logger.info(f"MQTT handler initialized for {self.config['host']}")
        except Exception as e:
            logger.error(f"MQTT Init fail: {e}")

    def _on_connect(self, client, userdata, flags, rc):
        self.mqtt_connected = rc == 0
        if self.mqtt_connected:
            logger.info("MQTT Connected successfully.")
        else:
            logger.error(f"MQTT Connection failed with code {rc}")

    def _on_disconnect(self, client, userdata, rc):
        self.mqtt_connected = False
        logger.warning(f"MQTT Disconnected (rc: {rc})")

    def _publish(self, topic, payload, retain=False):
        """Internal helper to handle publishing and stats"""
        if not self.mqtt_connected:
            self.failed_messages += 1
            logger.warning(f"MQTT not connected. Dropping message for {topic}")
            return False

        result = self.client.publish(topic, payload, qos=1, retain=retain)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            self.published_messages += 1
            self.last_publish_status = (
                f"Sent to {topic} at {datetime.now().strftime('%H:%M:%S')}"
            )
            logger.info(f"MQTT Published: {topic}")
            return True
        else:
            self.failed_messages += 1
            self.last_publish_status = f"Error {result.rc} on {topic}"
            logger.error(f"MQTT Publish failed for {topic} (rc: {result.rc})")
            return False

    def send_status(self, status: str, retain: bool = False):
        topic = self.config.get("status_topic", "mesh/status")
        payload = json.dumps(
            {"status": status, "timestamp": datetime.now().isoformat()}
        )
        self._publish(topic, payload, retain=retain)

    def publish_node(self, node):
        topic = self.config.get("node_topic", "mesh/nodes/new")
        self._publish(topic, json.dumps(node))

    def publish_message(self, msg):
        topic = self.config.get("message_topic", "mesh/messages")
        self._publish(topic, json.dumps(msg))

    def publish_advert(self, advert):
        topic = self.config.get("advert_topic", "mesh/advert")
        self._publish(topic, json.dumps(advert))

    def publish_channel_message(self, msg):
        topic = self.config.get("channels_topic", "meshcore/channels")
        self._publish(topic, json.dumps(msg))


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
        self._rebooting = False
        self._last_clock_sync = 0

        if self.serial_enabled:
            self._setup_event_listeners()

    def _setup_event_listeners(self):
        """Register real-time listeners for mesh events"""
        logger.info("Setting up real-time mesh event listeners...")

        async def on_message(event):
            try:
                data = event.payload
                pubkey_prefix = data.get("pubkey_prefix")
                logger.info(f"Event: New message from {pubkey_prefix}")

                # Resolve sender name from database
                sender_display = pubkey_prefix
                if pubkey_prefix:
                    logger.info(f"Looking for {pubkey_prefix} in the database")
                    node = self.db.get_node_by_pubkey_prefix(pubkey_prefix)
                    if node and node.get("adv_name"):
                        sender_display = node["adv_name"]
                        logger.info(
                            f"Resolved sender {pubkey_prefix} to '{sender_display}'"
                        )

                # Parse and format for DB storage
                msg_text = data.get("text", "")
                parsed = parse_mesh_message_advanced(msg_text)

                msg_record = {
                    "sender": sender_display,
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

        async def on_channel_message(event):
            try:
                data = event.payload
                channel_idx = data.get("channel_idx")
                pubkey_prefix = data.get("pubkey_prefix")
                text = data.get("text")

                logger.info(
                    f"Event: Channel message on Channel {channel_idx} from {pubkey_prefix}"
                )

                # Resolve sender name from database
                sender_display = pubkey_prefix
                if pubkey_prefix:
                    node = self.db.get_node_by_pubkey_prefix(pubkey_prefix)
                    if node and node.get("adv_name"):
                        sender_display = node["adv_name"]

                channel_msg = {
                    "channel": channel_idx,
                    "sender": sender_display,
                    "sender_pk": pubkey_prefix,
                    "text": text,
                    "timestamp": datetime.now().isoformat(),
                }

                self.mqtt.publish_channel_message(channel_msg)
            except Exception as e:
                logger.error(f"Error in on_channel_message listener: {e}")

        async def on_advert(event):
            try:
                node_info = event.payload
                logger.info(f"Event: Advert - {json.dumps(node_info, indent=2)}")
                if node_info.get("public_key"):
                    node = self.db.get_node_by_pubkey_prefix(
                        node_info.get("public_key")[:12]
                    )
                    if node and node.get("adv_name"):
                        sender = node.get("adv_name")
                    else:
                        sender = node_info.get("public_key")[:12]
                else:
                    sender = "unknown"
                self.mqtt.publish_advert({"sender": sender})
            except Exception as e:
                logger.error(f"Error in on_advert listener: {e}")

        # Hook into the MeshCore event system
        self.device.subscribe(EventType.CONTACT_MSG_RECV, on_message)
        self.device.subscribe(EventType.ADVERTISEMENT, on_advert)
        self.device.subscribe(EventType.CHANNEL_MSG_RECV, on_channel_message)

        # Optional: Listen for the 'waiting' signal to confirm the hardware-to-software flow
        async def on_waiting(event):
            logger.debug("Hardware notification: Messages waiting to be fetched.")

        self.device.subscribe(EventType.MESSAGES_WAITING, on_waiting)

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
                scheduled_hours = [8, 20]
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

                # 1. Send reboot command (internal logic now proactive disconnects)
                if self.device.reboot():
                    self._rebooting = True
                    logger.info(
                        "Reboot command acknowledged. Locking serial port for 60s..."
                    )

                    # Wait for the device to fully disappear and reappear
                    time.sleep(60)

                    self._rebooting = False
                    logger.info("Reboot window closed. Resuming normal operations.")
                else:
                    logger.error("Failed to send reboot command to device.")
                    # Ensure we are disconnected even on failure
                    self.device.disconnect()

        except Exception as e:
            logger.error(f"Reboot worker crashed: {e}")

    def _discovery_worker(self, interval):
        while not self._stop_event.is_set():
            try:
                # Confirm no reboot is in progress before attempting communication
                if self._rebooting:
                    logger.debug("Discovery cycle skipped: Reboot in progress.")
                    time.sleep(10)
                    continue

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
                            logger.error(f"DB Error processing node '{node_info}': {e}")
                else:
                    logger.debug("No contacts found on device.")

                # --- Stale Node Cleanup Logic ---
                try:
                    now_ts = time.time()
                    stale_threshold = 180 * 24 * 60 * 60  # 180 days in seconds
                    epoch_2000 = 946684800  # Jan 1, 2000

                    active_nodes = self.db.get_all_nodes()
                    for node in active_nodes:
                        if node.get("is_home"):
                            continue
                        last_adv = node.get("last_advert")
                        pk = node.get("public_key")
                        if pk and last_adv and last_adv > epoch_2000:
                            if (now_ts - last_adv) > stale_threshold:
                                logger.info(
                                    f"Purging stale node: {node.get('adv_name') or pk}"
                                )
                                if self.device.remove_contact(pk):
                                    self.db.mark_node_inactive(pk)
                except Exception as e:
                    logger.error(f"Error during stale node cleanup: {e}")

                # --- Periodic Clock Sync ---
                # Only sync if it's been more than 4 hours since the last one
                if time.time() - self._last_clock_sync > 14400:
                    logger.info("Performing periodic node clock sync...")
                    if self.device.sync_clock():
                        self._last_clock_sync = time.time()
                        logger.info("Clock sync successful.")

                self.mqtt.send_status("updated")
                end_time = time.time()
                duration = end_time - start_time
                logger.info(f"Node discovery cycle completed in {duration:.2f}s.")

                # Ensure we sleep for at least the interval
                sleep_duration = max(120, interval) - duration
                if sleep_duration > 0:
                    time.sleep(sleep_duration)
            except Exception as e:
                # This is the critical fix: prevent Thread-3 from dying.
                # If the device is missing (OSError 6), we wait 30s and try again.
                logger.error(f"Discovery worker iteration failed: {e}")
                time.sleep(30)

    def get_connection_stats(self):
        return {
            "connected": self.mqtt.mqtt_connected,
            "published_messages": self.mqtt.published_messages,
            "failed_messages": self.mqtt.failed_messages,
            "last_publish_status": self.mqtt.last_publish_status,
        }

    def cleanup(self):
        self._stop_event.set()
        self.device.disconnect()
        self.mqtt.client.loop_stop()
        self.mqtt.client.disconnect()
