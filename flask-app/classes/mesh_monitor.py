import json
import time
import subprocess
import paho.mqtt.client as mqtt
import logging
import os
import re
from pathlib import Path
from typing import Dict, Set, Any
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_mesh_message_advanced(raw_message):
    """
    Parse mesh messages with ANSI escape codes into structured parts.
    """
    # Remove ANSI escape codes
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    clean_message = ansi_escape.sub('', raw_message)
    
    # Try different patterns
    patterns = [
        # Pattern 1: Sender (Status): Message
        r'^(.+?)\s*(\([^)]+\)):\s*(.+)$',
        # Pattern 2: Sender: Message (no status)
        r'^(.+?):\s*(.+)$',
        # Pattern 3: Just message content
        r'^(.+)$'
    ]
    
    for pattern in patterns:
        match = re.match(pattern, clean_message)
        if match:
            if pattern == patterns[0]:  # Sender (Status): Message
                return {
                    'sender': match.group(1).strip(),
                    'status': match.group(2).strip(),
                    'message': match.group(3).strip(),
                    'raw': raw_message,
                    'clean': clean_message
                }
            elif pattern == patterns[1]:  # Sender: Message
                return {
                    'sender': match.group(1).strip(),
                    'status': None,
                    'message': match.group(2).strip(),
                    'raw': raw_message,
                    'clean': clean_message
                }
            else:  # Just message
                return {
                    'sender': None,
                    'status': None,
                    'message': match.group(1).strip(),
                    'raw': raw_message,
                    'clean': clean_message
                }
    
    # Fallback
    return {
        'sender': None,
        'status': None,
        'message': clean_message,
        'raw': raw_message,
        'clean': clean_message
    }


class MeshMonitor:
    def __init__(self, mqtt_config: Dict[str, Any], node_data_file: str, message_data_file: str):
        self.mqtt_config = mqtt_config
        self.node_data_file = Path(node_data_file)
        self.message_data_file = Path(message_data_file)
        
        # Serial device configuration
        self.serial_device = os.environ.get('MESH_SERIAL_DEVICE', '/dev/ttyACM0')
        self.serial_enabled = os.environ.get('MESH_SERIAL_ENABLED', 'true').lower() == 'true'
    # Topic for node announcements (base topic)
        self.node_topic = self.mqtt_config.get("node_topic", "mesh/nodes/new")
        # Separate topic for status messages (defaults to <node_topic>/status)
        self.status_topic = self.mqtt_config.get("status_topic", "mesh/status")
        
        # MQTT connection tracking
        self.mqtt_connected = False
        self.last_mqtt_connection_check = 0
        self.published_messages = 0
        self.failed_messages = 0
        self.last_publish_status = None
        
        # Ensure directory exists
        self.node_data_file.parent.mkdir(exist_ok=True)
        self.message_data_file.parent.mkdir(exist_ok=True)
        
        # Files to track state
        self.known_nodes_file = self.node_data_file.parent / "known_nodes.json"
        
        # Initialize known nodes
        self.known_nodes = self.load_known_nodes()
        
        # MQTT clients
        self.node_client = None
        self.message_client = None
        self.setup_mqtt()
        
        # Check serial device availability
        self.check_serial_device()
    
    def on_connect(self, client, userdata, flags, rc):
        """Callback for when the client receives a CONNACK response from the server."""
        if rc == 0:
            self.mqtt_connected = True
            logger.info("MQTT connected successfully")
        else:
            self.mqtt_connected = False
            connection_codes = {
                1: "Connection refused - incorrect protocol version",
                2: "Connection refused - invalid client identifier",
                3: "Connection refused - server unavailable",
                4: "Connection refused - bad username or password",
                5: "Connection refused - not authorised"
            }
            error_msg = connection_codes.get(rc, f"Connection refused - unknown error code {rc}")
            logger.error(f"MQTT connection failed: {error_msg}")
    
    def on_disconnect(self, client, userdata, rc):
        """Callback for when the client disconnects from the broker."""
        self.mqtt_connected = False
        if rc != 0:
            logger.warning(f"MQTT unexpected disconnection (code: {rc}) - will attempt to reconnect")
        else:
            logger.info("MQTT disconnected normally")
    
    def on_publish(self, client, userdata, mid):
        """Callback when a message is published successfully."""
        self.published_messages += 1
        logger.debug(f"Message {mid} confirmed published to MQTT broker")
        self.last_publish_status = "success"
    
    def on_log(self, client, userdata, level, buf):
        """Callback for MQTT log messages (useful for debugging)."""
        if level == mqtt.MQTT_LOG_DEBUG:
            logger.debug(f"MQTT: {buf}")
        elif level == mqtt.MQTT_LOG_INFO:
            logger.info(f"MQTT: {buf}")
        elif level == mqtt.MQTT_LOG_NOTICE:
            logger.info(f"MQTT: {buf}")
        elif level == mqtt.MQTT_LOG_WARNING:
            logger.warning(f"MQTT: {buf}")
        elif level == mqtt.MQTT_LOG_ERR:
            logger.error(f"MQTT: {buf}")
    
    def check_serial_device(self):
        """Check if serial device is available and log status"""
        if not self.serial_enabled:
            logger.info("Serial device monitoring is disabled via MESH_SERIAL_ENABLED")
            return
            
        if os.path.exists(self.serial_device):
            logger.info(f"Serial device {self.serial_device} is available")
        else:
            logger.warning(f"Serial device {self.serial_device} not found. Message polling will be disabled.")
    
    def setup_mqtt(self):
        """Setup MQTT connections for nodes and messages"""
        try:
            # Client for node announcements
            self.node_client = mqtt.Client()
            
            # Set up callbacks
            self.node_client.on_connect = self.on_connect
            self.node_client.on_disconnect = self.on_disconnect
            self.node_client.on_publish = self.on_publish
            self.node_client.on_log = self.on_log
            
            if self.mqtt_config.get("username"):
                self.node_client.username_pw_set(
                    self.mqtt_config["username"], 
                    self.mqtt_config.get("password", "")
                )
            
            # Set Last Will and Testament (LWT) to indicate offline state using ISO 8601 timestamp
            will_payload = json.dumps({
                "status": "offline",
                "timestamp": datetime.now().astimezone().isoformat()
            }, ensure_ascii=False)
            # Publish LWT to the dedicated status topic
            self.node_client.will_set(self.status_topic, payload=will_payload, qos=1, retain=True)
            
            self.node_client.connect(
                self.mqtt_config["host"], 
                self.mqtt_config.get("port", 1883),
                keepalive=60
            )
            self.node_client.loop_start()
            
            # Client for messages (can use same connection)
            self.message_client = self.node_client
            
            # Wait a moment for connection to establish
            time.sleep(2)
            # Publish a retained "started" status after attempting to connect
            try:
                # Try to send with confirmation; fall back to direct publish inside send_status
                self.send_status("started", retain=True)
            except Exception as e:
                logger.debug(f"Could not send started status: {e}")
            
            logger.info(f"MQTT client connecting to {self.mqtt_config['host']}:{self.mqtt_config.get('port', 1883)}")
            
        except Exception as e:
            logger.error(f"Failed to setup MQTT: {e}")
            self.mqtt_connected = False
    
    def check_mqtt_connection(self):
        """Check if MQTT connection is still healthy"""
        now = time.time()
        # Only check every 30 seconds to avoid log spam
        if now - self.last_mqtt_connection_check < 30:
            return self.mqtt_connected
            
        self.last_mqtt_connection_check = now
        
        if not self.mqtt_connected:
            logger.warning("MQTT connection is not active - attempting to reconnect...")
            try:
                self.node_client.reconnect()
                # Wait a moment for reconnection
                time.sleep(2)
            except Exception as e:
                logger.error(f"MQTT reconnection failed: {e}")
        
        return self.mqtt_connected
    
    def publish_with_confirmation(self, topic, payload, qos=1, retain=False, timeout=5):
        """
        Publish a message with confirmation.
        Returns True if published successfully, False otherwise.
        """
        try:
            if not self.check_mqtt_connection():
                logger.error("Cannot publish - MQTT connection is not available")
                self.failed_messages += 1
                self.last_publish_status = "failed_no_connection"
                return False
            
            # Reset publish status
            self.last_publish_status = None
            
            # Publish the message
            msg_info = self.message_client.publish(topic, payload, qos=qos, retain=retain)
            
            # Wait for the callback to be called (for QoS 1/2, this waits for the PUBACK)
            if msg_info.rc == mqtt.MQTT_ERR_SUCCESS:
                if qos > 0:
                    # For QoS 1/2, wait for the publish callback
                    msg_info.wait_for_publish(timeout=timeout)
                
                if self.last_publish_status == "success" or qos == 0:
                    logger.debug(f"Message published successfully to {topic}")
                    return True
                else:
                    logger.warning(f"Message publish confirmation not received within {timeout}s")
                    self.failed_messages += 1
                    self.last_publish_status = "failed_timeout"
                    return False
            else:
                logger.error(f"Failed to publish message (error code: {msg_info.rc})")
                self.failed_messages += 1
                self.last_publish_status = f"failed_error_{msg_info.rc}"
                return False
                
        except Exception as e:
            logger.error(f"Exception during MQTT publish: {e}")
            self.failed_messages += 1
            self.last_publish_status = "failed_exception"
            return False
    
    def get_connection_stats(self):
        """Get connection statistics"""
        return {
            "connected": self.mqtt_connected,
            "published_messages": self.published_messages,
            "failed_messages": self.failed_messages,
            "last_publish_status": self.last_publish_status,
            "success_rate": self.published_messages / max(1, self.published_messages + self.failed_messages) * 100
        }

    def send_status(self, status: str, retain: bool = False):
        """Publish a small JSON status message to the node topic with ISO 8601 timestamp.

        Tries a confirmed publish first (so counters/stats are updated). If that fails
        it falls back to a direct publish on the MQTT client to avoid crashing the monitor.
        """
        try:
            payload = json.dumps({
                "status": status,
                "timestamp": datetime.now().astimezone().isoformat()
            }, ensure_ascii=False)

            sent = False
            try:
                sent = self.publish_with_confirmation(self.status_topic, payload, qos=1, retain=retain)
            except Exception:
                sent = False

            if not sent:
                # Fallback to direct publish if confirmation path failed
                try:
                    if self.node_client:
                        self.node_client.publish(self.status_topic, payload, qos=1, retain=retain)
                except Exception as e:
                    logger.debug(f"send_status fallback publish failed: {e}")

        except Exception as e:
            logger.debug(f"send_status exception: {e}")
    
    def load_known_nodes(self) -> Set[str]:
        """Load previously known nodes from file"""
        try:
            if self.known_nodes_file.exists():
                with open(self.known_nodes_file, 'r') as f:
                    return set(json.load(f))
        except Exception as e:
            logger.error(f"Error loading known nodes: {e}")
        return set()
    
    def save_known_nodes(self):
        """Save current known nodes to file"""
        try:
            with open(self.known_nodes_file, 'w') as f:
                json.dump(list(self.known_nodes), f)
        except Exception as e:
            logger.error(f"Error saving known nodes: {e}")
    
    def check_for_new_nodes(self, node_list: list) -> list:
        """Check for new nodes and return any found"""
        new_nodes = []
        current_public_keys = set()
        
        for node in node_list:
            public_key = node.get("public_key")
            if not public_key:
                continue
                
            current_public_keys.add(public_key)
            
            # Check if this is a new node
            if public_key not in self.known_nodes:
                new_nodes.append(node)
                logger.info(f"New node detected: {node.get('adv_name', 'Unknown')} ({public_key[:8]}...)")
        
        # Update known nodes
        self.known_nodes = current_public_keys
        self.save_known_nodes()
        
        return new_nodes
    
    def send_node_announcement(self, node: Dict):
        """Send new node announcement via MQTT with confirmation"""
        try:
            topic = self.mqtt_config.get("node_topic", "mesh/nodes/new")
            message = json.dumps({
                "public_key": node.get("public_key"),
                "name": node.get("adv_name", "Unknown"),
                "type": node.get("type"),
                "location": {
                    "lat": node.get("adv_lat"),
                    "lon": node.get("adv_lon")
                },
                "last_advert": node.get("last_advert"),
                "timestamp": time.time()
            })
            
            success = self.publish_with_confirmation(topic, message, qos=1)
            
            if success:
                logger.info(f"MQTT announcement confirmed for node: {node.get('adv_name')}")
            else:
                logger.error(f"Failed to send MQTT announcement for node: {node.get('adv_name')}")
            
        except Exception as e:
            logger.error(f"Error sending node announcement: {e}")

    def poll_messages(self):
        """Poll for new messages using meshcli command"""
        # Check if serial is enabled and device exists
        if not self.serial_enabled:
            return False
            
        if not os.path.exists(self.serial_device):
            logger.warning(f"Serial device {self.serial_device} not available, skipping message polling")
            return False
            
        try:
            # Run the meshcli command
            result = subprocess.run([
                "uv", "run", "meshcli", "-s", self.serial_device, "sync_msgs"
            ], capture_output=True, text=True, timeout=30)
            
            # Check if command produced an error message
            if "Error:" in result.stdout or result.returncode != 0:
                logger.error(f"meshcli command failed: {result.stdout.strip()}")
                if result.stderr:
                    logger.error(f"meshcli stderr: {result.stderr.strip()}")
                return False
            
            # Write output to file only if we got valid data
            if result.stdout.strip():
                with open(self.message_data_file, 'w') as f:
                    f.write(result.stdout)
                logger.info("Successfully polled for messages")
            else:
                # No new messages
                return True
                
            return True
            
        except subprocess.TimeoutExpired:
            logger.error("meshcli command timed out")
        except Exception as e:
            logger.error(f"Error polling messages: {e}")
            
        return False
    
    def process_messages(self):
        """Process and send messages via MQTT with confirmation"""
        try:
            if not self.message_data_file.exists():
                return
            
            with open(self.message_data_file, 'r') as f:
                content = f.read().strip()
            
            if not content:
                return
            
            # Don't send error messages as MQTT messages
            if "Error:" in content:
                logger.error(f"Not sending error message via MQTT: {content}")
                # Clear the error message from file
                with open(self.message_data_file, 'w') as f:
                    f.write("")
                return
            
            # Send each line as a separate message
            topic = self.mqtt_config.get("message_topic", "mesh/messages")
            messages_sent = 0
            messages_failed = 0
            
            for line in content.split('\n'):
                line = line.strip()
                if line and "Error:" not in line:
                    # Parse the message to extract sender, status, and content
                    parsed_message = parse_mesh_message_advanced(line)
                    
                    message_data = {
                        "raw": parsed_message['raw'],
                        "clean": parsed_message['clean'],
                        "sender": parsed_message['sender'],
                        "status": parsed_message['status'],
                        "message": parsed_message['message'],
                        "timestamp": time.time(),
                        "source": "meshcli"
                    }
                    
                    success = self.publish_with_confirmation(
                        topic, 
                        json.dumps(message_data, ensure_ascii=False), 
                        qos=1
                    )
                    
                    if success:
                        messages_sent += 1
                        # Log the parsed message nicely
                        if parsed_message['sender'] and parsed_message['status']:
                            logger.info(f"Message confirmed sent - Sender: {parsed_message['sender']}, Status: {parsed_message['status']}, Message: {parsed_message['message']}")
                        elif parsed_message['sender']:
                            logger.info(f"Message confirmed sent - Sender: {parsed_message['sender']}, Message: {parsed_message['message']}")
                        else:
                            logger.info(f"Message confirmed sent: {parsed_message['message']}")
                    else:
                        messages_failed += 1
                        logger.error(f"Failed to send message via MQTT: {parsed_message['clean']}")
            
            if messages_failed > 0:
                logger.warning(f"Message batch completed: {messages_sent} sent, {messages_failed} failed")
            else:
                logger.info(f"Message batch completed: {messages_sent} sent successfully")
        
            # Clear the file after processing (only if all messages were processed or we don't care about failures)
            with open(self.message_data_file, 'w') as f:
                f.write("")
        
        except Exception as e:
            logger.error(f"Error processing messages: {e}")
    
    
    def monitor_loop(self, node_data_callback, check_interval: int = 30):
        """Main monitoring loop with connection health checks"""
        logger.info(f"Starting mesh network monitor (check interval: {check_interval}s)...")
        logger.info(f"Serial device: {self.serial_device} (enabled: {self.serial_enabled})")
        
        # Log connection stats periodically
        last_stats_log = 0
        stats_interval = 300  # 5 minutes
        
        while True:
            try:
                # Publish a periodic "updated" heartbeat each check
                try:
                    self.send_status("updated")
                except Exception:
                    pass
                # Periodically log connection statistics
                now = time.time()
                if now - last_stats_log >= stats_interval:
                    stats = self.get_connection_stats()
                    logger.info(f"MQTT Connection Stats: {stats['success_rate']:.1f}% success rate "
                               f"({stats['published_messages']} sent, {stats['failed_messages']} failed)")
                    last_stats_log = now
                
                # Check MQTT connection health
                self.check_mqtt_connection()
                
                # Get current node list from callback (which reads the file)
                current_nodes = node_data_callback()
                
                if current_nodes:
                    # Check for new nodes
                    new_nodes = self.check_for_new_nodes(current_nodes)
                    
                    # Send announcements for new nodes
                    for node in new_nodes:
                        self.send_node_announcement(node)
                
                # Poll for messages (only if serial is available)
                if self.serial_enabled and os.path.exists(self.serial_device):
                    if self.poll_messages():
                        self.process_messages()
                else:
                    # Sleep a bit longer if serial is not available to avoid log spam
                    time.sleep(check_interval * 2)
                    continue
                
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                logger.info("Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(check_interval)
    
    
    def cleanup(self):
        """Cleanup resources"""
        if self.node_client:
            # Send a disconnect message
            if self.mqtt_connected:
                shutdown_payload = json.dumps({
                    "status": "shutdown",
                    "timestamp": datetime.now().astimezone().isoformat()
                })
                try:
                    self.node_client.publish(self.status_topic, payload=shutdown_payload, qos=1, retain=True)
                except Exception as e:
                    logger.debug(f"Cleanup publish failed: {e}")
                time.sleep(1)  # Give it a moment to send
            
            self.node_client.loop_stop()
            self.node_client.disconnect()
            logger.info("MQTT client disconnected")