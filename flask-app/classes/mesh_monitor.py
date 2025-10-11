import json
import time
import subprocess
import paho.mqtt.client as mqtt
import logging
import os
import re 
from pathlib import Path
from typing import Dict, Set, Any

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
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
            if self.mqtt_config.get("username"):
                self.node_client.username_pw_set(
                    self.mqtt_config["username"], 
                    self.mqtt_config.get("password", "")
                )
            self.node_client.connect(
                self.mqtt_config["host"], 
                self.mqtt_config.get("port", 1883),
                keepalive=60
            )
            self.node_client.loop_start()
            
            # Client for messages (can use same connection)
            self.message_client = self.node_client
            
            logger.info(f"MQTT client connected to {self.mqtt_config['host']}:{self.mqtt_config.get('port', 1883)}")
            
        except Exception as e:
            logger.error(f"Failed to setup MQTT: {e}")
    
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
        """Send new node announcement via MQTT"""
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
            
            self.node_client.publish(topic, message, qos=1)
            logger.info(f"Sent MQTT announcement for node: {node.get('adv_name')}")
            
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
        """Process and send messages via MQTT"""
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
                    
                    self.message_client.publish(
                        topic, 
                        json.dumps(message_data, ensure_ascii=False), 
                        qos=1
                    )
                    
                    # Log the parsed message nicely
                    if parsed_message['sender'] and parsed_message['status']:
                        logger.info(f"Sent message - Sender: {parsed_message['sender']}, Status: {parsed_message['status']}, Message: {parsed_message['message']}")
                    elif parsed_message['sender']:
                        logger.info(f"Sent message - Sender: {parsed_message['sender']}, Message: {parsed_message['message']}")
                    else:
                        logger.info(f"Sent message: {parsed_message['message']}")
            
            # Clear the file after processing
            with open(self.message_data_file, 'w') as f:
                f.write("")
            
        except Exception as e:
            logger.error(f"Error processing messages: {e}")
    
    def monitor_loop(self, node_data_callback, check_interval: int = 30):
        """Main monitoring loop"""
        logger.info(f"Starting mesh network monitor (check interval: {check_interval}s)...")
        logger.info(f"Serial device: {self.serial_device} (enabled: {self.serial_enabled})")
        
        while True:
            try:
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
            self.node_client.loop_stop()
            self.node_client.disconnect()
            logger.info("MQTT client disconnected")