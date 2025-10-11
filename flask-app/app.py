from flask import Flask, request, render_template
from datetime import datetime
from zoneinfo import ZoneInfo 
import os
import time
import logging
import json
import re
import threading
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from includes.feed import parse_feed
from classes.mesh_monitor import MeshMonitor

# Configure application
app = Flask(__name__, static_folder='static')
app.secret_key = os.environ.get('SECRET_KEY', 'fallback-secret-key')

# Session cookie settings for reverse proxy/HTTPS at nginx
app.config['SESSION_COOKIE_SECURE'] = False  # HTTPS is terminated at nginx
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cache variables
entries_cache = None
last_loaded = 0
last_modified = ""
CACHE_TIMEOUT = 60  # seconds

# Get the environment variables from .env file
node_data_file = os.environ.get('NODE_DATA_FILE', '/app/node_data/nodes.json')
message_data_file = os.environ.get('MESSAGE_DATA_FILE', '/app/node_data/node_messages.txt')

# Mesh Monitor instance
mesh_monitor = None
monitor_thread = None
monitor_started = False

# Version
with open('version.txt') as vf:
    APP_VERSION = vf.read().strip()
    vf.close()

def load_entries():
    global entries_cache, last_loaded, last_modified
    now = time.time()
    
    # If cache expired or never loaded, reload file
    if entries_cache is None or now - last_loaded > CACHE_TIMEOUT:
        file_timestamp = os.path.getmtime(node_data_file)
        # convert to datetime
        dt = datetime.fromtimestamp(file_timestamp, tz=ZoneInfo("Europe/London"))
        # format as string
        last_modified = f'{dt.strftime("%Y-%m-%d %H:%M:%S")} UK time'

        with open(node_data_file, "r") as f:
            entries_cache = json.load(f)
        last_loaded = now
        logger.debug(f'File reloaded at {time.strftime("%X")}')  # For debugging
    
    return entries_cache

def get_current_nodes():
    """Get current nodes from the node data file"""
    try:
        if os.path.exists(node_data_file):
            with open(node_data_file, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error reading node data file: {e}")
    return []

def start_background_monitor():
    """Start the mesh monitor in a background thread"""
    global mesh_monitor, monitor_thread, monitor_started
    
    if monitor_started:
        return
        
    # Check if MQTT is configured
    mqtt_host = os.environ.get('MQTT_HOST')
    if not mqtt_host:
        logger.warning("MQTT_HOST not set in .env file - MQTT monitoring disabled")
        monitor_started = True
        return
    
    # MQTT configuration from environment variables
    mqtt_config = {
        "host": mqtt_host,
        "port": int(os.environ.get('MQTT_PORT', '1883')),
        "username": os.environ.get('MQTT_USERNAME'),
        "password": os.environ.get('MQTT_PASSWORD'),
        "node_topic": os.environ.get('MQTT_NODE_TOPIC', 'mesh/nodes/new'),
        "message_topic": os.environ.get('MQTT_MESSAGE_TOPIC', 'mesh/messages')
    }
    
    try:
        # Create mesh monitor instance
        mesh_monitor = MeshMonitor(mqtt_config, node_data_file, message_data_file)
        
        # Start monitor in background thread
        monitor_thread = threading.Thread(
            target=mesh_monitor.monitor_loop,
            args=(get_current_nodes, int(os.environ.get('MONITOR_INTERVAL', '30'))),
            daemon=True
        )
        monitor_thread.start()
        monitor_started = True
        logger.info("Mesh monitor started in background thread")
        
    except Exception as e:
        logger.error(f"Failed to start mesh monitor: {e}")
        monitor_started = True  # Don't keep trying if it failed

@app.before_request
def before_request():
    """Start monitor on first request"""
    global monitor_started
    if not monitor_started:
        logger.info("Starting mesh monitor on first request...")
        start_background_monitor()

@app.before_request
def log_request():
    logger.info(f"Request: {request.method} {request.path}")

@app.route('/')
def serve_index():
    entries = load_entries()
    headers, data = parse_feed(entries)
    return render_template('index.html.j2', headers=headers, data=data, 
                         version=APP_VERSION, last_modified=last_modified, 
                         enumerate=enumerate)

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "version": APP_VERSION,
        "monitor_running": monitor_started,
        "timestamp": datetime.now().isoformat()
    }

def cleanup():
    """Cleanup when app shuts down"""
    global mesh_monitor
    if mesh_monitor:
        mesh_monitor.cleanup()
        logger.info("Mesh monitor cleaned up")

# Register cleanup function to run when the app context tears down
@app.teardown_appcontext
def teardown(exception=None):
    cleanup()

# Also register for when the process exits
import atexit
atexit.register(cleanup)

if __name__ == '__main__':
    # Start monitor immediately when running directly
    start_background_monitor()
    
    try:
        from waitress import serve
        logger.info("Starting production server on HTTP...")
        serve(app, host='0.0.0.0', port=5050)
    except ImportError:
        logger.info("Starting development server on HTTP...")
        app.run(host='0.0.0.0', port=5050)