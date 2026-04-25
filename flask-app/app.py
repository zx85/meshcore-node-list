from flask import Flask, request, render_template, jsonify, send_from_directory
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import time
import logging
import json
import re
import threading
import atexit
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from includes.feed import parse_feed
from classes.mesh_monitor import MeshMonitor

# Configure application
app = Flask(__name__, static_folder="static")
app.secret_key = os.environ.get("SECRET_KEY", "fallback-secret-key")

# Session cookie settings for reverse proxy/HTTPS at nginx
app.config["SESSION_COOKIE_SECURE"] = False  # HTTPS is terminated at nginx
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cache variables
entries_cache = None
last_loaded = 0
last_modified = ""
CACHE_TIMEOUT = 60  # seconds

# Get the environment variables from .env file
node_data_file = os.environ.get("NODE_DATA_FILE", "/app/node_data/nodes.json")
message_data_file = os.environ.get(
    "MESSAGE_DATA_FILE", "/app/node_data/node_messages.txt"
)

# Mesh Monitor instance - global so it persists across requests
mesh_monitor = None
monitor_thread = None
monitor_started = False

# Get Google Maps API Key
google_maps_api_key = os.environ.get("GOOGLE_MAPS_API_KEY")

hours = int(os.environ.get("HOURS", "48"))

# Version
with open("version.txt") as vf:
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
        print("File reloaded at", time.strftime("%X"))  # For debugging

    return entries_cache


def get_current_nodes():
    """Get current nodes from the node data file"""
    try:
        if os.path.exists(node_data_file):
            with open(node_data_file, "r") as f:
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
    mqtt_host = os.environ.get("MQTT_HOST")
    if not mqtt_host:
        logger.warning("MQTT_HOST not set in .env file - MQTT monitoring disabled")
        monitor_started = True
        return

    # MQTT configuration from environment variables
    mqtt_config = {
        "host": mqtt_host,
        "port": int(os.environ.get("MQTT_PORT", "1883")),
        "username": os.environ.get("MQTT_USERNAME"),
        "password": os.environ.get("MQTT_PASSWORD"),
        "node_topic": os.environ.get("MQTT_NODE_TOPIC", "meshcore/nodes/new"),
        "status_topic": os.environ.get("MQTT_STATUS_TOPIC", "meshcore/status"),
        "message_topic": os.environ.get("MQTT_MESSAGE_TOPIC", "meshcore/messages"),
    }

    try:
        # Create mesh monitor instance
        mesh_monitor = MeshMonitor(mqtt_config, node_data_file, message_data_file)

        # Start monitor in background thread
        monitor_thread = threading.Thread(
            target=mesh_monitor.monitor_loop,
            args=(get_current_nodes, int(os.environ.get("MONITOR_INTERVAL", "30"))),
            daemon=True,
        )
        monitor_thread.start()
        monitor_started = True
        logger.info("Mesh monitor started in background thread")

    except Exception as e:
        logger.error(f"Failed to start mesh monitor: {e}")
        monitor_started = True  # Don't keep trying if it failed


def cleanup_mesh_monitor():
    """Cleanup mesh monitor resources - only called on application shutdown"""
    global mesh_monitor
    if mesh_monitor:
        logger.info("Shutting down mesh monitor...")
        mesh_monitor.cleanup()
        mesh_monitor = None
        logger.info("Mesh monitor shutdown complete")


# Register cleanup for application shutdown
atexit.register(cleanup_mesh_monitor)


@app.before_request
def initialize_monitor():
    """Ensure monitor is running (will only start once)"""
    global monitor_started
    if not monitor_started:
        logger.info("Initializing mesh monitor on first request...")
        start_background_monitor()


@app.before_request
def log_request():
    logger.info(f"Request: {request.method} {request.path}")


@app.route("/favicon.ico")
def favicon():
    return send_from_directory(
        os.path.join(app.root_path, "static"),
        "favicon.ico",
        mimetype="image/vnd.microsoft.icon",
    )


@app.route("/")
def serve_index():
    entries = load_entries()
    headers, data = parse_feed(feed=entries, hours=hours)
    return render_template(
        "index.html.j2",
        headers=headers,
        data=data,
        version=APP_VERSION,
        last_modified=last_modified,
        google_maps_api_key=google_maps_api_key,
        enumerate=enumerate,
        hours=hours,
    )


@app.route("/map-data")
def map_data():
    """Provides node data as JSON for the map."""
    nodes = load_entries()
    map_nodes = []

    # From includes/feed.py
    node_types = {0: "NONE", 1: "CHAT", 2: "REPEATER", 3: "ROOM", 4: "SENSOR"}

    cutoff_timestamp = time.time() - (48 * 60 * 60)

    if not nodes:
        return jsonify([])

    # First node is the home node
    home_node = nodes[0]
    if home_node.get("adv_lat") and home_node.get("adv_lon"):
        map_nodes.append(
            {
                "name": home_node.get("name", "Home"),
                "role": "CHAT",  # Home node is always a CHAT client
                "lat": home_node.get("adv_lat"),
                "lon": home_node.get("adv_lon"),
            }
        )

    # The rest are advertised nodes
    for node in nodes[1:]:
        last_advert_epoch = node.get("last_advert")
        if not last_advert_epoch or last_advert_epoch < cutoff_timestamp:
            continue

        lat = node.get("adv_lat")
        lon = node.get("adv_lon")
        if lat and lon and (lat * lon != 0):
            map_nodes.append(
                {
                    "name": node.get("adv_name", "Unknown"),
                    "role": node_types.get(node.get("type"), "OTHER").replace(",", ""),
                    "lat": lat,
                    "lon": lon,
                }
            )

    return jsonify(map_nodes)


@app.route("/status")
def status_check():
    """Status check endpoint with MQTT connection info"""
    global mesh_monitor, monitor_started

    mqtt_stats = {}
    if mesh_monitor:
        mqtt_stats = mesh_monitor.get_connection_stats()

    return {
        "status": "healthy",
        "version": APP_VERSION,
        "monitor_running": monitor_started,
        "mqtt_connected": mqtt_stats.get("connected", False),
        "mqtt_stats": mqtt_stats,
        "timestamp": datetime.now().isoformat(),
    }


@app.route("/health")
def health_check():
    """Simple health check endpoint"""
    return {
        "status": "healthy",
        "version": APP_VERSION,
        "timestamp": datetime.now().isoformat(),
    }


if __name__ == "__main__":
    # Start monitor immediately when running directly
    start_background_monitor()

    try:
        from waitress import serve

        logger.info("Starting production server on HTTP...")
        serve(app, host="0.0.0.0", port=5050)
    except ImportError:
        logger.info("Starting development server on HTTP...")
        app.run(host="0.0.0.0", port=5050)
