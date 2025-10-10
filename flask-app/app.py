from flask import Flask, request, render_template
from datetime import datetime
from zoneinfo import ZoneInfo 
import os
import time
import logging
import json
import re
from includes.feed import parse_feed

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
last_modified=""
CACHE_TIMEOUT = 60  # seconds

# Get the other environment variables
node_data_file = os.environ.get('node_data_file', '/app/node_data/nodes.json')

# Version
with open('version.txt') as vf:
    APP_VERSION = vf.read().strip()
    vf.close()

def load_entries():
    global entries_cache, last_loaded,last_modified
    now = time.time()
    
    # If cache expired or never loaded, reload file
    if entries_cache is None or now - last_loaded > CACHE_TIMEOUT:
        file_timestamp= os.path.getmtime(node_data_file)
        # convert to datetime
        dt = datetime.fromtimestamp(file_timestamp, tz=ZoneInfo("Europe/London"))
        # format as string
        last_modified = f'{dt.strftime("%Y-%m-%d %H:%M:%S")} UK time'

        with open(node_data_file, "r") as f:
            entries_cache = json.load(f)
        last_loaded = now
        print("File reloaded at", time.strftime("%X"))  # For debugging
    
    return entries_cache

@app.before_request
def log_request():
    logger.info(f"Request: {request.method} {request.path}")

@app.route('/')
def serve_index():
    entries = load_entries()
    headers,data=parse_feed(entries)
    # parse_feed(entries)
    return render_template('index.html.j2', headers=headers, data=data, version=APP_VERSION, last_modified=last_modified, enumerate=enumerate)

if __name__ == '__main__':
    try:
        from waitress import serve
        logger.info("Starting production server on HTTP...")
        serve(app, host='0.0.0.0', port=5050)
    except ImportError:
        logger.info("Starting development server on HTTP...")
        app.run(host='0.0.0.0', port=5050)

