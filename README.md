# meshcore-node-list

A small Flask web app that lists Meshcore nodes visible from a local monitor and optionally forwards events to MQTT. It is intended to provide two main functions:

- A simple webpage showing the current nodes (name, role, location, distance, hops, last heard), with an interactive map view.
- Background monitoring that publishes MQTT messages when new nodes appear and when messages are received by the monitor.

Version: see [flask-app/version.txt](flask-app/version.txt)

## Key components

- Web frontend and app entry: [flask-app/app.py](flask-app/app.py). The Flask application instance and routes (including `start_background_monitor`) live here.
  - Symbol: [`start_background_monitor`](flask-app/app.py)
- Feed parsing and presentation: [flask-app/includes/feed.py](flask-app/includes/feed.py), used to convert the JSON node dump into table headers and rows for the template.
  - Symbol: [`includes.feed.parse_feed`](flask-app/includes/feed.py)
  - Distance calculation: [`includes.maths.line_of_sight_distance`](flask-app/includes/maths.py)
- Background monitor, MQTT integration and message parsing: [flask-app/classes/mesh_monitor.py](flask-app/classes/mesh_monitor.py)
  - Symbol: [`classes.mesh_monitor.MeshMonitor`](flask-app/classes/mesh_monitor.py)
  - Message parsing helper: `parse_mesh_message_advanced` inside the same file.
- Frontend template and styles:
  - Template: [flask-app/templates/index.html.j2](flask-app/templates/index.html.j2)
  - CSS: [flask-app/static/css/styles.css](flask-app/static/css/styles.css) and [flask-app/static/css/styles-new.css](flask-app/static/css/styles-new.css)
- Docker and orchestration:
  - Container build: [flask-app/Dockerfile](flask-app/Dockerfile)
  - Example compose configuration: [docker-compose.yml](docker-compose.yml)
- Helper script for local node collection (non-container): [scripts/get_nodes.sh](scripts/get_nodes.sh)

## How it works (high level)

1. The background monitor (or the included script scripts/get_nodes.sh) collects node information and persists it to a SQLite database (`node_data/nodes.db`).
2. The Flask route `/` queries the database (via `load_entries` in `app.py`) and calls `includes.feed.parse_feed` to build the table shown by `index.html.j2`.
3. If MQTT or Serial is enabled, `classes.mesh_monitor.MeshMonitor` runs in a background thread. It:
   - Detects newly seen nodes and publishes announcements to the configured MQTT topic.
   - Listens to the serial device for real-time mesh events, parses them, and publishes structured MQTT messages.
4. A "View Map" button opens a modal dialog displaying all nodes with location data on a Google Map. This requires a `GOOGLE_MAPS_API_KEY`.

## Configurable settings

You can configure behavior via environment variables (set them in a `.env` file or in your container):

- App / file locations
  - NODE_DATA_FILE — path to the SQLite database (default `/app/node_data/nodes.db`)  
  - MESSAGE_DATA_FILE — path to message dump (default `/app/node_data/node_messages.txt`)
  - Version file used in the footer: [flask-app/version.txt](flask-app/version.txt)
  - GOOGLE_MAPS_API_KEY — Your Google Maps JavaScript API key. If not provided, the map feature will be disabled.

- MQTT / monitor
  - MQTT_HOST — hostname or IP of MQTT broker (if not set, monitor will not start)
  - MQTT_PORT — MQTT port (default 1883)
  - MQTT_USERNAME / MQTT_PASSWORD — credentials (optional)
  - MQTT_NODE_TOPIC — topic for node announcements (default `mesh/nodes/new`)
  - MQTT_MESSAGE_TOPIC — topic for parsed messages (default `mesh/messages`)
  - MONITOR_INTERVAL — seconds between node checks (default 30)

- Serial / device control (used by the monitor)
  - MESH_SERIAL_DEVICE — serial device path (default `/dev/ttyACM0`)
  - MESH_SERIAL_ENABLED — `true`/`false` to enable serial polling (default `true`)

See the example environment file: [flask-app/.env-example](flask-app/.env-example)

The container configuration in [docker-compose.yml](docker-compose.yml) maps the node_data and logs directories and also demonstrates environment variables used by the service.

## Running

### Bash script
- Locally: run the helper script to populate node data:
  - [scripts/get_nodes.sh](scripts/get_nodes.sh)
This should be set up as a cron job so that the node list is periodically picked up

### Flask app
- Locally (Native Python): Start the Flask app using `uv` (ensure environment variables are set in `.env` or exported)
  - `cd flask-app; uv sync && uv run python app.py`

- With Docker Compose:
  - Build and run the service: `docker-compose up --build`
  - The compose file uses the Docker image built from [flask-app/Dockerfile](flask-app/Dockerfile)

## Development notes

- Parsing and presentation:
  - The JSON -> table transformation happens in [`includes.feed.parse_feed`](flask-app/includes/feed.py); distance formatting calls [`includes.maths.line_of_sight_distance`](flask-app/includes/maths.py)
- Monitoring and MQTT:
  - [`classes.mesh_monitor.MeshMonitor`](flask-app/classes/mesh_monitor.py) handles MQTT connections, new-node detection (persisting known node keys to `node_data/known_nodes.json`) and message polling/processing
- Tweak CSS in [flask-app/static/css/styles.css](flask-app/static/css/styles.css) or replace with [styles-new.css](flask-app/static/css/styles-new.css) if you prefer the alternative theme

## Files of interest

- Application: [flask-app/app.py](flask-app/app.py)
- Monitor class: [flask-app/classes/mesh_monitor.py](flask-app/classes/mesh_monitor.py)
- Feed parsing: [flask-app/includes/feed.py](flask-app/includes/feed.py)
- Distance maths: [flask-app/includes/maths.py](flask-app/includes/maths.py)
- Template: [flask-app/templates/index.html.j2](flask-app/templates/index.html.j2)
- Dockerfile: [flask-app/Dockerfile](flask-app/Dockerfile)
- Compose: [docker-compose.yml](docker-compose.yml)
- Helper script: [scripts/get_nodes.sh](scripts/get_nodes.sh)

## License

This repository is distributed under the terms of the GNU GPL v3. See [LICENSE](LICENSE).
