let map;
let infoWindow;
let bounds;
let markers = [];
window.mapInitialized = false;

// Role to color mapping from styles-new.css
const roleColors = {
    'CHAT': '#3b82f6',       // .role-client
    'REPEATER': '#73137a',   // .role-repeater
    'SENSOR': '#93931a',     // .role-other
    'ROOM': '#6313aa',       // .role-router (closest match)
    'NONE': '#9ca3af',       // .role-na
    'OTHER': '#9ca3af'
};

async function initMap() {
    // This function is called by the Google Maps API script when it's ready.
    const mapElement = document.getElementById('map-container');
    if (!mapElement) {
        console.error("Map container not found");
        return;
    }

    // Default center (London) in case there are no nodes
    const defaultCenter = { lat: 51.5074, lng: -0.1278 };

    map = new google.maps.Map(mapElement, {
        center: defaultCenter,
        zoom: 8,
        mapId: 'MESHCORE_NODE_MAP' // An ID for potential cloud-based map styling
    });

    infoWindow = new google.maps.InfoWindow();
    bounds = new google.maps.LatLngBounds();

    try {
        const response = await fetch('/map-data');
        const nodes = await response.json();

        if (nodes.length === 0) {
            mapElement.innerHTML = '<p style="text-align: center; padding: 20px;">No nodes with location data available to display on map.</p>';
            return;
        }

        nodes.forEach(node => {
            const position = { lat: node.lat, lng: node.lon };
            const role = node.role.toUpperCase();
            const color = roleColors[role] || roleColors['OTHER'];

            // Create a modern marker with a short name and role-based color
            const pinGlyph = new google.maps.marker.PinElement({
                glyph: node.name.substring(0, 2).toUpperCase(),
                glyphColor: "white",
                background: color,
                borderColor: "#505050",
            });

            const marker = new google.maps.marker.AdvancedMarkerElement({
                position: position,
                map: map,
                title: `${node.name} (${node.role})`,
                content: pinGlyph.element,
            });

            // Add a click listener for the tooltip
            marker.addListener('click', () => {
                infoWindow.setContent(`<strong>${node.name}</strong><br>Role: ${node.role}`);
                infoWindow.open(map, marker);
            });

            markers.push(marker);
            bounds.extend(position);
        });

        // Set globals to be used when the modal is opened
        window.map = map;
        window.bounds = bounds;
        window.markers = markers;
        window.mapInitialized = true;

    } catch (error) {
        console.error('Error fetching or processing map data:', error);
        mapElement.innerHTML = '<p style="text-align: center; padding: 20px;">Could not load map data.</p>';
    }
}

// Expose the function to the global scope for the callback
window.initMap = initMap;