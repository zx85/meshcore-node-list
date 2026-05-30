import asyncio
from meshcore import MeshCore, EventType
import json
import os


async def main():
    device_path = os.environ.get("MESH_SERIAL_DEVICE", "/dev/ttyACM0")
    print(f"--- DIAGNOSTIC MODE: Connecting to {device_path} ---")

    # We enable debug=True to see raw serial traffic in the console
    try:
        meshcore = await MeshCore.create_serial(device_path, debug=True)
    except Exception as e:
        print(f"FAILED TO CONNECT: {e}")
        print(
            "Check if another process (like meshcli or a terminal) is using the port."
        )
        return

    # Enable auto-fetching of messages so that CONTACT_MSG_RECV triggers
    print("Enabling auto message fetching...")
    await meshcore.start_auto_message_fetching()

    # A "catch-all" handler to see everything the node sends
    async def universal_handler(event):
        print(f"\n[EVENT RECEIVED] Type: {event.type}")
        print(f"Payload: {json.dumps(event.payload, indent=2)}")

    print("Subscribing to all event types...")
    # Subscribe to the most common events for debugging
    for e_type in [
        EventType.CONTACT_MSG_RECV,
        EventType.ADVERTISEMENT,
        EventType.NODE_INFO,
    ]:
        meshcore.subscribe(e_type, universal_handler)

    print("Listening for 60 seconds... Send a message or advert now.")

    try:
        # Run for a minute then exit
        await asyncio.sleep(60)
    except KeyboardInterrupt:
        pass
    finally:
        print("Closing connection.")
        await meshcore.disconnect()


asyncio.run(main())
