import asyncio
from meshcore import MeshCore, EventType
import json

async def main():
    # Connect to your device
    meshcore = await MeshCore.create_serial("/dev/ttyACM0")
    
    # Get your contacts
    result = await meshcore.commands.get_contacts()
    if result.type == EventType.ERROR:
        print(f"Error getting contacts: {result.payload}")
        return
        
    contacts = result.payload
    print(f"Found {len(contacts)} contacts")
    print(json.dumps(contacts, indent=2))
    await meshcore.disconnect()

asyncio.run(main())