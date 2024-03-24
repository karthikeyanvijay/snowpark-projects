#!/bin/python
import json
import time
from datetime import datetime
import random

def generate_telemetry_data(device_id):
    """Generates mock telemetry data with additional fields for a specific device, using UTC time for the timestamp."""
    data = {
        'timestamp': datetime.utcnow().isoformat() + "Z",  # Generate UTC timestamp and append 'Z' to indicate UTC
        'device_id': device_id,
        'temperature': round(random.uniform(20, 30), 2),   # Random temperature between 20 and 30
        'humidity': round(random.uniform(30, 60), 2),      # Random humidity between 30 and 60
        'pressure': round(random.uniform(980, 1020), 2),   # Random pressure between 980 and 1020 hPa
        'altitude': round(random.uniform(100, 500), 2),    # Random altitude between 100 and 500 meters
        'battery_level': round(random.uniform(20, 100), 2) # Random battery level between 20% and 100%
    }
    return data

def emit_telemetry_data():
    """Continuously emits telemetry data for 12 different devices."""
    device_ids = [f"device{i:03}" for i in range(1, 13)]  # Generates device IDs from device001 to device012
    
    try:
        while True:
            for device_id in device_ids:
                # Generate telemetry data for the current device
                data = generate_telemetry_data(device_id)
                
                # Convert data to JSON format
                json_data = json.dumps(data)
                
                # Emit the data
                print(json_data)
            
            # Wait for a second before generating the next batch of data for all devices
            time.sleep(2)
    except KeyboardInterrupt:
        print("Program terminated by user.")

if __name__ == "__main__":
    emit_telemetry_data()

