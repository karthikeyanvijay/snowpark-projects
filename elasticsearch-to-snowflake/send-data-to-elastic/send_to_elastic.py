#!python
import subprocess
import json
import requests
import logging
import signal
import sys
import threading

api_key = "XXXXXXXXXXXXXXXXX"
es_url = "https://hostname.us-east-2.aws.elastic-cloud.com:443/_bulk?pipeline=timestamp_pipeline&pretty"


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
headers = {
    "Authorization": f"ApiKey {api_key}",
    "Content-Type": "application/json"
}

# Path to the telemetry emitting Python script
script_path = "emit_json_telemetry.py"

# Signal handler for graceful exit
def signal_handler(sig, frame):
    logging.info('You pressed Ctrl+C! Exiting gracefully...')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Function to send data to Elasticsearch
def send_to_elasticsearch(data):
    logging.info("Sending data to Elasticsearch...")
    try:
        response = requests.post(es_url, headers=headers, data=data)
        if response.status_code == 200:
            logging.info("Data successfully sent to Elasticsearch.")
        else:
            logging.error(f"Failed to send data to Elasticsearch: {response.text}")
        logging.info("Response from Elasticsearch: %s", response.json())
    except Exception as e:
        logging.error(f"Exception occurred while sending data to Elasticsearch: {e}")

# Function to read and process output from the telemetry script
def process_output(proc):
    while True:
        line = proc.stdout.readline()
        if not line:
            break
        action_meta_data = json.dumps({"index": {"_index": "telemetry_1"}})
        data = f"{action_meta_data}\n{line}"
        send_to_elasticsearch(data.encode('utf-8'))

# Main execution
if __name__ == "__main__":
    logging.info(f"Executing Python script: {script_path}")
    with subprocess.Popen(['python', script_path], stdout=subprocess.PIPE, text=True, bufsize=1, universal_newlines=True) as proc:
        process_thread = threading.Thread(target=process_output, args=(proc,))
        process_thread.start()
        process_thread.join()
    logging.info("Finished processing telemetry data.")

