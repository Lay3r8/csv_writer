# csv_writer
Useful to write stuff to a CSV file while another process is working

## Usage example
```python
from cgi import parse_header, parse_multipart
from os import getenv
from http.server import BaseHTTPRequestHandler, HTTPServer
from json.decoder import JSONDecodeError
from json import dumps as json_dumps, loads as json_loads
from urllib.parse import unquote_plus
from typing import Final
from queue import Queue
from http.server import BaseHTTPRequestHandler, HTTPServer
import paho.mqtt.publish as publish
from csv_writer import CsvWriterThread

VERSION: Final = getenv("VERSION", "0.0.0")
HTTP_PORT: Final = int(getenv("HTTP_PORT", 8080))
MQTT_HOST: Final = getenv("MQTT_HOST")
IOT_HUB_CONNECTION_STRING: Final = getenv("IOT_HUB_CONNECTION_STRING")
CSV_WRITER_PATH: Final = getenv("CSV_WRITER_PATH")

csv_writer_queue = Queue(maxsize=200)
csv_writer = CsvWriterThread(
    base_path=CSV_WRITER_PATH,
    headers=["timestamp_local", "license_plate", "direction"],
    fifo_queue=csv_writer_queue,
    max_files=5
)
csv_writer.start()

class Webhook(BaseHTTPRequestHandler):

    @staticmethod
    def process_json_data(json_data: dict):
        best_result = get_best_license_plate_result(json_data.get("data", {}))
        stdio_logger.info("Sending MQTT message")
        publish.single("PARKING", json_dumps(json_data, sort_keys=True), hostname=MQTT_HOST)
        # Webhook.send_iot_hub_message(json_data)
        csv_writer_queue.put_nowait([
            best_result.get("timestamp_local", "ERROR"),
            best_result.get("plate_number", "ERROR"),
            best_result.get("orientation", "ERROR"),
        ])

    def do_POST(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        ctype, pdict = parse_header(self.headers["Content-Type"])

        if ctype == "multipart/form-data":
            pdict["boundary"] = bytes(pdict["boundary"], "utf-8")
            fields = parse_multipart(self.rfile, pdict)
            # Get webhook content
            json_data = json_loads(fields.get("json")[0])
            self.process_json_data(json_data)

        elif ctype == "application/x-www-form-urlencoded":
            raw_data = self.get_raw_data()
            # print(f"raw_data: {raw_data}")
            decoded = unquote_plus(raw_data)
            # print(f"decoded: {decoded}")
            if decoded.startswith("json="):
                decoded = decoded[5:]
            json_data = json_loads(decoded)
            self.process_json_data(json_data)

        else:
            raw_data = self.get_raw_data()
            if raw_data.startswith("json="):
                raw_data = raw_data[5:]
            try:
                json_data = json_loads(raw_data)
            except JSONDecodeError:
                file_logger.exception("Something happened while trying to decode raw data")
            else:
                self.process_json_data(json_data)
        self.wfile.write(b"OK")

    def get_raw_data(self):
        return self.rfile.read(int(self.headers["content-length"])).decode("utf-8")


if __name__ == "__main__":
    server = HTTPServer(("", HTTP_PORT), Webhook)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        csv_writer.stop()
        csv_writer.join()
        server.server_close()
        print("Server stopped")
```
