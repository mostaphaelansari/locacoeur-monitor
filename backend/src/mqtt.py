import json
import logging
import ssl
import threading
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from collections import deque
import paho.mqtt.client as mqtt
import psycopg2
from psycopg2 import pool
from decouple import config
from cachetools import TTLCache
import smtplib
from email.mime.text import MIMEText

# Constants
TOPIC_PREFIX = "LC1"
MQTT_TOPICS = [
    f"{TOPIC_PREFIX}/+/event/#",
    f"{TOPIC_PREFIX}/+/command/#",
    f"{TOPIC_PREFIX}/+/result"
]
RESULT_CODES = {
    0: "Success",
    -1: "Missing opId",
    -2: "Null object reference",
    -3: "Audio file not found"
}
ALERT_CODES = {
    1: "Device moving",
    2: "Device removed",
    3: "Device connected to power",
    4: "Rapid temperature change (fan/heater active)",
    5: "Power cut",
    6: "Critical temperature",
    7: "Defibrillator fault",
    8: "Thermal regulation fault",
    9: "Periodic test failure"
}
VALID_LED_STATUSES = {"Green", "Red", "Off"}
VALID_AUDIO_MESSAGES = ["message_1", "message_2"]
VALID_POWER_SOURCES = ["ac", "battery"]
VALID_CONNECTIONS = ["lte", "wifi"]
VALID_DEFIBRILLATOR_CODES = [0, -1, -2, -3, -4, -5, -6, -7, -8, -9]

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("mqtt_client.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Timestamp debug logging
timestamp_logger = logging.getLogger("timestamp")
timestamp_handler = logging.FileHandler("timestamp_debug.log")
timestamp_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
timestamp_logger.addHandler(timestamp_handler)
timestamp_logger.setLevel(logging.DEBUG)

class MQTTService:
    def __init__(self):
        self.client = mqtt.Client(client_id="python_mqtt_service_" + str(uuid.uuid4()))
        self.client.tls_set(
            ca_certs=config("CA_CERT_PATH"),
            certfile=config("CLIENT_CERT_PATH"),
            keyfile=config("CLIENT_KEY_PATH"),
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLSv1_2
        )
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_log = self.on_log
        self.command_cache = {}
        self.command_lock = threading.Lock()
        self.processed_messages = deque(maxlen=1000)
        self.email_cache = TTLCache(maxsize=100, ttl=3600)
        self.config_cache = TTLCache(maxsize=100, ttl=3600)  # Cache configs for 1 hour
        self.connection_event = threading.Event()
        self.connection_event.set()  # Assume connected initially
        self.db_pool = None
        self.running = True
        self.setup_db_pool()
        self.start_followup_checker()

    def setup_db_pool(self) -> None:
        """Initialize the database connection pool."""
        try:
            self.db_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=20,
                dbname=config("DB_NAME"),
                user=config("DB_USER"),
                password=config("DB_PASSWORD"),
                host=config("DB_HOST"),
                port=config("DB_PORT")
            )
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            self.send_alert_email(
                "Database Pool Error",
                f"Failed to initialize database connection pool: {e}",
                "critical"
            )

    def connect_db(self) -> Optional[psycopg2.extensions.connection]:
        """Get a database connection from the pool."""
        try:
            return self.db_pool.getconn()
        except Exception as e:
            logger.error(f"Failed to get database connection: {e}")
            return None

    def release_db(self, conn: psycopg2.extensions.connection) -> None:
        """Release a database connection back to the pool."""
        try:
            self.db_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Failed to release database connection: {e}")

    def on_connect(self, client: mqtt.Client, userdata: Any, flags: Dict[str, Any], rc: int) -> None:
        """Handle MQTT connection event."""
        if rc == 0:
            logger.info("Connected to MQTT broker")
            self.connection_event.set()
            for topic in MQTT_TOPICS:
                client.subscribe(topic, qos=0)
                logger.info(f"Subscribed to topic: {topic}")
        else:
            logger.error(f"Failed to connect to MQTT broker with code {rc}")
            self.connection_event.clear()
            self.send_alert_email(
                "MQTT Connection Failure",
                f"Failed to connect to MQTT broker with code {rc}",
                "critical"
            )

    def on_disconnect(self, client: mqtt.Client, userdata: Any, rc: int) -> None:
        """Handle MQTT disconnection event."""
        if rc != 0:
            logger.error(f"Unexpected disconnection from MQTT broker with code {rc}")
            self.connection_event.clear()
            self.send_alert_email(
                "MQTT Disconnection",
                f"Unexpected disconnection from MQTT broker with code {rc}",
                "critical"
            )

    def on_log(self, client: mqtt.Client, userdata: Any, level: int, buf: str) -> None:
        """Log MQTT client messages."""
        logger.debug(f"MQTT Log: {buf}")

    def parse_timestamp(self, timestamp: Any, device_serial: str, return_unix: bool = False) -> Optional[datetime]:
        """Parse timestamp from MQTT message."""
        try:
            if isinstance(timestamp, int):
                ts = datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc)
                timestamp_logger.debug(f"Parsed timestamp for {device_serial}: {timestamp} -> {ts}")
                return int(ts.timestamp() * 1000) if return_unix else ts
            elif isinstance(timestamp, str):
                ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                timestamp_logger.debug(f"Parsed ISO timestamp for {device_serial}: {timestamp} -> {ts}")
                return int(ts.timestamp() * 1000) if return_unix else ts
            else:
                timestamp_logger.error(f"Invalid timestamp format for {device_serial}: {timestamp}")
                return None
        except Exception as e:
            timestamp_logger.error(f"Failed to parse timestamp for {device_serial}: {timestamp}, error: {e}")
            return None

    def validate_result_payload(self, device_serial: str, topic: str, data: Dict[str, Any]) -> bool:
        """Validate result payload against API documentation."""
        if "opId" not in data:
            logger.error(f"Missing opId in result payload for {device_serial} on topic {topic}")
            self.send_alert_email(
                "Invalid Result Payload",
                f"Missing opId in result for {device_serial} on topic {topic}",
                "warning"
            )
            return False

        result_code = data.get("result")
        valid_result_codes = list(RESULT_CODES.keys())
        if not isinstance(result_code, int) or result_code not in valid_result_codes:
            logger.error(f"Invalid result code for {device_serial}: {result_code}. Valid codes: {valid_result_codes}")
            self.send_alert_email(
                "Invalid Result Code",
                f"Invalid result code {result_code} for {device_serial} on topic {topic}. Valid codes: {valid_result_codes}",
                "warning"
            )
            return False

        return True

    def validate_alert_payload(self, device_serial: str, topic: str, data: Dict[str, Any]) -> bool:
        """Validate alert payload against API documentation."""
        required_fields = ["timestamp", "id"]
        for field in required_fields:
            if field not in data:
                logger.error(f"Missing required field '{field}' in alert for {device_serial} on topic {topic}")
                self.send_alert_email(
                    "Invalid Alert Payload",
                    f"Missing required field '{field}' in alert for {device_serial} on topic {topic}",
                    "warning"
                )
                return False

        timestamp = data.get("timestamp")
        if not isinstance(timestamp, int) or timestamp < 0 or timestamp > 2**32-1:
            logger.error(f"Invalid timestamp in alert for {device_serial}: {timestamp}")
            self.send_alert_email(
                "Invalid Timestamp",
                f"Invalid timestamp in alert for {device_serial} on topic {topic}: {timestamp}",
                "warning"
            )
            return False

        alert_id = data.get("id")
        valid_alert_ids = list(ALERT_CODES.keys())
        if not isinstance(alert_id, int) or alert_id not in valid_alert_ids:
            logger.error(f"Invalid alert ID for {device_serial}: {alert_id}. Valid IDs: {valid_alert_ids}")
            self.send_alert_email(
                "Invalid Alert ID",
                f"Invalid alert ID {alert_id} for {device_serial} on topic {topic}. Valid IDs: {valid_alert_ids}",
                "warning"
            )
            return False

        alert_message = data.get("message")
        if alert_message and (not isinstance(alert_message, str) or len(alert_message) > 255):
            logger.error(f"Invalid alert message for {device_serial}: {alert_message}")
            self.send_alert_email(
                "Invalid Alert Message",
                f"Invalid or too long alert message for {device_serial} on topic {topic}: {alert_message}",
                "warning"
            )
            return False

        return True

    def validate_status_payload(self, device_serial: str, topic: str, data: Dict[str, Any]) -> bool:
        """Validate status payload against API documentation."""
        required_fields = ["timestamp", "defibrillator", "battery", "power_source"]
        for field in required_fields:
            if field not in data:
                logger.error(f"Missing required field '{field}' in status for {device_serial} on topic {topic}")
                self.send_alert_email(
                    "Invalid Status Payload",
                    f"Missing required field '{field}' in status for {device_serial} on topic {topic}",
                    "warning"
                )
                return False

        timestamp = data.get("timestamp")
        if not isinstance(timestamp, int) or timestamp < 0 or timestamp > 2**32-1:
            logger.error(f"Invalid timestamp for {device_serial}: {timestamp}")
            self.send_alert_email(
                "Invalid Timestamp",
                f"Invalid timestamp for {device_serial} on topic {topic}: {timestamp}",
                "warning"
            )
            return False

        defibrillator = data.get("defibrillator")
        if defibrillator not in VALID_DEFIBRILLATOR_CODES:
            logger.error(f"Invalid defibrillator code for {device_serial}: {defibrillator}")
            self.send_alert_email(
                "Invalid Status Data",
                f"Invalid defibrillator code for {device_serial} on topic {topic}: {defibrillator}",
                "warning"
            )
            return False

        battery = data.get("battery")
        if not isinstance(battery, int) or not (0 <= battery <= 100):
            logger.error(f"Invalid battery level for {device_serial}: {battery}")
            self.send_alert_email(
                "Invalid Status Data",
                f"Invalid battery level for {device_serial} on topic {topic}: {battery}",
                "warning"
            )
            return False

        power_source = data.get("power_source")
        if power_source not in VALID_POWER_SOURCES:
            logger.error(f"Invalid power_source for {device_serial}: {power_source}")
            self.send_alert_email(
                "Invalid Status Data",
                f"Invalid power_source for {device_serial} on topic {topic}: {power_source}",
                "warning"
            )
            return False

        connection = data.get("connection")
        if connection is not None and connection not in VALID_CONNECTIONS:
            logger.error(f"Invalid connection type for {device_serial}: {connection}")
            self.send_alert_email(
                "Invalid Status Data",
                f"Invalid connection type for {device_serial} on topic {topic}: {connection}",
                "warning"
            )
            return False

        location_data = data.get("location")
        if location_data is not None:
            if not isinstance(location_data, dict):
                logger.error(f"Invalid location data type for {device_serial}: {type(location_data)}")
                self.send_alert_email(
                    "Invalid Status Data",
                    f"Invalid location data type for {device_serial} on topic {topic}: {type(location_data)}",
                    "warning"
                )
                return False

            lat = location_data.get("latitude")
            lon = location_data.get("longitude")
            if lat is not None or lon is not None:
                if (not isinstance(lat, (int, float)) or
                    not isinstance(lon, (int, float)) or
                    not (-90 <= lat <= 90) or
                    not (-180 <= lon <= 180)):
                    logger.error(f"Invalid location coordinates for {device_serial}: lat={lat}, lon={lon}")
                    self.send_alert_email(
                        "Invalid Status Data",
                        f"Invalid location coordinates for {device_serial} on topic {topic}: lat={lat}, lon={lon}",
                        "warning"
                    )
                    return False

        return True

    def process_device_status(self, device_serial: str, data: Dict[str, Any]) -> Dict[str, str]:
        """Process device status and extract LED states according to API documentation."""
        led_states = {
            "led_power": None,
            "led_defibrillator": None,
            "led_monitoring": None,
            "led_assistance": None,
            "led_mqtt": None,
            "led_environmental": None
        }

        # LED 1 - Power Status
        power_source = data.get("power_source")
        if power_source == "ac":
            led_states["led_power"] = "Green"
        elif power_source == "battery":
            led_states["led_power"] = "Red"

        # LED 2 - Defibrillator Status
        defibrillator = data.get("defibrillator")
        if defibrillator == 0:
            led_states["led_defibrillator"] = "Green"
        elif defibrillator in VALID_DEFIBRILLATOR_CODES:
            led_states["led_defibrillator"] = "Red"

        # LED 5 - MQTT Connectivity
        connection = data.get("connection")
        if connection in VALID_CONNECTIONS:
            led_states["led_mqtt"] = "Green"
        else:
            led_states["led_mqtt"] = "Red"

        # LED 3 & 4 - Monitoring/Assistance (from config cache or database)
        cache_key = f"{device_serial}_config"
        if cache_key in self.config_cache:
            monitoring, assistance = self.config_cache[cache_key]
            led_states["led_monitoring"] = "Green" if monitoring == "true" else "Red"
            led_states["led_assistance"] = "Green" if assistance == "true" else "Red"
        else:
            conn = self.connect_db()
            if conn:
                try:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        SELECT payload->'config'->'services'->>'monitoring',
                               payload->'config'->'services'->>'assistance'
                        FROM Events
                        WHERE device_serial = %s AND topic LIKE %s
                        ORDER BY event_timestamp DESC LIMIT 1
                        """,
                        (device_serial[:50], f"{TOPIC_PREFIX}/{device_serial}/event/config")
                    )
                    result = cur.fetchone()
                    if result:
                        monitoring, assistance = result
                        led_states["led_monitoring"] = "Green" if monitoring == "true" else "Red"
                        led_states["led_assistance"] = "Green" if assistance == "true" else "Red"
                        self.config_cache[cache_key] = (monitoring, assistance)
                except Exception as e:
                    logger.error(f"Failed to fetch config for {device_serial}: {e}")
                finally:
                    cur.close()
                    self.release_db(conn)

        # LED 6 - Environmental Control (default to Off, updated by alert events)
        led_states["led_environmental"] = "Off"

        return led_states

    def on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        """Handle incoming MQTT messages."""
        try:
            topic = msg.topic
            payload = msg.payload.decode("utf-8")
            data = json.loads(payload)
            device_serial = topic.split("/")[1]

            # Deduplicate messages
            message_hash = hashlib.md5((topic + payload).encode()).hexdigest()
            if message_hash in self.processed_messages:
                logger.debug(f"Duplicate message ignored for {device_serial} on topic {topic}")
                return
            self.processed_messages.append(message_hash)

            logger.debug(f"Received message for {device_serial} on topic {topic}: {payload}")

            if topic.endswith("/result"):
                self.insert_result(device_serial, topic, data)
            else:
                self.insert_device_data(device_serial, topic, data)

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in message for topic {msg.topic}: {e}")
            self.send_alert_email(
                "Invalid JSON",
                f"Invalid JSON in message for topic {msg.topic}: {e}",
                "warning"
            )
        except Exception as e:
            logger.error(f"Error processing message for topic {msg.topic}: {e}")
            self.send_alert_email(
                "Message Processing Error",
                f"Error processing message for topic {msg.topic}: {e}",
                "critical"
            )

    def insert_result(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Insert result data into the Results table with improved validation."""
        if not self.validate_result_payload(device_serial, topic, data):
            return

        conn = self.connect_db()
        if not conn:
            return

        try:
            cur = conn.cursor()
            payload_str = json.dumps(data, sort_keys=True)
            result_code = data.get("result")
            result_message = data.get("message", "")
            timestamp = self.parse_timestamp(data.get("timestamp"), device_serial)
            if timestamp is None:
                logger.error(f"Invalid timestamp for device {device_serial} on topic {topic}")
                self.send_alert_email(
                    "Invalid Timestamp",
                    f"Invalid timestamp for device {device_serial} on topic {topic}: {data.get('timestamp')}",
                    "warning"
                )
                return

            cur.execute(
                """
                INSERT INTO Devices (serial, mqtt_broker_url)
                VALUES (%s, %s)
                ON CONFLICT (serial) DO NOTHING
                """,
                (device_serial[:50], "mqtts://mqtt.locacoeur.com:8883")
            )

            command_id = None
            cur.execute(
                """
                SELECT command_id FROM Commands
                WHERE device_serial = %s AND payload->>'opId' = %s
                ORDER BY created_at DESC LIMIT 1
                """,
                (device_serial[:50], data["opId"])
            )
            result = cur.fetchone()
            command_id = result[0] if result else None

            cur.execute(
                """
                INSERT INTO Results (
                    command_id, device_serial, topic, result_status, result_message,
                    payload, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    command_id,
                    device_serial[:50],
                    topic[:255],
                    str(result_code),
                    result_message[:255],
                    payload_str,
                    datetime.now()
                )
            )
            conn.commit()
            logger.info(f"Inserted result for device {device_serial} on topic {topic}")

            with self.command_lock:
                if (device_serial, data["opId"]) in self.command_cache:
                    logger.debug(f"Removed command opId {data['opId']} for {device_serial} from cache after receiving result")
                    del self.command_cache[(device_serial, data["opId"])]

            if command_id and ("update" in topic.lower() or (result_message and "update" in result_message.lower())):
                result_desc = RESULT_CODES.get(result_code, f"Unknown result code: {result_code}")
                logger.info(f"Firmware update result for {device_serial}: status={result_desc}, message={result_message}")
                if result_code != 0:
                    self.send_alert_email(
                        "Firmware Update Result",
                        f"Firmware update for {device_serial} failed: {result_desc} - {result_message}",
                        "critical"
                    )
        except Exception as e:
            logger.error(f"Database error for device {device_serial}: {e}")
            conn.rollback()
            self.send_alert_email(
                "Database Error",
                f"Failed to insert result for device {device_serial} on topic {topic}: {e}",
                "critical"
            )
        finally:
            cur.close()
            self.release_db(conn)

    def insert_device_data(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Insert device data into the device_data table with improved validation and LED inference."""
        conn = self.connect_db()
        if not conn:
            logger.error("Failed to connect to database")
            return

        try:
            cur = conn.cursor()
            payload_str = json.dumps(data, sort_keys=True)
            timestamp = self.parse_timestamp(data.get("timestamp"), device_serial, return_unix=True)

            if timestamp is None:
                logger.error(f"Invalid timestamp for device {device_serial} on topic {topic}")
                self.send_alert_email(
                    "Invalid Timestamp",
                    f"Invalid timestamp for device {device_serial} on topic {topic}: {data.get('timestamp')}",
                    "warning"
                )
                return

            # Insert or update device record
            try:
                cur.execute(
                    """
                    INSERT INTO Devices (serial, mqtt_broker_url)
                    VALUES (%s, %s)
                    ON CONFLICT (serial) DO NOTHING
                    """,
                    (device_serial[:50], "mqtts://mqtt.locacoeur.com:8883")
                )
            except psycopg2.errors.UniqueViolation:
                logger.debug(f"Duplicate device record skipped for {device_serial}")
                conn.rollback()
                cur = conn.cursor()
            except Exception as e:
                logger.error(f"Error inserting into Devices for {device_serial}: {e}")
                conn.rollback()
                self.send_alert_email(
                    "Database Error",
                    f"Failed to insert into Devices for {device_serial}: {e}",
                    "critical"
                )
                return

            # Handle location events
            if topic.endswith("/event/location"):
                if not self.validate_status_payload(device_serial, topic, data):
                    return
                latitude = data.get("latitude")
                longitude = data.get("longitude")
                logger.debug(f"Processing location event for {device_serial}: latitude={latitude}, longitude={longitude}")

                if not isinstance(latitude, (int, float)) or not isinstance(longitude, (int, float)):
                    logger.error(f"Invalid location data types for device {device_serial}: latitude={latitude}, longitude={longitude}")
                    self.send_alert_email(
                        "Invalid Location Data",
                        f"Invalid location data types for device {device_serial}: latitude={latitude}, longitude={longitude}",
                        "warning"
                    )
                    return

                if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
                    logger.error(f"Invalid location data for device {device_serial}: latitude={latitude}, longitude={longitude}")
                    self.send_alert_email(
                        "Invalid Location Data",
                        f"Invalid location for device {device_serial}: latitude={latitude}, longitude={longitude}",
                        "warning"
                    )
                    return

                cur.execute(
                    """
                    INSERT INTO device_data (
                        device_serial, topic, latitude, longitude, timestamp,
                        original_timestamp, payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        device_serial[:50],
                        topic[:255],
                        float(latitude),
                        float(longitude),
                        timestamp,
                        timestamp,
                        payload_str
                    )
                )

            # Handle status events
            elif topic.endswith("/event/status"):
                if not self.validate_status_payload(device_serial, topic, data):
                    return

                battery = data.get("battery")
                connection = data.get("connection")
                defibrillator = data.get("defibrillator")
                power_source = data.get("power_source")

                # Process LED states
                led_states = self.process_device_status(device_serial, data)

                # Handle nested location data
                latitude = None
                longitude = None
                location_data = data.get("location")
                if location_data and isinstance(location_data, dict):
                    latitude = location_data.get("latitude")
                    longitude = data.get("longitude")

                # Insert status data
                cur.execute(
                    """
                    INSERT INTO device_data (
                        device_serial, topic, battery, connection, defibrillator, latitude, longitude,
                        power_source, led_power, led_defibrillator, led_monitoring, led_assistance,
                        led_mqtt, led_environmental, timestamp, original_timestamp, payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        device_serial[:50],
                        topic[:255],
                        battery,
                        connection[:50] if connection else None,
                        defibrillator,
                        float(latitude) if latitude is not None else None,
                        float(longitude) if longitude is not None else None,
                        power_source[:50] if power_source else None,
                        led_states["led_power"],
                        led_states["led_defibrillator"],
                        led_states["led_monitoring"],
                        led_states["led_assistance"],
                        led_states["led_mqtt"],
                        led_states["led_environmental"],
                        timestamp,
                        timestamp,
                        payload_str
                    )
                )

                # Update LEDs table
                for led_type, status in led_states.items():
                    if status is not None and status in VALID_LED_STATUSES:
                        logger.debug(f"Updating LED: device={device_serial}, type={led_type}, status={status}")
                        cur.execute(
                            """
                            INSERT INTO LEDs (device_serial, led_type, status, description, last_updated)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (device_serial, led_type)
                            DO UPDATE SET status = EXCLUDED.status, description = EXCLUDED.description, last_updated = EXCLUDED.last_updated
                            """,
                            (device_serial[:50], led_type[4:], status, None, datetime.now())
                        )
                        logger.info(f"Updated LED for device {device_serial}: type={led_type[4:]}, status={status}")

                # Send alerts for critical conditions
                if battery is not None and battery < 20:
                    self.send_alert_email(
                        "Low Battery",
                        f"Low battery for device {device_serial}: {battery}%",
                        "critical"
                    )
                if led_states["led_power"] == "Red":
                    self.send_alert_email(
                        "Power Supply Issue",
                        f"Power supply issue for device {device_serial}: led_power=Red",
                        "critical"
                    )
                if led_states["led_defibrillator"] == "Red":
                    self.send_alert_email(
                        "Defibrillator Fault",
                        f"Defibrillator fault for device {device_serial}: led_defibrillator=Red",
                        "critical"
                    )
                if led_states["led_mqtt"] == "Red":
                    self.send_alert_email(
                        "Connection Lost",
                        f"Device connection lost for {device_serial}: led_mqtt=Red",
                        "critical"
                    )

            # Handle alert events
            elif topic.endswith("/event/alert"):
                if not self.validate_alert_payload(device_serial, topic, data):
                    return

                alert_id = data.get("id")
                alert_message = data.get("message")
                logger.debug(f"Processing alert event for {device_serial}: alert_id={alert_id}, alert_message={alert_message}")

                # Update Environmental LED based on alert_id
                led_environmental = "Off"
                if alert_id in [6, 8]:  # Critical temperature or thermal regulation fault
                    led_environmental = "Red"
                elif alert_id == 4:  # Rapid temperature change
                    led_environmental = "Green"

                cur.execute(
                    """
                    INSERT INTO LEDs (device_serial, led_type, status, description, last_updated)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (device_serial, led_type)
                    DO UPDATE SET status = EXCLUDED.status, description = EXCLUDED.description, last_updated = EXCLUDED.last_updated
                    """,
                    (device_serial[:50], "Environmental", led_environmental, None, datetime.now())
                )

                cur.execute(
                    """
                    INSERT INTO device_data (
                        device_serial, topic, alert_id, alert_message, timestamp,
                        original_timestamp, payload, led_environmental
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        device_serial[:50],
                        topic[:255],
                        alert_id,
                        alert_message[:255] if alert_message else None,
                        timestamp,
                        timestamp,
                        payload_str,
                        led_environmental
                    )
                )

                alert_desc = ALERT_CODES.get(alert_id, f"Unknown alert ID: {alert_id}")
                self.send_alert_email(
                    f"Critical Alert - Device {device_serial}",
                    f"Alert for device {device_serial}: {alert_desc} (ID: {alert_id})",
                    "critical"
                )

            # Handle config events (update cache)
            elif topic.endswith("/event/config"):
                services = data.get("config", {}).get("services", {})
                monitoring = services.get("monitoring")
                assistance = services.get("assistance")
                if monitoring is not None and assistance is not None:
                    self.config_cache[f"{device_serial}_config"] = (str(monitoring).lower(), str(assistance).lower())

                cur.execute(
                    """
                    INSERT INTO device_data (
                        device_serial, topic, timestamp, original_timestamp, payload
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        device_serial[:50],
                        topic[:255],
                        timestamp,
                        timestamp,
                        payload_str
                    )
                )

            # Handle other event types (version, log)
            else:
                logger.debug(f"Processing generic event for {device_serial} on topic {topic}")
                cur.execute(
                    """
                    INSERT INTO device_data (
                        device_serial, topic, timestamp, original_timestamp, payload
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        device_serial[:50],
                        topic[:255],
                        timestamp,
                        timestamp,
                        payload_str
                    )
                )

            conn.commit()
            logger.info(f"Inserted into device_data for device {device_serial} on topic {topic}")

        except Exception as e:
            logger.error(f"Database error for device {device_serial}: {e}")
            conn.rollback()
            self.send_alert_email(
                "Database Error",
                f"Failed to insert device data for device {device_serial} on topic {topic}: {e}",
                "critical"
            )
        finally:
            cur.close()
            self.release_db(conn)

    def send_alert_email(self, subject: str, message: str, alert_type: str) -> None:
        """Send an email alert with rate limiting."""
        cache_key = f"{subject}_{message}"
        if cache_key in self.email_cache:
            logger.debug(f"Email alert suppressed due to rate limiting: {subject}")
            return

        try:
            msg = MIMEText(message)
            msg["Subject"] = f"[{alert_type.upper()}] {subject}"
            msg["From"] = config("EMAIL_FROM")
            msg["To"] = config("EMAIL_TO")

            with smtplib.SMTP(config("SMTP_HOST"), config("SMTP_PORT")) as server:
                server.starttls()
                server.login(config("SMTP_USER"), config("SMTP_PASSWORD"))
                server.send_message(msg)

            logger.info(f"Sent email alert: {subject}")
            self.email_cache[cache_key] = True
        except Exception as e:
            logger.error(f"Failed to send email alert: {subject}, error: {e}")

    def request_config(self, device_serial: str) -> None:
        """Request the current device configuration using /get subtopic."""
        try:
            op_id = str(uuid.uuid4())
            topic = f"{TOPIC_PREFIX}/{device_serial}/command/config/get"
            payload = {"opId": op_id}
            self.client.publish(topic, json.dumps(payload), qos=0)
            logger.info(f"Sent config request to {topic} with opId {op_id}")

            with self.command_lock:
                self.command_cache[(device_serial, op_id)] = (topic, datetime.now(timezone.utc))
        except Exception as e:
            logger.error(f"Failed to send config request for {device_serial}: {e}")
            self.send_alert_email(
                "Config Request Failed",
                f"Failed to send config request for device {device_serial}: {e}",
                "warning"
            )

    def set_config(self, device_serial: str, config_data: Dict[str, Any]) -> None:
        """Set device configuration."""
        try:
            op_id = str(uuid.uuid4())
            topic = f"{TOPIC_PREFIX}/{device_serial}/command/config"
            payload = {"opId": op_id, "config": config_data}
            self.client.publish(topic, json.dumps(payload), qos=0)
            logger.info(f"Sent set config command to {topic} with opId {op_id}")

            with self.command_lock:
                self.command_cache[(device_serial, op_id)] = (topic, datetime.now(timezone.utc))
        except Exception as e:
            logger.error(f"Failed to send set config command for {device_serial}: {e}")
            self.send_alert_email(
                "Set Config Command Failed",
                f"Failed to send set config command for device {device_serial}: {e}",
                "warning"
            )

    def request_firmware_version(self, device_serial: str) -> None:
        """Request firmware version using /get subtopic."""
        try:
            op_id = str(uuid.uuid4())
            topic = f"{TOPIC_PREFIX}/{device_serial}/command/version/get"
            payload = {"opId": op_id}
            self.client.publish(topic, json.dumps(payload), qos=0)
            logger.info(f"Sent firmware version request to {topic} with opId {op_id}")

            with self.command_lock:
                self.command_cache[(device_serial, op_id)] = (topic, datetime.now(timezone.utc))
        except Exception as e:
            logger.error(f"Failed to send version request for {device_serial}: {e}")
            self.send_alert_email(
                "Version Request Failed",
                f"Failed to send firmware version request for device {device_serial}: {e}",
                "warning"
            )

    def update_firmware(self, device_serial: str, version: str) -> None:
        """Update device firmware."""
        try:
            op_id = str(uuid.uuid4())
            topic = f"{TOPIC_PREFIX}/{device_serial}/command/update"
            payload = {"opId": op_id, "version": version}
            self.client.publish(topic, json.dumps(payload), qos=0)
            logger.info(f"Sent firmware update command to {topic} with opId {op_id}")

            with self.command_lock:
                self.command_cache[(device_serial, op_id)] = (topic, datetime.now(timezone.utc))
        except Exception as e:
            logger.error(f"Failed to send firmware update command for {device_serial}: {e}")
            self.send_alert_email(
                "Firmware Update Command Failed",
                f"Failed to send firmware update command for device {device_serial}: {e}",
                "warning"
            )

    def request_location(self, device_serial: str) -> None:
        """Request the current device location (main topic equals /get subtopic)."""
        try:
            op_id = str(uuid.uuid4())
            topic = f"{TOPIC_PREFIX}/{device_serial}/command/location"
            payload = {"opId": op_id}
            self.client.publish(topic, json.dumps(payload), qos=0)
            logger.info(f"Sent location request to {topic} with opId {op_id}")

            with self.command_lock:
                self.command_cache[(device_serial, op_id)] = (topic, datetime.now(timezone.utc))
        except Exception as e:
            logger.error(f"Failed to send location request for {device_serial}: {e}")
            self.send_alert_email(
                "Location Request Failed",
                f"Failed to send location request for device {device_serial}: {e}",
                "warning"
            )

    def play_audio(self, device_serial: str, audio_message: str) -> None:
        """Play audio message on device."""
        if audio_message not in VALID_AUDIO_MESSAGES:
            logger.error(f"Invalid audio message for {device_serial}: {audio_message}")
            self.send_alert_email(
                "Invalid Audio Message",
                f"Invalid audio message for {device_serial}: {audio_message}. Valid messages: {VALID_AUDIO_MESSAGES}",
                "warning"
            )
            return

        try:
            op_id = str(uuid.uuid4())
            topic = f"{TOPIC_PREFIX}/{device_serial}/command/play"
            payload = {"opId": op_id, "audioMessage": audio_message}
            self.client.publish(topic, json.dumps(payload), qos=0)
            logger.info(f"Sent play audio command to {topic} with opId {op_id}")

            with self.command_lock:
                self.command_cache[(device_serial, op_id)] = (topic, datetime.now(timezone.utc))
        except Exception as e:
            logger.error(f"Failed to send play audio command for {device_serial}: {e}")
            self.send_alert_email(
                "Play Audio Command Failed",
                f"Failed to send play audio command for device {device_serial}: {e}",
                "warning"
            )

    def request_log(self, device_serial: str) -> None:
        """Request device logs using /get subtopic."""
        try:
            op_id = str(uuid.uuid4())
            topic = f"{TOPIC_PREFIX}/{device_serial}/command/log/get"
            payload = {"opId": op_id}
            self.client.publish(topic, json.dumps(payload), qos=0)
            logger.info(f"Sent log request to {topic} with opId {op_id}")

            with self.command_lock:
                self.command_cache[(device_serial, op_id)] = (topic, datetime.now(timezone.utc))
        except Exception as e:
            logger.error(f"Failed to send log request for {device_serial}: {e}")
            self.send_alert_email(
                "Log Request Failed",
                f"Failed to send log request for device {device_serial}: {e}",
                "warning"
            )

    def check_command_timeouts(self) -> None:
        """Check for commands that have timed out (5 minutes)."""
        while self.running:
            try:
                with self.command_lock:
                    now = datetime.now(timezone.utc)
                    timeout_threshold = 300  # 5 minutes
                    timed_out = [
                        (serial, op_id) for (serial, op_id), (topic, sent_time) in self.command_cache.items()
                        if (now - sent_time).total_seconds() > timeout_threshold
                    ]
                    for serial, op_id in timed_out:
                        topic, _ = self.command_cache[(serial, op_id)]
                        logger.error(f"Command timeout for {serial} with opId {op_id} on topic {topic}")
                        self.send_alert_email(
                            "Command Timeout",
                            f"Command timeout for device {serial} with opId {op_id} on topic {topic}",
                            "critical"
                        )
                        del self.command_cache[(serial, op_id)]
                time.sleep(60)
            except Exception as e:
                logger.error(f"Error in command timeout checker: {e}")
                time.sleep(60)

    def start_followup_checker(self) -> None:
        """Start the command timeout checker thread."""
        threading.Thread(target=self.check_command_timeouts, daemon=True).start()
        logger.info("Started command timeout checker thread")

    def start(self) -> None:
        """Start the MQTT client and connect to the broker."""
        try:
            self.client.connect("mqtt.locacoeur.com", 8883, keepalive=60)
            self.client.loop_start()
            logger.info("MQTT client started")
        except Exception as e:
            logger.error(f"Failed to start MQTT client: {e}")
            self.send_alert_email(
                "MQTT Client Start Failure",
                f"Failed to start MQTT client: {e}",
                "critical"
            )

    def stop(self) -> None:
        """Stop the MQTT client and cleanup."""
        self.running = False
        self.client.loop_stop()
        self.client.disconnect()
        logger.info("MQTT client stopped")
        if self.db_pool:
            self.db_pool.closeall()
            logger.info("Database connection pool closed")

if __name__ == "__main__":
    import signal
    import sys

    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        mqtt_service.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    mqtt_service = MQTTService()
    mqtt_service.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        mqtt_service.stop()
