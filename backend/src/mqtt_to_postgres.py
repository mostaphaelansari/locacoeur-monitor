import sys
import time
import uuid
import ssl
import signal
import json
import hashlib
import logging
import threading
import smtplib
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate
from collections import deque

import paho.mqtt.client as mqtt
import psycopg2
from psycopg2 import errors
from psycopg2 import pool
from decouple import config
from cachetools import TTLCache

# Constants
TOPIC_PREFIX = "LC1"
MQTT_TOPICS = [
    (f"{TOPIC_PREFIX}/+/event/#", 0),     # QoS 0 for events
    (f"{TOPIC_PREFIX}/+/command/#", 1),   # QoS 1 for commands (more reliable)
    (f"{TOPIC_PREFIX}/+/result", 1)       # QoS 1 for results (more reliable)
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mqtt_client.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

timestamp_logger = logging.getLogger('timestamp_debug')
timestamp_handler = logging.FileHandler('timestamp_debug.log')
timestamp_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
timestamp_logger.addHandler(timestamp_handler)
timestamp_logger.setLevel(logging.DEBUG)

# MQTT settings
MQTT_BROKER = config("MQTT_BROKER", default="mqtt.locacoeur.com")
MQTT_PORT = int(config("MQTT_PORT", default=8883))
MQTT_CLIENT_ID = f"locacoeur-client-{uuid.uuid4()}"
MQTT_CA_CERT = config("MQTT_CA_CERT", default="../certs/ca.crt")
MQTT_CLIENT_CERT = config("MQTT_CLIENT_CERT", default="../certs/client.crt")
MQTT_CLIENT_KEY = config("MQTT_CLIENT_KEY", default="../certs/client.key")
MQTT_USERNAME = config("MQTT_USERNAME", default="locacoeur")
MQTT_PASSWORD = config("MQTT_PASSWORD", default=None)

# Database settings
DB_CONFIG = {
    "dbname": config("DB_NAME", default="mqtt_db"),
    "user": config("DB_USER", default="mqtt_user"),
    "password": config("DB_PASSWORD"),
    "host": config("DB_HOST", default="91.134.90.10"),
    "port": config("DB_PORT", default="5432")
}

# Email settings
EMAIL_CONFIG = {
    "smtp_server": config("SMTP_SERVER", default="ssl0.ovh.net"),
    "smtp_port": int(config("SMTP_PORT", default=587)),
    "username": config("SMTP_USERNAME", default="support@locacoeur.com"),
    "password": config("SMTP_PASSWORD"),
    "from_email": config("SMTP_FROM_EMAIL", default="support@locacoeur.com"),
    "to_emails": config("SMTP_TO_EMAILS", default="alertHousse@proton.me").split(","),
    "enabled": config("SMTP_ENABLED", default=True, cast=bool)
}

class MQTTService:
    def __init__(self):
        self.running = True
        self.reconnect_count = 0
        self.last_connection_time = None
        self.email_cache = TTLCache(maxsize=100, ttl=3600)
        self.alert_cache = TTLCache(maxsize=100, ttl=60)
        self.config_cache = TTLCache(maxsize=100, ttl=3600)  # Cache configs for 1 hour
        self.db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **DB_CONFIG)
        self.command_lock = threading.Lock()
        self.command_cache = {}
        self.message_lock = threading.Lock()
        self.processed_messages = deque(maxlen=1000)
        self.client = None
        self.connection_established = threading.Event()
        self.setup_signal_handlers()
        self.initialize_db()
        self.setup_mqtt_client()
        self.start_followup_checker()

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        if self.client:
            try:
                self.client.disconnect()
                self.client.loop_stop()
            except Exception as e:
                logger.warning(f"Error during MQTT disconnection in signal handler: {e}")
        try:
            self.db_pool.closeall()
        except Exception as e:
            logger.warning(f"Error closing database pool: {e}")
        sys.exit(0)

    def connect_db(self) -> Optional[psycopg2.extensions.connection]:
        """Get a database connection from the pool"""
        try:
            return self.db_pool.getconn()
        except Exception as e:
            logger.error(f"Failed to get database connection: {e}")
            self.send_alert_email("Database Connection Failed", f"Failed to get database connection: {e}", "critical")
            return None

    def release_db(self, conn):
        """Release a database connection back to the pool"""
        try:
            self.db_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Failed to release database connection: {e}")

    def get_server_id(self) -> Optional[int]:
        """Retrieve or create server_id for the current server with better error handling"""
        conn = self.connect_db()
        if not conn:
            logger.error("Failed to connect to database for server_id retrieval")
            return None

        try:
            cur = conn.cursor()
            mqtt_url = "mqtts://mqtt.locacoeur.com:8883"

            # First try to get existing server_id
            cur.execute(
                """
                SELECT server_id FROM Servers
                WHERE environment = %s AND mqtt_url = %s
                """,
                ("production", mqtt_url)
            )
            result = cur.fetchone()
            if result:
                server_id = result[0]
                logger.debug(f"Found existing server_id: {server_id}")
                return server_id

            # Try to insert new server record
            try:
                cur.execute(
                    """
                    INSERT INTO Servers (environment, mqtt_url)
                    VALUES (%s, %s)
                    RETURNING server_id
                    """,
                    ("production", mqtt_url)
                )
                result = cur.fetchone()
                if result:
                    server_id = result[0]
                    conn.commit()
                    logger.info(f"Created new server_id: {server_id} for environment='production'")
                    return server_id
            except psycopg2.errors.UniqueViolation:
                # Another process might have inserted it
                conn.rollback()
                cur.execute(
                    """
                    SELECT server_id FROM Servers
                    WHERE environment = %s AND mqtt_url = %s
                    """,
                    ("production", mqtt_url)
                )
                result = cur.fetchone()
                if result:
                    server_id = result[0]
                    logger.info(f"Retrieved server_id after conflict: {server_id}")
                    return server_id

            # If we still don't have a server_id, something is wrong
            logger.error("Failed to retrieve or create server_id")
            self.send_alert_email(
                "Database Error",
                "Failed to retrieve or create server_id",
                "critical"
            )
            return None

        except Exception as e:
            logger.error(f"Error retrieving or creating server_id: {e}")
            try:
                conn.rollback()
            except:
                pass
            self.send_alert_email(
                "Database Error",
                f"Failed to retrieve or create server_id: {e}",
                "critical"
            )
            return None
        finally:
            try:
                cur.close()
            except:
                pass
            self.release_db(conn)

    def initialize_db(self):
        """Initialize database schema with corrected column names"""
        conn = self.connect_db()
        if not conn:
            return
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Servers (
                    server_id SERIAL PRIMARY KEY,
                    environment TEXT NOT NULL,
                    mqtt_url TEXT NOT NULL,
                    UNIQUE (environment)
                );
                CREATE TABLE IF NOT EXISTS Devices (
                    serial VARCHAR(50) PRIMARY KEY,
                    mqtt_broker_url VARCHAR(255),
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS Commands (
                    command_id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
                    server_id INTEGER REFERENCES Servers(server_id) ON DELETE RESTRICT,
                    operation_id VARCHAR(50),
                    topic VARCHAR(255),
                    message_id VARCHAR(50),
                    payload JSONB,
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    is_get BOOLEAN,
                    UNIQUE (device_serial, operation_id)
                );
                CREATE TABLE IF NOT EXISTS Results (
                    result_id SERIAL PRIMARY KEY,
                    command_id INTEGER REFERENCES Commands(command_id) ON DELETE SET NULL,
                    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
                    topic VARCHAR(255),
                    result_status VARCHAR(50),
                    result_message TEXT,
                    payload JSONB,
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS Events (
                    event_id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
                    server_id INTEGER REFERENCES Servers(server_id) ON DELETE RESTRICT,
                    operation_id VARCHAR(50),
                    topic VARCHAR(255),
                    message_id VARCHAR(50),
                    payload JSONB,
                    event_timestamp TIMESTAMP WITHOUT TIME ZONE,
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    original_timestamp TEXT,
                    UNIQUE (device_serial, topic, event_timestamp)
                );
                CREATE TABLE IF NOT EXISTS LEDs (
                    led_id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
                    led_type VARCHAR(50) NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    description TEXT,
                    last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT leds_status_check CHECK (status IN ('Green', 'Red', 'Off')),
                    UNIQUE (device_serial, led_type)
                );
                CREATE TABLE IF NOT EXISTS device_data (
                    id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
                    topic VARCHAR(255),
                    battery INTEGER CHECK (battery >= 0 AND battery <= 100),
                    connection VARCHAR(50),
                    defibrillator INTEGER CHECK (defibrillator >= -9 AND defibrillator <= 0),
                    latitude DOUBLE PRECISION CHECK (latitude BETWEEN -90 AND 90),
                    longitude DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),
                    power_source VARCHAR(50),
                    timestamp BIGINT,
                    led_power VARCHAR(50) CHECK (led_power IN ('Green', 'Red')),
                    led_defibrillator VARCHAR(50) CHECK (led_defibrillator IN ('Green', 'Red')),
                    led_monitoring VARCHAR(50) CHECK (led_monitoring IN ('Green', 'Red')),
                    led_assistance VARCHAR(50) CHECK (led_assistance IN ('Green', 'Red')),
                    led_mqtt VARCHAR(50) CHECK (led_mqtt IN ('Green', 'Red')),
                    led_environmental VARCHAR(50) CHECK (led_environmental IN ('Green', 'Red', 'Off')),
                    payload JSONB,
                    alert_id INTEGER,
                    alert_message VARCHAR(255),
                    original_timestamp BIGINT,
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS device_data_backup (
                    LIKE device_data INCLUDING ALL,
                    backup_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_device_data_backup_serial ON device_data_backup(device_serial);
                CREATE INDEX IF NOT EXISTS idx_device_data_backup_timestamp ON device_data_backup(timestamp);
                CREATE TABLE IF NOT EXISTS Locations (
                    id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
                    timestamp BIGINT,
                    latitude DOUBLE PRECISION CHECK (latitude BETWEEN -90 AND 90),
                    longitude DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),
                    UNIQUE (device_serial, timestamp)
                );
                CREATE TABLE IF NOT EXISTS Versions (
                    id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
                    timestamp BIGINT,
                    version VARCHAR(50),
                    UNIQUE (device_serial, timestamp)
                );
                CREATE TABLE IF NOT EXISTS Status (
                    id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
                    timestamp BIGINT,
                    state VARCHAR(50),
                    UNIQUE (device_serial, timestamp)
                );
                CREATE TABLE IF NOT EXISTS Alerts (
                    id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
                    timestamp BIGINT,
                    code VARCHAR(50),
                    UNIQUE (device_serial, timestamp, code)
                );
            """)
            conn.commit()
            logger.info("Database schema initialized")
        except Exception as e:
            logger.error(f"Error initializing database schema: {e}")
            conn.rollback()
        finally:
            cur.close()
            self.release_db(conn)

    def start_followup_checker(self):
        """Start thread to check for command timeouts"""
        def check_timeouts():
            while self.running:
                try:
                    with self.command_lock:
                        expired = []
                        current_time = datetime.now(timezone.utc)
                        for key, (topic, sent_time) in self.command_cache.items():
                            if (current_time - sent_time).total_seconds() > 300:  # 5 minutes
                                device_serial, op_id = key
                                logger.warning(f"Command timeout: No result for opId {op_id} on {device_serial}")
                                self.send_alert_email(
                                    "Command Timeout",
                                    f"No result received for command {op_id} on device {device_serial} after 5 minutes",
                                    "critical"
                                )
                                expired.append(key)
                        for key in expired:
                            del self.command_cache[key]
                except Exception as e:
                    logger.error(f"Error in command timeout checker: {e}")
                time.sleep(60)
        thread = threading.Thread(target=check_timeouts, daemon=True)
        thread.start()
        logger.info("Started command timeout checker thread")

    def backup_device_data(self):
        """Backup old device_data records and clean up"""
        conn = self.connect_db()
        if not conn:
            return
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO device_data_backup
                SELECT *, CURRENT_TIMESTAMP FROM device_data WHERE created_at < %s
            """, (datetime.now(timezone.utc) - timedelta(days=30),))
            cur.execute("DELETE FROM device_data WHERE created_at < %s",
                    (datetime.now(timezone.utc) - timedelta(days=30),))
            conn.commit()
            logger.info("Backed up and cleaned old device_data")
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            conn.rollback()
        finally:
            cur.close()
            self.release_db(conn)

    def send_alert_email(self, subject: str, message: str, priority: str = "normal"):
        """Send email alert with rate limiting"""
        if not EMAIL_CONFIG["enabled"]:
            logger.debug(f"Email alerts disabled. Would send: {subject}")
            return
        cache_key = f"{subject}:{priority}:{message[:50]}"
        if cache_key in self.email_cache:
            logger.debug(f"Email suppressed for {subject} (rate limit)")
            return
        self.email_cache[cache_key] = True
        max_retries = 3
        for attempt in range(max_retries):
            try:
                msg = MIMEMultipart()
                msg['From'] = EMAIL_CONFIG["from_email"]
                msg['To'] = ", ".join(EMAIL_CONFIG["to_emails"])
                msg['Date'] = formatdate(localtime=True)
                msg['Subject'] = f"[LOCACOEUR-{priority.upper()}] {subject}"
                body = f"""
                LOCACOEUR MQTT Client Alert
                Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
                Priority: {priority.upper()}
                Subject: {subject}
                Details:
                {message}
                Reconnection count: {self.reconnect_count}
                Last connection: {self.last_connection_time}
                This is an automated message from the LOCACOEUR MQTT monitoring system.
                """
                msg.attach(MIMEText(body, 'plain'))
                server = smtplib.SMTP(EMAIL_CONFIG["smtp_server"], EMAIL_CONFIG["smtp_port"])
                server.starttls()
                server.login(EMAIL_CONFIG["username"], EMAIL_CONFIG["password"])
                server.send_message(msg)
                server.quit()
                logger.info(f"Alert email sent successfully: {subject}")
                return
            except Exception as e:
                logger.error(f"Failed to send email alert (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
        logger.error(f"Failed to send email alert after {max_retries} attempts")

    def parse_timestamp(self, timestamp, device_serial: str, return_unix: bool = False):
        """Parse timestamp and log only once"""
        try:
            if isinstance(timestamp, (int, float)):
                logger.debug(f"Device {device_serial}: Original timestamp = {timestamp} (type: {type(timestamp)})")
                ts = datetime.fromtimestamp(timestamp / 1000.0 if timestamp > 9999999999 else timestamp, tz=timezone.utc)
                logger.debug(f"Device {device_serial}: Parsed timestamp {timestamp} -> {ts}")
                return int(ts.timestamp() * 1000) if return_unix else ts
            elif isinstance(timestamp, str):
                logger.debug(f"Device {device_serial}: Original timestamp = {timestamp} (type: {type(timestamp)})")
                try:
                    ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    logger.debug(f"Device {device_serial}: Parsed timestamp {timestamp} -> {ts}")
                    return int(ts.timestamp() * 1000) if return_unix else ts
                except ValueError:
                    ts = datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
                    logger.debug(f"Device {device_serial}: Parsed timestamp {timestamp} -> {ts}")
                    return int(ts.timestamp() * 1000) if return_unix else ts
            else:
                logger.warning(f"Invalid timestamp type for device {device_serial}: {type(timestamp)}")
                return None
        except Exception as e:
            logger.error(f"Error parsing timestamp for device {device_serial}: {e}")
            return None

    def validate_result_payload(self, device_serial: str, topic: str, data: Dict[str, Any]) -> bool:
        """Validate result payload against API documentation"""
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
        """Validate alert payload against API documentation"""
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

        return True

    def validate_status_payload(self, device_serial: str, topic: str, data: Dict[str, Any]) -> bool:
        """Validate status payload against API documentation"""
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

        return True

    def process_device_status(self, device_serial: str, data: Dict[str, Any]) -> Dict[str, str]:
        """Process device status and extract LED states according to API documentation"""
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

    def detect_critical_alerts(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Detect critical conditions and send alerts"""
        alerts = []
        critical_alert_messages = ["Defibrillator fault", "Power is cut", "Device is removed"]

        if "alert" in topic.lower():
            alert_message = data.get("message")
            if alert_message in critical_alert_messages:
                alerts.append(f"Critical alert: {alert_message} (ID: {data.get('id')})")
                self.alert_cache[device_serial] = data

        battery = data.get("battery")
        if battery is not None and isinstance(battery, (int, float)) and battery < 20:
            alerts.append(f"Low battery: {battery}%")

        led_mqtt = data.get("led_mqtt")
        if led_mqtt == "Red":
            alerts.append("Device connection lost")

        led_defibrillator = data.get("led_defibrillator")
        if led_defibrillator == "Red":
            alerts.append("Defibrillator not ready")

        led_power = data.get("led_power")
        if led_power == "Red":
            alerts.append("Power supply issue")

        led_environmental = data.get("led_environmental")
        if led_environmental == "Red":
            alerts.append("Temperature outside safe limits (below 5°C or above 40°C)")
        elif led_environmental == "Off":
            logger.debug(f"Device {device_serial}: Normal temperature range")

        if alerts:
            alert_message = f"Critical alerts for device {device_serial}:\n" + "\n".join(f"- {alert}" for alert in alerts)
            alert_message += f"\n\nTopic: {topic}\nData: {json.dumps(data, indent=2)}"
            self.send_alert_email(f"Critical Alert - Device {device_serial}", alert_message, "critical")
            logger.warning(f"Critical alerts sent for device {device_serial}: {alerts}")
        else:
            logger.debug(f"No critical alerts detected for device {device_serial} on topic {topic}")

    def insert_command(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        conn = self.connect_db()
        if not conn:
            return
        try:
            cur = conn.cursor()
            if "opId" not in data:
                logger.error(f"Missing opId in command payload for {device_serial} on topic {topic}")
                self.send_alert_email(
                    "Invalid Command Payload",
                    f"Missing opId in command for {device_serial} on topic {topic}",
                    "warning"
                )
                return
            payload_str = json.dumps(data, sort_keys=True)
            server_id = self.get_server_id()
            if server_id is None:
                logger.error(f"No server_id found for device {device_serial}")
                self.send_alert_email(
                    "Database Error",
                    f"No server_id found for device {device_serial} on topic {topic}",
                    "critical"
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
            operation_id = topic.split("/")[-1]
            message_id = data.get("message_id", str(uuid.uuid4()))
            cur.execute(
                """
                INSERT INTO Commands (
                    device_serial, server_id, operation_id, topic, message_id,
                    payload, created_at, is_get
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    device_serial[:50],
                    server_id,
                    operation_id[:50],
                    topic[:255],
                    message_id[:50],
                    payload_str,
                    datetime.now(),
                    "get" in operation_id.lower()
                )
            )
            conn.commit()
            logger.info(f"Inserted command for device {device_serial} on topic {topic}")
            with self.command_lock:
                self.command_cache[(device_serial, data["opId"])] = (topic, datetime.now(timezone.utc))
        except Exception as e:
            logger.error(f"Database error for device {device_serial}: {e}")
            conn.rollback()
            self.send_alert_email(
                "Database Error",
                f"Failed to insert command for device {device_serial} on topic {topic}: {e}",
                "critical"
            )
        finally:
            cur.close()
            self.release_db(conn)

    def insert_device_data(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Insert device data into the device_data table with improved validation and LED inference"""
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

            # Insert or update device record with improved error handling
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
                # Direct location event
                location_data = data.get("location", {})
                latitude = location_data.get("latitude")
                longitude = location_data.get("longitude")
                
                logger.debug(f"Processing location event for {device_serial}: latitude={latitude}, longitude={longitude}")
                
                if latitude is None or longitude is None:
                    logger.error(f"Missing location data for device {device_serial}: latitude={latitude}, longitude={longitude}")
                    self.send_alert_email(
                        "Invalid Location Data",
                        f"Missing location data for device {device_serial}: latitude={latitude}, longitude={longitude}",
                        "warning"
                    )
                    return
                
                if not isinstance(latitude, (int, float)) or not isinstance(longitude, (int, float)):
                    logger.error(f"Invalid location data types for device {device_serial}: latitude={type(latitude)}, longitude={type(longitude)}")
                    self.send_alert_email(
                        "Invalid Location Data",
                        f"Invalid location data types for device {device_serial}: latitude={type(latitude)}, longitude={type(longitude)}",
                        "warning"
                    )
                    return
                
                latitude = float(latitude)
                longitude = float(longitude)
                
                if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
                    logger.error(f"Invalid location coordinates for device {device_serial}: latitude={latitude}, longitude={longitude}")
                    self.send_alert_email(
                        "Invalid Location Data",
                        f"Invalid location coordinates for device {device_serial}: latitude={latitude}, longitude={longitude}",
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
                        latitude,
                        longitude,
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
                
                # Process LED states using improved logic
                led_states = self.process_device_status(device_serial, data)
                
                # Handle nested location data if present - FIXED LOGIC
                latitude = None
                longitude = None
                
                # Check for nested location data in status message
                location_data = data.get("location")
                if location_data and isinstance(location_data, dict):
                    lat_val = location_data.get("latitude")
                    lon_val = location_data.get("longitude")
                    
                    # Validate location data
                    if lat_val is not None and lon_val is not None:
                        try:
                            latitude = float(lat_val)
                            longitude = float(lon_val)
                            
                            # Validate coordinate ranges
                            if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
                                logger.warning(f"Invalid location coordinates for device {device_serial}: latitude={latitude}, longitude={longitude}")
                                latitude = longitude = None
                            else:
                                logger.debug(f"Valid location data for device {device_serial}: latitude={latitude}, longitude={longitude}")
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Could not convert location data for device {device_serial}: {e}")
                            latitude = longitude = None
                    else:
                        logger.debug(f"No location data in status message for device {device_serial}")
                
                logger.debug(f"Processing status event for {device_serial}: battery={battery}, "
                            f"led_power={led_states['led_power']}, led_defibrillator={led_states['led_defibrillator']}, "
                            f"led_monitoring={led_states['led_monitoring']}, led_assistance={led_states['led_assistance']}, "
                            f"led_mqtt={led_states['led_mqtt']}, led_environmental={led_states['led_environmental']}, "
                            f"location=({latitude}, {longitude})")
                
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
                        latitude,  # Now properly handled
                        longitude,  # Now properly handled
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
                    (device_serial[:50], "environmental", led_environmental, None, datetime.now())
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

            # Handle other event types - generic insertion
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
            
            # Detect critical alerts after successful insertion
            self.detect_critical_alerts(device_serial, topic, data)
            
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

    def insert_result(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Insert result data into the Results table with improved validation"""
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

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        """MQTT connection callback with improved handling"""
        if reason_code == 0:
            logger.info(f"Connected to MQTT broker at {MQTT_BROKER}:{MQTT_PORT} with client ID {MQTT_CLIENT_ID}")
            self.last_connection_time = datetime.now(timezone.utc)
            self.connection_established.set()

            # Subscribe to topics
            for topic, qos in MQTT_TOPICS:
                result = client.subscribe(topic, qos)
                if result[0] == mqtt.MQTT_ERR_SUCCESS:
                    logger.info(f"Subscribed to {topic} with QoS {qos}")
                else:
                    logger.error(f"Failed to subscribe to {topic}: {result}")

            # Send connection restored alert if this was a reconnection
            if self.reconnect_count > 0:
                self.send_alert_email(
                    "MQTT Connection Restored",
                    f"Successfully reconnected to MQTT broker after {self.reconnect_count} attempts",
                    "info"
                )

            self.reconnect_count = 0
        else:
            logger.error(f"Connection failed with reason code {reason_code}")
            self.connection_established.clear()
            self.reconnect_count += 1
            if self.reconnect_count >= 5:
                self.send_alert_email(
                    "MQTT Connection Failed",
                    f"Failed to connect to MQTT broker after {self.reconnect_count} attempts. Reason code: {reason_code}",
                    "critical"
                )

    def on_message(self, client, userdata, msg):
        """Handle incoming MQTT messages with robust deduplication and improved validation"""
        try:
            topic = msg.topic
            if not msg.payload:
                logger.warning(f"Empty payload received on topic {topic}")
                self.send_alert_email(
                    "Empty Payload",
                    f"Received empty payload on topic {topic}",
                    "warning"
                )
                return

            try:
                payload = msg.payload.decode('utf-8')
            except UnicodeDecodeError as e:
                logger.error(f"Failed to decode payload on topic {topic}: {e}")
                self.send_alert_email(
                    "Payload Decode Error",
                    f"Failed to decode payload on topic {topic}: {e}",
                    "warning"
                )
                return

            logger.debug(f"Received message on topic {topic}: {payload}")
            try:
                data = json.loads(payload)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in payload on topic {topic}: {e}")
                self.send_alert_email(
                    "Invalid Payload",
                    f"Failed to parse JSON on topic {topic}: {e}",
                    "warning"
                )
                return

            timestamp = data.get("timestamp", int(time.time() * 1000))
            message_id = hashlib.sha256(f"{topic}:{payload}:{timestamp}".encode()).hexdigest()
            with self.message_lock:
                if message_id in self.processed_messages:
                    logger.warning(f"Duplicate message detected for topic {topic}, message_id {message_id}")
                    return
                self.processed_messages.append(message_id)

            topic_parts = topic.split('/')
            if len(topic_parts) < 3:
                logger.error(f"Invalid topic format: {topic}")
                self.send_alert_email(
                    "Invalid Topic",
                    f"Invalid topic format: {topic}",
                    "warning"
                )
                return

            device_serial = topic_parts[1]
            event_type = topic_parts[2]
            operation_id = topic_parts[3] if len(topic_parts) > 3 else None

            if not device_serial:
                logger.error(f"Invalid device serial in topic: {topic}")
                self.send_alert_email(
                    "Invalid Topic",
                    f"Invalid device serial in topic: {topic}",
                    "warning"
                )
                return

            # Route to appropriate handler based on topic structure
            if event_type == "event":
                self.insert_device_data(device_serial, topic, data)
            elif event_type == "result":
                self.insert_result(device_serial, topic, data)
            elif event_type == "command":
                self.insert_command(device_serial, topic, data)
            else:
                logger.warning(f"Unknown event type: {event_type} for topic {topic}")
                # Fallback to generic processing
                self.insert_device_data(device_serial, topic, data)

            logger.info(f"Message processed for device {device_serial} from topic {topic}")

        except Exception as e:
            logger.error(f"Error processing message on topic {topic}: {e}")
            self.send_alert_email(
                "Message Processing Error",
                f"Failed to process message on topic {topic}: {e}",
                "critical"
            )

    def on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        """MQTT disconnect callback with improved handling"""
        logger.warning(f"Disconnected from MQTT broker with reason code {reason_code}")
        self.connection_established.clear()

        if self.running:
            self.reconnect_count += 1
            self.send_alert_email(
                "MQTT Disconnection",
                f"Disconnected from MQTT broker. Reason code: {reason_code}. Reconnect attempt: {self.reconnect_count}",
                "warning"
            )

    def on_log(self, client, userdata, level, buf):
        """MQTT log callback"""
        logger.debug(f"MQTT log: {buf}")

    def connect_with_retry(self, max_retries=5, retry_delay=5, max_total_attempts=25):
        """Connect to MQTT broker with improved retry logic and connection verification"""
        total_attempts = 0

        while self.running and total_attempts < max_total_attempts:
            for attempt in range(max_retries):
                total_attempts += 1
                try:
                    logger.info(f"Attempting to connect to MQTT broker at {MQTT_BROKER}:{MQTT_PORT} (attempt {attempt + 1}/{max_retries}, total: {total_attempts})")

                    # Clear the connection event before attempting connection
                    self.connection_established.clear()

                    # Attempt connection
                    result = self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
                    if result != mqtt.MQTT_ERR_SUCCESS:
                        logger.error(f"Connection call failed with code: {result}")
                        continue

                    # Start the network loop to process connection
                    self.client.loop_start()

                    # Wait for connection callback with timeout
                    if self.connection_established.wait(timeout=10):
                        logger.info("MQTT connection established successfully")
                        return True
                    else:
                        logger.warning("Connection timeout - callback not received within 10 seconds")
                        # Stop the loop and try again
                        try:
                            self.client.loop_stop()
                        except:
                            pass

                except Exception as e:
                    logger.error(f"MQTT connection attempt {attempt + 1} failed: {e}")
                    try:
                        self.client.loop_stop()
                    except:
                        pass

                if total_attempts >= max_total_attempts:
                    logger.error("Max total attempts reached")
                    self.send_alert_email(
                        "MQTT Connection Failed",
                        "Max total connection attempts reached",
                        "critical"
                    )
                    return False

                if attempt < max_retries - 1:
                    logger.info(f"Waiting {retry_delay} seconds before next attempt...")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 1.2, 30)  # Exponential backoff with max 30s

            if total_attempts < max_total_attempts:
                logger.info("Waiting 30 seconds before next retry cycle...")
                time.sleep(30)

        return False

    def setup_mqtt_client(self):
        """Setup MQTT client with TLS and callbacks"""
        # Create a new client instance
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=MQTT_CLIENT_ID,
            protocol=mqtt.MQTTv5,
            transport="tcp"
        )

        # Configure TLS
        try:
            self.client.tls_set(
                ca_certs=MQTT_CA_CERT,
                certfile=MQTT_CLIENT_CERT,
                keyfile=MQTT_CLIENT_KEY,
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLSv1_2
            )
            logger.debug("TLS configuration set successfully")
        except Exception as e:
            logger.error(f"Failed to configure TLS: {e}")
            raise

        # Set username/password if provided
        if MQTT_USERNAME and MQTT_PASSWORD:
            self.client.username_pw_set(username=MQTT_USERNAME, password=MQTT_PASSWORD)
            logger.debug("MQTT credentials set")

        # Set callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_log = self.on_log

        # Set additional client options for better reliability
        self.client.reconnect_delay_set(min_delay=1, max_delay=120)

        logger.debug("MQTT client configured successfully")

    # Command methods
    def request_config(self, device_serial: str) -> None:
        """Request the current device configuration using /get subtopic"""
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
        """Set device configuration"""
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
        """Request firmware version using /get subtopic"""
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

    def update_firmware(self, device_serial: str, version: str, firmware_url: str = None) -> None:
        """Update device firmware"""
        try:
            op_id = str(uuid.uuid4())
            topic = f"{TOPIC_PREFIX}/{device_serial}/command/update"
            payload = {"opId": op_id, "version": version}
            if firmware_url:
                payload["url"] = firmware_url
            self.client.publish(topic, json.dumps(payload), qos=0)
            logger.info(f"Sent firmware update command to {topic} with opId {op_id}")

            with self.command_lock:
                self.command_cache[(device_serial, op_id)] = (topic, datetime.now(timezone.utc))
        except Exception as e:
            logger.error(f"Failed to send firmware update command for {device_serial}: {e}")
            self.send_alert_email(
                "Firmware Update Command Failed",
                f"Failed to send firmware update command for device {device_serial}: {e}",
                "critical"
            )

    def request_location(self, device_serial: str) -> None:
        """Request the current device location"""
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
        """Play audio message on device"""
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
                "critical"
            )

    def request_log(self, device_serial: str) -> None:
        """Request device logs using /get subtopic"""
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

    def run(self):
        """Run the MQTT service with improved connection handling"""
        logger.info("Starting LOCACOEUR MQTT service...")
        threading.Timer(86400, self.backup_device_data).start()

        # Initial connection
        connected = False
        connection_attempts = 0
        max_connection_attempts = 3

        while self.running and not connected and connection_attempts < max_connection_attempts:
            connection_attempts += 1
            logger.info(f"Initial connection attempt {connection_attempts}/{max_connection_attempts}")
            connected = self.connect_with_retry()

            if not connected:
                logger.error(f"Failed to establish initial connection (attempt {connection_attempts})")
                if connection_attempts < max_connection_attempts:
                    logger.info("Waiting 30 seconds before next connection attempt...")
                    time.sleep(30)

        if not connected:
            logger.error("Failed to connect to MQTT broker after all initial attempts")
            self.send_alert_email(
                "MQTT Service Startup Failed",
                "Failed to establish initial MQTT connection",
                "critical"
            )
            return False

        logger.info("MQTT service started successfully")

        try:
            # Main service loop
            while self.running:
                try:
                    # Check connection status every 10 seconds
                    if not self.connection_established.is_set():
                        logger.warning("MQTT connection lost, attempting to reconnect...")

                        # Stop current loop if running
                        try:
                            self.client.loop_stop()
                            logger.debug("MQTT loop stopped for reconnection")
                        except Exception as e:
                            logger.warning(f"Error stopping MQTT loop: {e}")

                        # Create new client and attempt reconnection
                        self.setup_mqtt_client()

                        reconnect_attempts = 0
                        max_reconnect_attempts = 5
                        reconnected = False

                        while self.running and not reconnected and reconnect_attempts < max_reconnect_attempts:
                            reconnect_attempts += 1
                            logger.info(f"Reconnection attempt {reconnect_attempts}/{max_reconnect_attempts}")

                            reconnected = self.connect_with_retry(max_retries=3, retry_delay=5)

                            if not reconnected and reconnect_attempts < max_reconnect_attempts:
                                logger.info("Waiting 15 seconds before next reconnection attempt...")
                                time.sleep(15)

                        if not reconnected:
                            logger.error("Failed to reconnect after maximum attempts")
                            self.send_alert_email(
                                "MQTT Reconnection Failed",
                                "Failed to reconnect to MQTT broker after multiple attempts",
                                "critical"
                            )
                            break
                        else:
                            logger.info("Successfully reconnected to MQTT broker")

                except Exception as e:
                    logger.error(f"Error in main service loop: {e}")
                    self.send_alert_email(
                        "Service Loop Error",
                        f"Error in main service loop: {e}",
                        "critical"
                    )

                # Sleep for 10 seconds between connection checks
                time.sleep(10)

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
        except Exception as e:
            logger.error(f"Main loop crashed: {e}")
            self.send_alert_email("Main Loop Crashed", f"Main loop crashed with error: {e}", "critical")
        finally:
            logger.info("Shutting down MQTT service...")
            self.running = False

            try:
                if hasattr(self, 'client') and self.client:
                    self.client.loop_stop()
                    self.client.disconnect()
                    logger.info("MQTT client disconnected and loop stopped")
            except Exception as e:
                logger.warning(f"Error during final MQTT cleanup: {e}")

            try:
                if hasattr(self, 'db_pool') and self.db_pool:
                    self.db_pool.closeall()
                    logger.info("Database pool closed")
            except Exception as e:
                logger.warning(f"Error closing database pool: {e}")

        logger.info("LOCACOEUR MQTT service stopped")
        return True

if __name__ == "__main__":
    service = MQTTService()
    try:
        sys.stdout.write("")
        sys.stdout.flush()
        console_available = True
    except:
        console_available = False
    if not console_available:
        logger.info("Running as a service (no console)")
    try:
        service.run()
    except Exception as e:
        logger.error(f"Service crashed: {e}")
        service.send_alert_email("Service Crashed", f"MQTT service crashed with error: {e}", "critical")
        sys.exit(1)
