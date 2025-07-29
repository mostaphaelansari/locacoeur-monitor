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

import paho.mqtt.client as mqtt
import psycopg2
from psycopg2 import errors
from psycopg2 import pool
from decouple import config
from cachetools import TTLCache
from collections import deque
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/locacoeur-monitor/backend/logs/mqtt_client.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

timestamp_logger = logging.getLogger('timestamp_debug')
timestamp_handler = logging.FileHandler('/opt/locacoeur-monitor/backend/logs/timestamp_debug.log')
timestamp_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
timestamp_logger.addHandler(timestamp_handler)
timestamp_logger.setLevel(logging.DEBUG)

# MQTT settings
MQTT_BROKER = config("MQTT_BROKER", default="mqtt.locacoeur.com")
MQTT_PORT = int(config("MQTT_PORT", default=8883))
MQTT_TOPICS = [("LC1/+/event/#", 0), ("LC1/+/command/#", 0), ("LC1/+/result", 0)]
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
        self.db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **DB_CONFIG)
        self.command_lock = threading.Lock()
        self.command_cache = {}
        self.message_lock = threading.Lock()  # Fix for message_lock AttributeError
        self.processed_messages = deque(maxlen=1000)  # Use deque for FIFO eviction
        self.client = None
        self.setup_signal_handlers()
        self.initialize_db()
        self.setup_mqtt_client()
        self.start_followup_checker()

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown (unchanged)"""
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
        """Get a database connection from the pool (unchanged)"""
        try:
            return self.db_pool.getconn()
        except Exception as e:
            logger.error(f"Failed to get database connection: {e}")
            self.send_alert_email("Database Connection Failed", f"Failed to get database connection: {e}", "critical")
            return None

    def release_db(self, conn):
        """Release a database connection back to the pool (unchanged)"""
        try:
            self.db_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Failed to release database connection: {e}")

    def get_server_id(self) -> int:
        """Retrieve or create server_id for the current server"""
        conn = self.connect_db()
        if not conn:
            logger.error("Failed to connect to database for server_id retrieval")
            return None
        try:
            cur = conn.cursor()
            mqtt_url = "mqtts://mqtt.locacoeur.com:8883"
            cur.execute(
                """
                SELECT server_id FROM Servers
                WHERE environment = %s AND mqtt_url = %s
                """,
                ("production", mqtt_url)
            )
            result = cur.fetchone()
            if result:
                return result[0]

            cur.execute(
                """
                INSERT INTO Servers (environment, mqtt_url)
                VALUES (%s, %s)
                ON CONFLICT (environment) DO NOTHING
                RETURNING server_id
                """,
                ("production", mqtt_url)
            )
            result = cur.fetchone()
            server_id = result[0] if result else None
            if server_id is None:
                cur.execute(
                    """
                    SELECT server_id FROM Servers
                    WHERE environment = %s AND mqtt_url = %s
                    """,
                    ("production", mqtt_url)
                )
                result = cur.fetchone()
                server_id = result[0] if result else None
            if server_id is None:
                raise ValueError("Failed to retrieve or create server_id")
            conn.commit()
            logger.info(f"Created or retrieved server_id {server_id} for environment='production'")
            return server_id
        except Exception as e:
            logger.error(f"Error retrieving or creating server_id: {e}")
            conn.rollback()
            self.send_alert_email(
                "Database Error",
                f"Failed to retrieve or create server_id: {e}",
                "critical"
            )
            return None
        finally:
            cur.close()
            self.release_db(conn)

    def initialize_db(self):
        """Initialize database schema with device_data_backup"""
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
                    device_serial VARCHAR(50) PRIMARY KEY,
                    mqtt_broker_url VARCHAR(255),
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS Commands (
                    command_id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(device_serial) ON DELETE CASCADE,
                    server_id INTEGER REFERENCES Servers(server_id) ON DELETE RESTRICT,
                    operation_id VARCHAR(50),
                    topic VARCHAR(255),
                    message_id VARCHAR(50),
                    payload JSONB,
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    is_get BOOLEAN
                );
                CREATE TABLE IF NOT EXISTS Results (
                    result_id SERIAL PRIMARY KEY,
                    command_id INTEGER REFERENCES Commands(command_id) ON DELETE SET NULL,
                    device_serial VARCHAR(50) REFERENCES Devices(device_serial) ON DELETE CASCADE,
                    topic VARCHAR(255),
                    result_status VARCHAR(50),
                    result_message TEXT,
                    payload JSONB,
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS Events (
                    event_id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(device_serial) ON DELETE CASCADE,
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
                    device_serial VARCHAR(50) REFERENCES Devices(device_serial) ON DELETE CASCADE,
                    led_type VARCHAR(50) NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    description TEXT,
                    last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT leds_status_check CHECK (status IN ('Green', 'Red', 'Off')),
                    UNIQUE (device_serial, led_type)
                );
                CREATE TABLE IF NOT EXISTS device_data (
                    id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(device_serial) ON DELETE CASCADE,
                    topic VARCHAR(255),
                    battery INTEGER CHECK (battery >= 0 AND battery <= 100),
                    connection VARCHAR(50),
                    defibrillator INTEGER CHECK (defibrillator >= 0),
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
                    received_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    alert_id INTEGER,
                    alert_message VARCHAR(255),
                    original_timestamp BIGINT
                );
                CREATE TABLE IF NOT EXISTS device_data_backup (
                    LIKE device_data INCLUDING ALL,
                    backup_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_device_data_backup_serial ON device_data_backup(device_serial);
                CREATE INDEX IF NOT EXISTS idx_device_data_backup_received_at ON device_data_backup(received_at);
                -- New tables for insert_data
                CREATE TABLE IF NOT EXISTS Locations (
                    id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(device_serial) ON DELETE CASCADE,
                    timestamp BIGINT,
                    latitude DOUBLE PRECISION CHECK (latitude BETWEEN -90 AND 90),
                    longitude DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),
                    UNIQUE (device_serial, timestamp)
                );
                CREATE TABLE IF NOT EXISTS Versions (
                    id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(device_serial) ON DELETE CASCADE,
                    timestamp BIGINT,
                    version VARCHAR(50),
                    UNIQUE (device_serial, timestamp)
                );
                CREATE TABLE IF NOT EXISTS Status (
                    id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(device_serial) ON DELETE CASCADE,
                    timestamp BIGINT,
                    state VARCHAR(50),
                    UNIQUE (device_serial, timestamp)
                );
                CREATE TABLE IF NOT EXISTS Alerts (
                    id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(device_serial) ON DELETE CASCADE,
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
        """Start a background thread to check for missing command results."""
        def check_missing_results():
            while self.running:
                try:
                    self.check_command_timeouts()
                except Exception as e:
                    logger.error(f"Error in command timeout checker: {e}")
                    self.send_alert_email(
                        "Command Timeout Checker Error",
                        f"Error in background command timeout checker: {e}",
                        "critical"
                    )
                time.sleep(60)  # Check every minute
        checker_thread = threading.Thread(target=check_missing_results, daemon=True)
        checker_thread.start()
        logger.info("Started command timeout checker thread")

    def check_command_timeouts(self):
        """Check for commands that haven't received results within 5 minutes."""
        with self.command_lock:
            logger.debug(f"Checking command cache: {list(self.command_cache.items())}")
            expired_commands = []
            current_time = datetime.now(timezone.utc)
            for (device_serial, op_id), (topic, sent_time) in list(self.command_cache.items()):
                if current_time - sent_time > timedelta(minutes=5):
                    expired_commands.append((device_serial, op_id, topic, sent_time))
                    del self.command_cache[(device_serial, op_id)]
            logger.debug(f"Expired commands: {expired_commands}")

        if not expired_commands:
            logger.debug("No expired commands found")
            return

        conn = self.connect_db()
        if not conn:
            logger.error("Failed to connect to database for timeout check")
            return
        try:
            cur = conn.cursor()
            for device_serial, op_id, topic, sent_time in expired_commands:
                cur.execute(
                    """
                    SELECT result_id FROM Results
                    WHERE device_serial = %s AND payload->>'opId' = %s
                    """,
                    (device_serial, op_id)
                )
                if cur.fetchone():
                    logger.debug(f"Result found for command {op_id} on {device_serial}, no alert needed")
                    continue
                logger.warning(f"Command timeout: No result for opId {op_id} on {device_serial} (topic: {topic})")
                self.send_alert_email(
                    f"Command Timeout - Device {device_serial}",
                    f"No result received for command opId {op_id} on topic {topic} "
                    f"sent at {sent_time.strftime('%Y-%m-%d %H:%M:%S UTC')} after 5 minutes.",
                    "critical"
                )
            conn.commit()
        except Exception as e:
            logger.error(f"Database error during timeout check: {e}")
            conn.rollback()
            self.send_alert_email(
                "Database Error",
                f"Failed to check command timeouts: {e}",
                "critical"
            )
        finally:
            cur.close()
            self.release_db(conn)

    def backup_device_data(self):
        """Backup old device_data records and clean up"""
        conn = self.connect_db()
        if not conn:
            return
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO device_data_backup
                SELECT *, CURRENT_TIMESTAMP FROM device_data WHERE received_at < %s
            """, (datetime.now(timezone.utc) - timedelta(days=30),))
            cur.execute("DELETE FROM device_data WHERE received_at < %s",
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
        """Send email alert with rate limiting (unchanged)"""
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

    def parse_timestamp(self, timestamp_value: Any, device_serial: str, return_unix: bool = False) -> Any:
        """Parse timestamps and return UTC datetime or Unix timestamp (milliseconds)"""
        current_time = datetime.now(timezone.utc)
        timestamp_logger.debug(f"Device {device_serial}: Original timestamp = {timestamp_value} (type: {type(timestamp_value)})")

        if not timestamp_value:
            logger.debug(f"Device {device_serial}: No timestamp provided, using current time")
            return int(current_time.timestamp() * 1000) if return_unix else current_time

        parsed_dt = None
        if isinstance(timestamp_value, (int, float)):
            try:
                if timestamp_value > 1e12:  # Milliseconds
                    timestamp_value = timestamp_value / 1000
                parsed_dt = datetime.fromtimestamp(timestamp_value, tz=timezone.utc)
            except (ValueError, OSError) as e:
                logger.warning(f"Device {device_serial}: Invalid Unix timestamp: {timestamp_value}, using current time. Error: {e}")
                return int(current_time.timestamp() * 1000) if return_unix else current_time
        elif isinstance(timestamp_value, str):
            try:
                if timestamp_value.endswith('Z'):
                    parsed_dt = datetime.fromisoformat(timestamp_value.replace('Z', '+00:00'))
                elif '+' in timestamp_value or timestamp_value.endswith(('00', '30')):
                    parsed_dt = datetime.fromisoformat(timestamp_value)
                else:
                    parsed_dt = datetime.fromisoformat(timestamp_value).replace(tzinfo=timezone.utc)
            except ValueError:
                try:
                    unix_ts = float(timestamp_value)
                    if unix_ts > 1e12:  # Milliseconds
                        unix_ts = unix_ts / 1000
                    parsed_dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                except (ValueError, OSError):
                    logger.warning(f"Device {device_serial}: Failed to parse timestamp string: {timestamp_value}")
        elif isinstance(timestamp_value, datetime):
            if timestamp_value.tzinfo is None:
                parsed_dt = timestamp_value.replace(tzinfo=timezone.utc)
            else:
                parsed_dt = timestamp_value.astimezone(timezone.utc)

        if parsed_dt is None:
            logger.warning(f"Device {device_serial}: Unable to parse timestamp: {timestamp_value}, using current time")
            return int(current_time.timestamp() * 1000) if return_unix else current_time

        min_valid_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        max_valid_time = current_time + timedelta(days=1)
        if parsed_dt < min_valid_time or parsed_dt > max_valid_time:
            logger.warning(f"Device {device_serial}: Timestamp out of range ({parsed_dt}), using current time")
            timestamp_logger.debug(f"Device {device_serial}: Replaced out-of-range timestamp {timestamp_value} -> {current_time}")
            return int(current_time.timestamp() * 1000) if return_unix else current_time

        timestamp_logger.debug(f"Device {device_serial}: Parsed timestamp {timestamp_value} -> {parsed_dt}")
        return int(parsed_dt.timestamp() * 1000) if return_unix else parsed_dt

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

    def request_firmware_version(self, device_serial: str) -> None:
        try:
            op_id = str(uuid.uuid4())
            topic = f"LC1/{device_serial}/command/version"
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

    def update_firmware(self, device_serial: str, firmware_version: str, firmware_url: str) -> None:
        """Send a firmware update command to the specified device."""
        try:
            op_id = str(uuid.uuid4())
            topic = f"LC1/{device_serial}/command/update"
            payload = {
                "opId": op_id,
                "version": firmware_version,
                "url": firmware_url
            }
            self.client.publish(topic, json.dumps(payload), qos=0)
            logger.info(f"Sent firmware update command to {topic} with opId {op_id}, payload: {payload}")
        except Exception as e:
            logger.error(f"Failed to send firmware update for {device_serial}: {e}")
            self.send_alert_email(
                "Firmware Update Failed",
                f"Failed to send firmware update command for device {device_serial}: {e}",
                "critical"
            )

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
        """Insert device data into the device_data table with improved error handling"""
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
                        original_timestamp, received_at, payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        device_serial[:50],
                        topic[:255],
                        float(latitude),
                        float(longitude),
                        timestamp,
                        timestamp,
                        datetime.now(),
                        payload_str
                    )
                )

            # Handle status events
            elif topic.endswith("/event/status"):
                battery = data.get("battery")
                led_power = data.get("led_power")
                led_defibrillator = data.get("led_defibrillator")
                led_monitoring = data.get("led_monitoring")
                led_assistance = data.get("led_assistance")
                led_mqtt = data.get("led_mqtt")
                led_environmental = data.get("led_environmental")
                
                logger.debug(f"Processing status event for {device_serial}: battery={battery}, "
                            f"led_power={led_power}, led_defibrillator={led_defibrillator}, "
                            f"led_monitoring={led_monitoring}, led_assistance={led_assistance}, "
                            f"led_mqtt={led_mqtt}, led_environmental={led_environmental}")
                
                # Validate battery value
                if battery is not None and (not isinstance(battery, int) or not 0 <= battery <= 100):
                    logger.error(f"Invalid battery value for device {device_serial}: {battery}")
                    self.send_alert_email(
                        "Invalid Status Data",
                        f"Invalid battery value for device {device_serial}: {battery}",
                        "warning"
                    )
                    return
                
                # Validate LED values
                valid_leds = {"Green", "Red", "Off"}
                for led, value in [
                    ("Power", led_power),
                    ("Defibrillator", led_defibrillator),
                    ("Monitoring", led_monitoring),
                    ("Assistance", led_assistance),
                    ("MQTT", led_mqtt),
                    ("Environmental", led_environmental)
                ]:
                    if value is not None and value not in valid_leds:
                        logger.error(f"Invalid LED value for {led} on device {device_serial}: {value}")
                        self.send_alert_email(
                            "Invalid Status Data",
                            f"Invalid LED value for {led} on device {device_serial}: {value}",
                            "warning"
                        )
                        return
                
                # Insert status data
                cur.execute(
                    """
                    INSERT INTO device_data (
                        device_serial, topic, battery, led_power, led_defibrillator,
                        led_monitoring, led_assistance, led_mqtt, led_environmental,
                        timestamp, original_timestamp, received_at, payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        device_serial[:50],
                        topic[:255],
                        battery,
                        led_power if led_power else None,
                        led_defibrillator if led_defibrillator else None,
                        led_monitoring if led_monitoring else None,
                        led_assistance if led_assistance else None,
                        led_mqtt if led_mqtt else None,
                        led_environmental if led_environmental else None,
                        timestamp,
                        timestamp,
                        datetime.now(),
                        payload_str
                    )
                )
                
                # Update LEDs table
                led_values = [
                    ("Power", led_power),
                    ("Defibrillator", led_defibrillator),
                    ("Monitoring", led_monitoring),
                    ("Assistance", led_assistance),
                    ("MQTT", led_mqtt),
                    ("Environmental", led_environmental)
                ]
                
                for led_type, status in led_values:
                    if status is not None:
                        logger.debug(f"Inserting LED: device={device_serial}, type={led_type}, status={status}")
                        cur.execute(
                            """
                            INSERT INTO LEDs (device_serial, led_type, status, description, last_updated)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (device_serial, led_type)
                            DO UPDATE SET status = EXCLUDED.status, description = EXCLUDED.description, last_updated = EXCLUDED.last_updated
                            """,
                            (device_serial[:50], led_type, status, None, datetime.now())
                        )
                        logger.info(f"Inserted into LEDs for device {device_serial}: type={led_type}, status={status}")
                    else:
                        logger.warning(f"Missing LED value for {led_type} on device {device_serial}")
                
                # Send alerts for critical conditions
                if battery is not None and battery < 20:
                    self.send_alert_email(
                        "Low Battery",
                        f"Low battery for device {device_serial}: {battery}%",
                        "critical"
                    )
                
                if led_power == "Red":
                    self.send_alert_email(
                        "Power Supply Issue",
                        f"Power supply issue for device {device_serial}: led_power=Red",
                        "critical"
                    )

            # Handle alert events
            elif topic.endswith("/event/alert"):
                alert_id = data.get("id")
                alert_message = data.get("message")
                logger.debug(f"Processing alert event for {device_serial}: alert_id={alert_id}, alert_message={alert_message}")
                
                if not isinstance(alert_id, int):
                    logger.error(f"Invalid alert_id for device {device_serial}: {alert_id}")
                    self.send_alert_email(
                        "Invalid Alert Data",
                        f"Invalid alert_id for device {device_serial}: {alert_id}",
                        "warning"
                    )
                    return
                
                cur.execute(
                    """
                    INSERT INTO device_data (
                        device_serial, topic, alert_id, alert_message, timestamp,
                        original_timestamp, received_at, payload
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
                        datetime.now(),
                        payload_str
                    )
                )
                
                self.send_alert_email(
                    f"Critical Alert - Device {device_serial}",
                    f"Alert for device {device_serial}: {alert_message}",
                    "critical"
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

    def insert_version_data(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Insert version data into the Events table"""
        conn = self.connect_db()
        if not conn:
            logger.error("Failed to connect to database")
            return
        try:
            cur = conn.cursor()
            server_id = self.get_server_id()
            if server_id is None:
                logger.error(f"Failed to get server_id for device {device_serial}")
                return
            payload_str = json.dumps(data, sort_keys=True)
            timestamp = data.get("timestamp")
            if not isinstance(timestamp, int) or timestamp < 0 or timestamp > 2**32-1:
                logger.error(f"Invalid timestamp for device {device_serial}: {timestamp}")
                self.send_alert_email(
                    "Invalid Timestamp",
                    f"Invalid timestamp for device {device_serial} on topic {topic}: {timestamp}",
                    "warning"
                )
                return
            timestamp = self.parse_timestamp(timestamp, device_serial, return_unix=False)
            if timestamp is None:
                logger.error(f"Invalid parsed timestamp for device {device_serial} on topic {topic}")
                return
            version = data.get("version")
            if not isinstance(version, str):
                logger.error(f"Invalid version for device {device_serial}: {version}")
                self.send_alert_email(
                    "Invalid Version Data",
                    f"Invalid version for device {device_serial}: {version}",
                    "warning"
                )
                return
            logger.debug(f"Inserting version event for {device_serial}: version={version}")
            cur.execute(
                """
                INSERT INTO Events (
                    device_serial, server_id, operation_id, topic, payload,
                    event_timestamp, created_at, original_timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (device_serial, topic, event_timestamp)
                DO UPDATE SET
                    payload = EXCLUDED.payload,
                    created_at = EXCLUDED.created_at,
                    original_timestamp = EXCLUDED.original_timestamp
                """,
                (
                    device_serial[:50],
                    server_id,
                    "version",
                    topic[:255],
                    payload_str,
                    timestamp,
                    datetime.now(),
                    str(data.get("timestamp"))
                )
            )
            conn.commit()
            logger.info(f"Inserted version data for device {device_serial} on topic {topic}")
        except Exception as e:
            logger.error(f"Database error for device {device_serial}: {e}")
            conn.rollback()
            self.send_alert_email(
                "Database Error",
                f"Failed to insert version data for device {device_serial} on topic {topic}: {e}",
                "critical"
            )
        finally:
            cur.close()
            self.release_db(conn)

    def insert_log_data(self, device_serial: str, topic: str, data: Any) -> None:
        """Insert log data into the Events table"""
        conn = self.connect_db()
        if not conn:
            logger.error("Failed to connect to database")
            return
        try:
            cur = conn.cursor()
            server_id = self.get_server_id()
            if server_id is None:
                logger.error(f"Failed to get server_id for device {device_serial}")
                return
            timestamp = None
            payload_str = data if isinstance(data, str) else json.dumps(data, sort_keys=True)
            if isinstance(data, dict):
                timestamp = data.get("timestamp")
                if not isinstance(timestamp, int) or timestamp < 0 or timestamp > 2**32-1:
                    logger.error(f"Invalid timestamp for device {device_serial}: {timestamp}")
                    self.send_alert_email(
                        "Invalid Timestamp",
                        f"Invalid timestamp for device {device_serial} on topic {topic}: {timestamp}",
                        "warning"
                    )
                    return
                timestamp = self.parse_timestamp(timestamp, device_serial, return_unix=False)
                if timestamp is None:
                    logger.error(f"Invalid parsed timestamp for device {device_serial} on topic {topic}")
                    return
            else:
                timestamp = datetime.now(timezone.utc)
                logger.debug(f"Using current time as timestamp for string log payload from {device_serial}")
            if not isinstance(data, (str, dict)):
                logger.error(f"Invalid log payload for device {device_serial}: {data}")
                self.send_alert_email(
                    "Invalid Log Data",
                    f"Invalid log payload for device {device_serial}: {data}",
                    "warning"
                )
                return
            logger.debug(f"Inserting log event for {device_serial}: payload={payload_str}")
            cur.execute(
                """
                INSERT INTO Events (
                    device_serial, server_id, operation_id, topic, payload,
                    event_timestamp, created_at, original_timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (device_serial, topic, event_timestamp)
                DO UPDATE SET
                    payload = EXCLUDED.payload,
                    created_at = EXCLUDED.created_at,
                    original_timestamp = EXCLUDED.original_timestamp
                """,
                (
                    device_serial[:50],
                    server_id,
                    "log",
                    topic[:255],
                    payload_str,
                    timestamp,
                    datetime.now(),
                    str(data.get("timestamp")) if isinstance(data, dict) else None
                )
            )
            conn.commit()
            logger.info(f"Inserted log data for device {device_serial} on topic {topic}")
        except Exception as e:
            logger.error(f"Database error for device {device_serial}: {e}")
            conn.rollback()
            self.send_alert_email(
                "Database Error",
                f"Failed to insert log data for device {device_serial} on topic {topic}: {e}",
                "critical"
            )
        finally:
            cur.close()
            self.release_db(conn)

    def insert_result(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Insert result data into the Results table"""
        conn = self.connect_db()
        if not conn:
            return
        try:
            cur = conn.cursor()
            payload_str = json.dumps(data, sort_keys=True)
            result_status = str(data.get("result", ""))[:50]
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
            if "opId" not in data:
                logger.error(f"Missing opId in result payload for {device_serial} on topic {topic}")
                self.send_alert_email(
                    "Invalid Result Payload",
                    f"Missing opId in result for {device_serial} on topic {topic}",
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
            if "opId" in data:
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
                    result_status,
                    result_message,
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
                result_codes = {
                    "0": "Success",
                    "-1": "Missing opId",
                    "-2": "Null object reference",
                    "-3": "Audio file not found"
                }
                result_desc = result_codes.get(result_status, f"Unknown result code: {result_status}")
                logger.info(f"Firmware update result for {device_serial}: status={result_desc}, message={result_message}")
                if result_status != "0":
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

    def insert_data(self, topic: str, payload: Dict[str, Any]):
        """Insert data into database with granular UniqueViolation handling"""
        conn = self.connect_db()
        if not conn:
            logger.error("Failed to connect to database")
            return
        cur = conn.cursor()
        try:
            device_serial = payload.get("device_serial")
            event = payload.get("event")
            timestamp = payload.get("timestamp")

            if not all([device_serial, event, timestamp]):
                logger.warning(f"Missing fields in payload: {payload}")
                return

            event_type = event.get("type")
            if not event_type:
                logger.warning(f"Missing event type in payload: {payload}")
                return

            # Insert into Devices table
            try:
                cur.execute(
                    """
                    INSERT INTO Devices (device_serial)
                    VALUES (%s)
                    ON CONFLICT (device_serial) DO NOTHING
                    """,
                    (device_serial[:50],)
                )
            except psycopg2.errors.UniqueViolation:
                logger.debug(f"Duplicate device record skipped for {device_serial}")
                conn.rollback()
                cur = conn.cursor()  # Re-create cursor after rollback
            except Exception as e:
                logger.error(f"Error inserting into Devices for {device_serial}: {e}")
                conn.rollback()
                self.send_alert_email(
                    "Database Error",
                    f"Failed to insert into Devices for {device_serial}: {e}",
                    "critical"
                )
                return

            # Insert into Events table
            try:
                cur.execute(
                    """
                    INSERT INTO Events (device_serial, timestamp, type, raw_payload)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (device_serial[:50], timestamp, event_type[:50], json.dumps(payload))
                )
            except psycopg2.errors.UniqueViolation:
                logger.debug(f"Duplicate event record skipped for {device_serial} on topic {topic}")
                conn.rollback()
                cur = conn.cursor()
            except Exception as e:
                logger.error(f"Error inserting into Events for {device_serial}: {e}")
                conn.rollback()
                self.send_alert_email(
                    "Database Error",
                    f"Failed to insert into Events for {device_serial} on topic {topic}: {e}",
                    "critical"
                )
                return

            # Handle 'location' event
            if event_type == "location":
                lat = event.get("latitude")
                lon = event.get("longitude")
                if lat is None or lon is None:
                    logger.warning(f"Incomplete location data from {device_serial}")
                    return
                try:
                    cur.execute(
                        """
                        INSERT INTO Locations (device_serial, timestamp, latitude, longitude)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (device_serial[:50], timestamp, lat, lon)
                    )
                except psycopg2.errors.UniqueViolation:
                    logger.debug(f"Duplicate location record skipped for {device_serial} at timestamp {timestamp}")
                    conn.rollback()
                    cur = conn.cursor()
                except Exception as e:
                    logger.error(f"Error inserting into Locations for {device_serial}: {e}")
                    conn.rollback()
                    self.send_alert_email(
                        "Database Error",
                        f"Failed to insert into Locations for {device_serial}: {e}",
                        "critical"
                    )
                    return

            # Handle 'version' event
            elif event_type == "version":
                version = event.get("firmware")
                if not version:
                    logger.warning(f"Missing firmware version from {device_serial}")
                    return
                try:
                    cur.execute(
                        """
                        INSERT INTO Versions (device_serial, timestamp, version)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (device_serial, timestamp) DO UPDATE SET version = EXCLUDED.version, timestamp = EXCLUDED.timestamp
                        """,
                        (device_serial[:50], timestamp, version[:50])
                    )
                except psycopg2.errors.UniqueViolation:
                    logger.debug(f"Duplicate version record skipped for {device_serial} at timestamp {timestamp}")
                    conn.rollback()
                    cur = conn.cursor()
                except Exception as e:
                    logger.error(f"Error inserting into Versions for {device_serial}: {e}")
                    conn.rollback()
                    self.send_alert_email(
                        "Database Error",
                        f"Failed to insert into Versions for {device_serial}: {e}",
                        "critical"
                    )
                    return

            # Handle 'status' event
            elif event_type == "status":
                status = event.get("state")
                if not status:
                    logger.warning(f"Missing status from {device_serial}")
                    return
                try:
                    cur.execute(
                        """
                        INSERT INTO Status (device_serial, timestamp, state)
                        VALUES (%s, %s, %s)
                        """,
                        (device_serial[:50], timestamp, status[:50])
                    )
                except psycopg2.errors.UniqueViolation:
                    logger.debug(f"Duplicate status record skipped for {device_serial} at timestamp {timestamp}")
                    conn.rollback()
                    cur = conn.cursor()
                except Exception as e:
                    logger.error(f"Error inserting into Status for {device_serial}: {e}")
                    conn.rollback()
                    self.send_alert_email(
                        "Database Error",
                        f"Failed to insert into Status for {device_serial}: {e}",
                        "critical"
                    )
                    return

            # Handle 'alert' event
            elif event_type == "alert":
                alert_code = event.get("code")
                if not alert_code:
                    logger.warning(f"Missing alert code from {device_serial}")
                    return
                cache_key = f"{device_serial}:{alert_code}"
                if cache_key not in self.alert_cache:
                    self.alert_cache[cache_key] = True
                    self.send_alert_email(
                        "Device Alert",
                        f"Alert {alert_code} from device {device_serial}",
                        "warning"
                    )
                try:
                    cur.execute(
                        """
                        INSERT INTO Alerts (device_serial, timestamp, code)
                        VALUES (%s, %s, %s)
                        """,
                        (device_serial[:50], timestamp, alert_code[:50])
                    )
                except psycopg2.errors.UniqueViolation:
                    logger.debug(f"Duplicate alert record skipped for {device_serial} with code {alert_code}")
                    conn.rollback()
                    cur = conn.cursor()
                except Exception as e:
                    logger.error(f"Error inserting into Alerts for {device_serial}: {e}")
                    conn.rollback()
                    self.send_alert_email(
                        "Database Error",
                        f"Failed to insert into Alerts for {device_serial}: {e}",
                        "critical"
                    )
                    return

            # Handle LEDs
            leds = event.get("leds")
            if leds:
                for led in leds:
                    led_type = led.get("type")
                    state = led.get("state")
                    if not led_type or state is None:
                        continue
                    try:
                        cur.execute(
                            """
                            INSERT INTO LEDs (device_serial, timestamp, type, state)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (device_serial, type) DO UPDATE SET state = EXCLUDED.state, timestamp = EXCLUDED.timestamp
                            """,
                            (device_serial[:50], timestamp, led_type[:50], state[:50])
                        )
                    except psycopg2.errors.UniqueViolation:
                        logger.debug(f"Duplicate LED record skipped for {device_serial}, type {led_type}")
                        conn.rollback()
                        cur = conn.cursor()
                    except Exception as e:
                        logger.error(f"Error inserting into LEDs for {device_serial}, type {led_type}: {e}")
                        conn.rollback()
                        self.send_alert_email(
                            "Database Error",
                            f"Failed to insert into LEDs for {device_serial}, type {led_type}: {e}",
                            "critical"
                        )
                        continue

            # Handle Results
            result = payload.get("result")
            if result:
                op_id = result.get("opId")
                res = result.get("value")
                if op_id:
                    try:
                        cur.execute(
                            """
                            INSERT INTO Results (device_serial, timestamp, op_id, result)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (op_id) DO UPDATE SET result = EXCLUDED.result, timestamp = EXCLUDED.timestamp
                            """,
                            (device_serial[:50], timestamp, op_id[:50], res)
                        )
                    except psycopg2.errors.UniqueViolation:
                        logger.debug(f"Duplicate result record skipped for {device_serial}, op_id {op_id}")
                        conn.rollback()
                        cur = conn.cursor()
                    except Exception as e:
                        logger.error(f"Error inserting into Results for {device_serial}: {e}")
                        conn.rollback()
                        self.send_alert_email(
                            "Database Error",
                            f"Failed to insert into Results for {device_serial}: {e}",
                            "critical"
                        )
                        return

            # Handle Commands
            command = payload.get("command")
            if command:
                op_id = command.get("opId")
                action = command.get("action")
                if op_id and action:
                    with self.command_lock:
                        self.command_cache[op_id] = {"device_serial": device_serial, "action": action}
                    try:
                        cur.execute(
                            """
                            INSERT INTO Commands (device_serial, timestamp, op_id, action)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (op_id) DO NOTHING
                            """,
                            (device_serial[:50], timestamp, op_id[:50], action[:50])
                        )
                    except psycopg2.errors.UniqueViolation:
                        logger.debug(f"Duplicate command record skipped for {device_serial}, op_id {op_id}")
                        conn.rollback()
                        cur = conn.cursor()
                    except Exception as e:
                        logger.error(f"Error inserting into Commands for {device_serial}: {e}")
                        conn.rollback()
                        self.send_alert_email(
                            "Database Error",
                            f"Failed to insert into Commands for {device_serial}: {e}",
                            "critical"
                        )
                        return

            conn.commit()
            logger.info(f"Data processed for device {device_serial} from topic {topic}")

        except Exception as e:
            logger.error(f"Unexpected error for device {device_serial}: {e}")
            conn.rollback()
            self.send_alert_email(
                "Unexpected Error",
                f"Unexpected error processing data for {device_serial} on topic {topic}: {e}",
                "critical"
            )
        finally:
            cur.close()
            self.release_db(conn)

    def request_config(self, device_serial: str) -> None:
        """Request the current device configuration."""
        try:
            op_id = str(uuid.uuid4())
            topic = f"LC1/{device_serial}/command/config/get"
            payload = {"opId": op_id}
            self.client.publish(topic, json.dumps(payload), qos=0)
            logger.info(f"Sent config request to {topic} with opId {op_id}")
        except Exception as e:
            logger.error(f"Failed to send config request for {device_serial}: {e}")
            self.send_alert_email(
                "Config Request Failed",
                f"Failed to send config request for device {device_serial}: {e}",
                "warning"
            )

    def set_config(self, device_serial: str, config: Dict[str, Any]) -> None:
        """Set device configuration."""
        try:
            op_id = str(uuid.uuid4())
            topic = f"LC1/{device_serial}/command/config"
            payload = {"opId": op_id, "config": config}
            self.client.publish(topic, json.dumps(payload), qos=0)
            logger.info(f"Sent config update to {topic} with opId {op_id}")
        except Exception as e:
            logger.error(f"Failed to send config update for {device_serial}: {e}")
            self.send_alert_email(
                "Config Update Failed",
                f"Failed to send config update for device {device_serial}: {e}",
                "critical"
            )

    def request_location(self, device_serial: str) -> None:
        """Request the current device location."""
        try:
            op_id = str(uuid.uuid4())
            topic = f"LC1/{device_serial}/command/location"
            payload = {"opId": op_id}
            self.client.publish(topic, json.dumps(payload), qos=0)
            logger.info(f"Sent location request to {topic} with opId {op_id}")
        except Exception as e:
            logger.error(f"Failed to send location request for {device_serial}: {e}")
            self.send_alert_email(
                "Location Request Failed",
                f"Failed to send location request for device {device_serial}: {e}",
                "warning"
            )

    def request_log(self, device_serial: str) -> None:
        try:
            op_id = str(uuid.uuid4())
            topic = f"LC1/{device_serial}/command/log"
            payload = {}
            self.client.publish(topic, json.dumps(payload), qos=0)
            logger.info(f"Sent log request to {topic} with opId {op_id}")
        except Exception as e:
            logger.error(f"Failed to send log request for {device_serial}: {e}")
            self.send_alert_email(
                "Log Request Failed",
                f"Failed to send log request for device {device_serial}: {e}",
                "warning"
            )

    def play_audio(self, device_serial: str, audio_message: str) -> None:
        """Send audio playback command."""
        valid_messages = ["message_1", "message_2"]
        if audio_message not in valid_messages:
            logger.error(f"Invalid audio message for {device_serial}: {audio_message}")
            self.send_alert_email(
                "Invalid Audio Message",
                f"Invalid audio message for device {device_serial}: {audio_message}",
                "warning"
            )
            return
        try:
            op_id = str(uuid.uuid4())
            topic = f"LC1/{device_serial}/command/play"
            payload = {"opId": op_id, "audioMessage": audio_message}
            self.client.publish(topic, json.dumps(payload), qos=0)
            logger.info(f"Sent audio play command to {topic} with opId {op_id}")
        except Exception as e:
            logger.error(f"Failed to send audio play command for {device_serial}: {e}")
            self.send_alert_email(
                "Audio Play Command Failed",
                f"Failed to send audio play command for device {device_serial}: {e}",
                "critical"
            )

    def insert_config_data(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Insert config data into the Events table"""
        conn = self.connect_db()
        if not conn:
            logger.error("Failed to connect to database")
            return
        try:
            cur = conn.cursor()
            server_id = self.get_server_id()
            if server_id is None:
                logger.error(f"Failed to get server_id for device {device_serial}")
                return
            payload_str = json.dumps(data, sort_keys=True)
            timestamp = data.get("timestamp")
            if not isinstance(timestamp, int) or timestamp < 0 or timestamp > 2**32-1:
                logger.error(f"Invalid timestamp for device {device_serial}: {timestamp}")
                self.send_alert_email(
                    "Invalid Timestamp",
                    f"Invalid timestamp for device {device_serial} on topic {topic}: {timestamp}",
                    "warning"
                )
                return
            timestamp = self.parse_timestamp(timestamp, device_serial, return_unix=False)
            if timestamp is None:
                logger.error(f"Invalid parsed timestamp for device {device_serial} on topic {topic}")
                return
            config = data.get("config")
            if not isinstance(config, dict):
                logger.error(f"Invalid config for device {device_serial}: {config}")
                self.send_alert_email(
                    "Invalid Config Data",
                    f"Invalid config for device {device_serial}: {config}",
                    "warning"
                )
                return
            for field in ["report_interval", "license_paid"]:
                if field not in config:
                    logger.warning(f"Missing config field {field} for device {device_serial}")
            if "phones" in config and not isinstance(config["phones"], list):
                logger.error(f"Invalid phones format for device {device_serial}: {config['phones']}")
                self.send_alert_email(
                    "Invalid Config Data",
                    f"Invalid phones format for device {device_serial}: {config['phones']}",
                    "warning"
                )
                return
            if "emails" in config and not isinstance(config["emails"], list):
                logger.error(f"Invalid emails format for device {device_serial}: {config['emails']}")
                self.send_alert_email(
                    "Invalid Config Data",
                    f"Invalid emails format for device {device_serial}: {config['emails']}",
                    "warning"
                )
                return
            if "services" in config and not isinstance(config["services"], dict):
                logger.error(f"Invalid services format for device {device_serial}: {config['services']}")
                self.send_alert_email(
                    "Invalid Config Data",
                    f"Invalid services format for device {device_serial}: {config['services']}",
                    "warning"
                )
                return
            logger.debug(f"Inserting config event for {device_serial}: config={config}")
            cur.execute(
                """
                INSERT INTO Events (
                    device_serial, server_id, operation_id, topic, payload,
                    event_timestamp, created_at, original_timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (device_serial, topic, event_timestamp)
                DO UPDATE SET
                    payload = EXCLUDED.payload,
                    created_at = EXCLUDED.created_at,
                    original_timestamp = EXCLUDED.original_timestamp
                """,
                (
                    device_serial[:50],
                    server_id,
                    "config",
                    topic[:255],
                    payload_str,
                    timestamp,
                    datetime.now(),
                    str(data.get("timestamp"))
                )
            )
            conn.commit()
            logger.info(f"Inserted config data for device {device_serial} on topic {topic}")
        except Exception as e:
            logger.error(f"Database error for device {device_serial}: {e}")
            conn.rollback()
            self.send_alert_email(
                "Database Error",
                f"Failed to insert config data for device {device_serial} on topic {topic}: {e}",
                "critical"
            )
        finally:
            cur.close()
            self.release_db(conn)

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        """MQTT connection callback (unchanged)"""
        if reason_code == 0:
            logger.info(f"Connected to MQTT broker at {MQTT_BROKER}:{MQTT_PORT} with client ID {MQTT_CLIENT_ID}")
            self.last_connection_time = datetime.now(timezone.utc)
            self.reconnect_count = 0
            for topic, qos in MQTT_TOPICS:
                client.subscribe(topic, qos)
                logger.info(f"Subscribed to {topic} with QoS {qos}")
            if self.reconnect_count > 0:
                self.send_alert_email("MQTT Connection Restored", f"Successfully reconnected to MQTT broker after {self.reconnect_count} attempts")
        else:
            logger.error(f"Connection failed with reason code {reason_code}")
            self.reconnect_count += 1
            if self.reconnect_count >= 5:
                self.send_alert_email("MQTT Connection Failed", f"Failed to connect to MQTT broker after {self.reconnect_count} attempts. Reason code: {reason_code}", "critical")

    def on_message(self, client, userdata, msg):
        """Handle incoming MQTT messages with fixed deduplication"""
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8')
            logger.debug(f"Received message on topic {topic}: {payload}")
            message_id = hashlib.sha256(f"{topic}:{payload}".encode()).hexdigest()
            with self.message_lock:
                if message_id in self.processed_messages:
                    logger.warning(f"Duplicate message detected for topic {topic}, message_id {message_id}")
                    return
                self.processed_messages.append(message_id)  # Use deque append

            if not payload:
                logger.warning(f"Empty payload received on topic {topic}")
                return
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

            device_serial = topic.split('/')[1]
            if not device_serial or len(topic.split('/')) < 3:
                logger.error(f"Invalid topic format: {topic}")
                self.send_alert_email(
                    "Invalid Topic",
                    f"Invalid topic format: {topic}",
                    "warning"
                )
                return

            # Adapt payload for insert_data
            event_type = topic.split('/')[2] if len(topic.split('/')) > 2 else None
            operation_id = topic.split('/')[3] if len(topic.split('/')) > 3 else None
            adapted_payload = {
                "device_serial": device_serial,
                "timestamp": data.get("timestamp", int(time.time() * 1000)),
                "event": {"type": operation_id}
            }
            if event_type == "event":
                if operation_id == "location":
                    adapted_payload["event"].update({
                        "latitude": data.get("latitude"),
                        "longitude": data.get("longitude")
                    })
                elif operation_id == "version":
                    adapted_payload["event"]["firmware"] = data.get("version")
                elif operation_id == "status":
                    adapted_payload["event"]["state"] = data.get("state")
                    adapted_payload["event"]["leds"] = [
                        {"type": key[4:], "state": value}
                        for key, value in data.items()
                        if key.startswith("led_") and value in {"Green", "Red", "Off"}
                    ]
                elif operation_id == "alert":
                    adapted_payload["event"]["code"] = data.get("message")
            elif event_type == "result":
                adapted_payload["result"] = {
                    "opId": data.get("opId"),
                    "value": data.get("result")
                }
            elif event_type == "command":
                adapted_payload["command"] = {
                    "opId": data.get("opId"),
                    "action": operation_id
                }

            self.insert_data(topic, adapted_payload)
            logger.info(f"Data processed for device {device_serial} from topic {topic}")
        except Exception as e:
            logger.error(f"Error processing message on topic {topic}: {e}")
            self.send_alert_email(
                "Message Processing Error",
                f"Failed to process message on topic {topic}: {e}",
                "critical"
            )

    def on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        """MQTT disconnect callback (unchanged)"""
        logger.warning(f"Disconnected from MQTT broker with reason code {reason_code}")
        if self.running:
            self.reconnect_count += 1
            self.send_alert_email("MQTT Disconnection", f"Disconnected from MQTT broker. Reason code: {reason_code}. Reconnect attempt: {self.reconnect_count}", "warning")


    def on_log(self, client, userdata, level, buf):
        """MQTT log callback (unchanged)"""
        logger.debug(f"MQTT log: {buf}")

    def connect_with_retry(self, max_retries=10, retry_delay=10, max_total_attempts=50):
        """Connect to MQTT broker with retry logic (unchanged)"""
        total_attempts = 0
        while self.running and total_attempts < max_total_attempts:
            for attempt in range(max_retries):
                try:
                    logger.info(f"Attempting to connect to MQTT broker (attempt {attempt + 1}/{max_retries})")
                    self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
                    return True
                except Exception as e:
                    total_attempts += 1
                    logger.error(f"MQTT connection attempt {attempt + 1} failed: {e}")
                    if total_attempts >= max_total_attempts:
                        logger.error("Max total attempts reached")
                        self.send_alert_email("MQTT Connection Failed", "Max total connection attempts reached", "critical")
                        return False
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay = min(retry_delay * 1.5, 60)
            time.sleep(30)
        return False

    def setup_mqtt_client(self):
        """Setup MQTT client with TLS and callbacks"""
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=MQTT_CLIENT_ID,
            protocol=mqtt.MQTTv5,
            transport="tcp"
        )
        self.client.tls_set(
            ca_certs=MQTT_CA_CERT,
            certfile=MQTT_CLIENT_CERT,
            keyfile=MQTT_CLIENT_KEY,
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLSv1_2  # Specify TLS version
        )
        if MQTT_USERNAME and MQTT_PASSWORD:
            self.client.username_pw_set(username=MQTT_USERNAME, password=MQTT_PASSWORD)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_log = self.on_log


    def run(self):
        """Run the MQTT service (unchanged)"""
        logger.info("Starting LOCACOEUR MQTT service...")
        threading.Timer(86400, self.backup_device_data).start()
        connected = False
        while self.running and not connected:
            connected = self.connect_with_retry()
            if not connected:
                logger.error("Failed to connect to MQTT broker. Retrying in 30 seconds...")
                time.sleep(30)
        if not connected:
            logger.error("Failed to connect to MQTT broker after all retries")
            return False
        try:
            self.client.loop_start()
            while self.running:
                try:
                    if not self.client.is_connected():
                        logger.warning("MQTT connection lost, attempting to reconnect...")
                        try:
                            self.client.loop_stop()
                        except Exception as e:
                            logger.warning(f"Error stopping MQTT loop: {e}")
                        connected = False
                        while self.running and not connected:
                            connected = self.connect_with_retry()
                            if not connected:
                                time.sleep(30)
                        if connected:
                            try:
                                self.client.loop_start()
                            except Exception as e:
                                logger.error(f"Error restarting MQTT loop: {e}")
                except Exception as e:
                    logger.error(f"Error checking MQTT connection: {e}")
                time.sleep(1)
        except Exception as e:
            logger.error(f"Main loop crashed: {e}")
            self.send_alert_email("Main Loop Crashed", f"Main loop crashed with error: {e}", "critical")
        finally:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except Exception as e:
                logger.warning(f"Error during final MQTT cleanup: {e}")
            try:
                self.db_pool.closeall()
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
