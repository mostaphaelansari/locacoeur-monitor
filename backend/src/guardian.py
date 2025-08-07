import dash
from dash import dcc, html, Input, Output, State, dash_table, callback_context, no_update
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import psycopg2
from psycopg2 import pool
import json
import uuid
import time
from datetime import datetime, timedelta, timezone
from decouple import config
import paho.mqtt.client as mqtt
import ssl
import threading
import logging
import hashlib
from collections import deque
from cachetools import TTLCache

# Add required imports at the top
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    "dbname": config("DB_NAME", default="mqtt_db"),
    "user": config("DB_USER", default="mqtt_user"),
    "password": config("DB_PASSWORD"),
    "host": config("DB_HOST", default="91.134.90.10"),
    "port": config("DB_PORT", default="5432")
}

# MQTT Configuration
MQTT_BROKER = config("MQTT_BROKER", default="mqtt.locacoeur.com")
MQTT_PORT = int(config("MQTT_PORT", default=8883))
MQTT_CA_CERT = config("MQTT_CA_CERT", default="../certs/ca.crt")
MQTT_CLIENT_CERT = config("MQTT_CLIENT_CERT", default="../certs/client.crt")
MQTT_CLIENT_KEY = config("MQTT_CLIENT_KEY", default="../certs/client.key")
MQTT_USERNAME = config("MQTT_USERNAME", default="locacoeur")
MQTT_PASSWORD = config("MQTT_PASSWORD", default=None)

# Constants
TOPIC_PREFIX = "LC1"
ALERT_CODES = {
    1: "Device is moving",
    2: "Power is cut",
    3: "Defibrillator fault",
    4: "Rapid temperature change",
    5: "Defibrillator hold range alert",
    6: "Critical temperature alert",
    7: "Lost connection",
    8: "Thermal regulation alert",
    9: "Device is removed"
}

RESULT_CODES = {
    0: "Success",
    -1: "Missing opId",
    -2: "Null object reference",
    -3: "Audio file not found"
}

# Email settings
EMAIL_CONFIG = {
    "smtp_server": config("SMTP_SERVER", default="ssl0.ovh.net"),
    "smtp_port": int(config("SMTP_PORT", default=587)),
    "username": config("SMTP_USERNAME", default="support@locacoeur.com"),
    "password": config("SMTP_PASSWORD", default=""),
    "from_email": config("SMTP_FROM_EMAIL", default="support@locacoeur.com"),
    "to_emails": config("SMTP_TO_EMAILS", default="alert@locacoeur.com").split(","),
    "enabled": config("SMTP_ENABLED", default=True, cast=bool)
}

VALID_LED_STATUSES = {"Green", "Red", "Off"}
VALID_AUDIO_MESSAGES = ["message_1", "message_2"]
VALID_POWER_SOURCES = ["ac", "battery"]
VALID_CONNECTIONS = ["lte", "wifi"]
VALID_DEFIBRILLATOR_CODES = [0, -1, -2, -3, -4, -5, -6, -7, -8, -9]

class DatabaseManager:
    def __init__(self):
        self.db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **DB_CONFIG)

    def get_connection(self):
        return self.db_pool.getconn()

    def release_connection(self, conn):
        self.db_pool.putconn(conn)

    def execute_query(self, query, params=None):
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(query, params)
            if query.strip().upper().startswith('SELECT'):
                result = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                return pd.DataFrame(result, columns=columns)
            else:
                conn.commit()
                return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise e
        finally:
            cur.close()
            self.release_connection(conn)

    def get_device_locations(self, device_serial=None, limit=100):
        """Get device locations from database - FIXED"""
        if device_serial:
            query = """
            SELECT device_serial, latitude, longitude, timestamp,
                   CASE
                       WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                       ELSE CURRENT_TIMESTAMP
                   END as location_time
            FROM device_data
            WHERE device_serial = %s
            AND latitude IS NOT NULL
            AND longitude IS NOT NULL
            ORDER BY timestamp DESC NULLS LAST
            LIMIT %s
            """
            params = (device_serial, limit)
        else:
            query = """
            SELECT DISTINCT ON (device_serial)
                device_serial, latitude, longitude, timestamp,
                CASE
                    WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                    ELSE CURRENT_TIMESTAMP
                END as location_time
            FROM device_data
            WHERE latitude IS NOT NULL
            AND longitude IS NOT NULL
            ORDER BY device_serial,
                     timestamp DESC NULLS LAST
            """
            params = None

        return self.execute_query(query, params)

    def get_service_status(self, device_serial):
        """Get monitoring and assistance service status - FIXED"""
        query = """
        SELECT payload->'config'->'services'->>'monitoring' as monitoring,
               payload->'config'->'services'->>'assistance' as assistance,
               CASE
                   WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                   ELSE CURRENT_TIMESTAMP
               END as created_at
        FROM device_data
        WHERE device_serial = %s
        AND topic LIKE %s
        AND payload->'config'->'services' IS NOT NULL
        ORDER BY CASE
                    WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                    ELSE CURRENT_TIMESTAMP
                 END DESC
        LIMIT 1
        """
        return self.execute_query(query, (device_serial, f"{TOPIC_PREFIX}/{device_serial}/event/config"))

    def get_device_alerts(self, device_serial=None, hours=24):
        """Get device alerts from the last N hours - FIXED"""
        base_query = """
        SELECT device_serial, alert_id, alert_message, timestamp,
               CASE
                   WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                   ELSE CURRENT_TIMESTAMP
               END as alert_time
        FROM device_data
        WHERE alert_id IS NOT NULL
        AND CASE
                WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                ELSE CURRENT_TIMESTAMP
            END >= NOW() - INTERVAL '{} hours'
        """.format(hours)

        if device_serial:
            query = base_query + " AND device_serial = %s ORDER BY alert_time DESC"
            params = (device_serial,)
        else:
            query = base_query + " ORDER BY alert_time DESC"
            params = None

        return self.execute_query(query, params)

    def get_firmware_versions(self):
        """Get firmware versions for all devices - FIXED"""
        query = """
        SELECT DISTINCT ON (device_serial)
            device_serial,
            payload->>'version' as firmware_version,
            CASE
                WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                ELSE CURRENT_TIMESTAMP
            END as created_at
        FROM device_data
        WHERE topic LIKE %s
        AND payload->>'version' IS NOT NULL
        ORDER BY device_serial,
                 CASE
                    WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                    ELSE CURRENT_TIMESTAMP
                 END DESC
        """
        return self.execute_query(query, (f"{TOPIC_PREFIX}/%/event/version",))

    def get_recent_device_data(self, hours=24):
        """Get recent device data with proper timestamp handling - NEW"""
        query = """
        SELECT DISTINCT ON (device_serial)
            device_serial, battery, connection, defibrillator, latitude, longitude,
            power_source, led_power, led_defibrillator, led_monitoring,
            led_assistance, led_mqtt, led_environmental, timestamp,
            CASE
                WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                ELSE CURRENT_TIMESTAMP
            END as created_at
        FROM device_data
        WHERE CASE
                WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                ELSE CURRENT_TIMESTAMP
              END >= NOW() - INTERVAL '{} hours'
        ORDER BY device_serial,
                 CASE
                    WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                    ELSE CURRENT_TIMESTAMP
                 END DESC
        """.format(hours)
        return self.execute_query(query)

    def get_commands_and_results(self, hours=24):
        """Get recent commands and results - FIXED"""
        query = """
        SELECT c.device_serial, c.operation_id, c.topic,
               c.created_at as command_time,
               r.result_status, r.result_message,
               r.created_at as result_time
        FROM Commands c
        LEFT JOIN Results r ON c.command_id = r.command_id
        WHERE c.created_at >= NOW() - INTERVAL '{} hours'
        ORDER BY c.created_at DESC
        LIMIT 50
        """.format(hours)
        return self.execute_query(query)

    def get_recent_alerts(self, hours=1):
        """Get recent critical alerts - FIXED"""
        query = """
        SELECT device_serial, alert_id, alert_message,
               CASE
                   WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                   ELSE CURRENT_TIMESTAMP
               END as alert_time
        FROM device_data
        WHERE alert_id IN (2, 3, 6, 7, 9)  -- Critical alerts: Power cut, Defibrillator fault, Critical temp, Lost connection, Device removed
        AND CASE
                WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                ELSE CURRENT_TIMESTAMP
            END >= NOW() - INTERVAL '{} hours'
        ORDER BY CASE
                    WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                    ELSE CURRENT_TIMESTAMP
                 END DESC
        LIMIT 5
        """.format(hours)
        return self.execute_query(query)

    def get_analytics_data(self, days=7):
        """Get data for analytics - FIXED"""
        query = """
        SELECT device_serial, battery, timestamp,
               CASE
                   WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                   ELSE CURRENT_TIMESTAMP
               END as created_at,
               EXTRACT(hour FROM
                   CASE
                       WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                       ELSE CURRENT_TIMESTAMP
                   END
               ) as hour_of_day,
               EXTRACT(dow FROM
                   CASE
                       WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                       ELSE CURRENT_TIMESTAMP
                   END
               ) as day_of_week
        FROM device_data
        WHERE battery IS NOT NULL
        AND CASE
                WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                ELSE CURRENT_TIMESTAMP
            END >= NOW() - INTERVAL '{} days'
        ORDER BY CASE
                   WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                   ELSE CURRENT_TIMESTAMP
                 END
        """.format(days)
        return self.execute_query(query)

class MQTTPublisher:
    def __init__(self):
        self.client = None
        self.command_cache = {}
        self.command_lock = threading.Lock()
        self.setup_client()
        self.connect()

    def setup_client(self):
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"dashboard-{uuid.uuid4()}",
            protocol=mqtt.MQTTv5
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
        except Exception as e:
            logger.error(f"TLS configuration failed: {e}")

        if MQTT_USERNAME and MQTT_PASSWORD:
            self.client.username_pw_set(username=MQTT_USERNAME, password=MQTT_PASSWORD)

    def connect(self):
        try:
            self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            self.client.loop_start()
            logger.info("MQTT Publisher connected")
            return True
        except Exception as e:
            logger.error(f"MQTT connection failed: {e}")
            return False

    def publish_command(self, device_serial, command_type, payload):
        """Publish command to device"""
        topic = f"{TOPIC_PREFIX}/{device_serial}/command/{command_type}"
        try:
            result = self.client.publish(topic, json.dumps(payload), qos=1)
            logger.info(f"Published command to {topic}: {payload}")

            # Store command for tracking
            with self.command_lock:
                self.command_cache[(device_serial, payload.get("opId"))] = {
                    "topic": topic,
                    "sent_time": datetime.now(timezone.utc),
                    "payload": payload
                }

            return result.rc == mqtt.MQTT_ERR_SUCCESS
        except Exception as e:
            logger.error(f"Failed to publish command: {e}")
            return False

    def request_config(self, device_serial):
        """Request device configuration"""
        op_id = str(uuid.uuid4())
        payload = {"opId": op_id}
        return self.publish_command(device_serial, "config/get", payload)

    def set_config(self, device_serial, config_data):
        """Set device configuration"""
        op_id = str(uuid.uuid4())
        payload = {"opId": op_id, "config": config_data}
        return self.publish_command(device_serial, "config", payload)

    def request_firmware_version(self, device_serial):
        """Request firmware version"""
        op_id = str(uuid.uuid4())
        payload = {"opId": op_id}
        return self.publish_command(device_serial, "version/get", payload)

    def update_firmware(self, device_serial, version, firmware_url=None):
        """Update device firmware"""
        op_id = str(uuid.uuid4())
        payload = {"opId": op_id, "version": version}
        if firmware_url:
            payload["url"] = firmware_url
        return self.publish_command(device_serial, "update", payload)

    def request_location(self, device_serial):
        """Request device location"""
        op_id = str(uuid.uuid4())
        payload = {"opId": op_id}
        return self.publish_command(device_serial, "location", payload)

    def play_audio(self, device_serial, audio_message):
        """Play audio message on device"""
        if audio_message not in VALID_AUDIO_MESSAGES:
            logger.error(f"Invalid audio message: {audio_message}")
            return False

        op_id = str(uuid.uuid4())
        payload = {"opId": op_id, "audioMessage": audio_message}
        return self.publish_command(device_serial, "play", payload)

    def request_logs(self, device_serial):
        """Request device logs"""
        op_id = str(uuid.uuid4())
        payload = {"opId": op_id}
        return self.publish_command(device_serial, "log/get", payload)

# Initialize components
db_manager = DatabaseManager()
mqtt_publisher = MQTTPublisher()

# Initialize Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP])
app.title = "NYXEOS MQTT Dashboard"

# Layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1([
                html.I(className="bi bi-heart-pulse me-2"),
                "NYXEOS MQTT Dashboard"
            ], className="text-center mb-4 text-primary"),
            html.Hr()
        ])
    ]),

    # Alert Banner
    html.Div(id="alert-banner", className="mb-3"),

    # Control Panel
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    html.I(className="bi bi-gear-fill me-2"),
                    "Device Control Panel"
                ]),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Select Device:"),
                            dcc.Dropdown(
                                id="device-selector",
                                placeholder="Select a device...",
                                style={"marginBottom": "10px"}
                            )
                        ], width=4),
                        dbc.Col([
                            dbc.Label("Command Type:"),
                            dcc.Dropdown(
                                id="command-type",
                                options=[
                                    {"label": "Get Config", "value": "config/get"},
                                    {"label": "Set Config", "value": "config"},
                                    {"label": "Get Version", "value": "version/get"},
                                    {"label": "Update Firmware", "value": "update"},
                                    {"label": "Get Location", "value": "location"},
                                    {"label": "Play Audio", "value": "play"},
                                    {"label": "Get Logs", "value": "log/get"}
                                ],
                                placeholder="Select command...",
                                style={"marginBottom": "10px"}
                            )
                        ], width=4),
                        dbc.Col([
                            dbc.Label("Quick Actions:"),
                            html.Div([
                                dbc.Button([
                                    html.I(className="bi bi-arrow-clockwise me-1"),
                                    "Refresh"
                                ], id="refresh-btn", color="secondary", size="sm", className="me-1"),
                                dbc.Button([
                                    html.I(className="bi bi-exclamation-triangle me-1"),
                                    "Alerts"
                                ], id="show-alerts-btn", color="warning", size="sm")
                            ])
                        ], width=4)
                    ]),
                    html.Br(),
                    dbc.Row([
                        dbc.Col([
                            dbc.Button([
                                html.I(className="bi bi-send me-1"),
                                "Execute Command"
                            ], id="execute-btn", color="primary", className="me-2"),
                            dbc.Button([
                                html.I(className="bi bi-geo-alt me-1"),
                                "Get All Locations"
                            ], id="get-locations-btn", color="info", className="me-2"),
                            dbc.Button([
                                html.I(className="bi bi-list-check me-1"),
                                "Check Services"
                            ], id="check-services-btn", color="success")
                        ])
                    ]),
                    html.Div(id="command-response", className="mt-3")
                ])
            ])
        ])
    ], className="mb-4"),

    # Device Status Cards
    dbc.Row([
        dbc.Col([
            html.H3([
                html.I(className="bi bi-activity me-2"),
                "Device Status Overview"
            ]),
            html.Div(id="device-status-cards")
        ])
    ], className="mb-4"),

    # Tabs for different views
    dbc.Tabs([
        dbc.Tab(label="Real-time Monitoring", tab_id="monitoring",
                tab_style={"padding": "10px"}, active_tab_style={"fontWeight": "bold"}),
        dbc.Tab(label="Device Data", tab_id="data",
                tab_style={"padding": "10px"}, active_tab_style={"fontWeight": "bold"}),
        dbc.Tab(label="Alerts & Events", tab_id="alerts",
                tab_style={"padding": "10px"}, active_tab_style={"fontWeight": "bold"}),
        dbc.Tab(label="Commands & Results", tab_id="commands",
                tab_style={"padding": "10px"}, active_tab_style={"fontWeight": "bold"}),
        dbc.Tab(label="Configuration", tab_id="config",
                tab_style={"padding": "10px"}, active_tab_style={"fontWeight": "bold"}),
        dbc.Tab(label="Location Tracking", tab_id="locations",
                tab_style={"padding": "10px"}, active_tab_style={"fontWeight": "bold"}),
        dbc.Tab(label="Service Management", tab_id="services",
                tab_style={"padding": "10px"}, active_tab_style={"fontWeight": "bold"}),
        dbc.Tab(label="Analytics", tab_id="analytics",
                tab_style={"padding": "10px"}, active_tab_style={"fontWeight": "bold"})
    ], id="main-tabs", active_tab="monitoring"),

    html.Div(id="tab-content", className="mt-4"),

    # Auto-refresh interval
    dcc.Interval(
        id='interval-component',
        interval=30*1000,  # 30 seconds
        n_intervals=0
    ),

    # Stores for data
    dcc.Store(id='device-data-store'),
    dcc.Store(id='selected-device-store'),
    dcc.Store(id='locations-store'),
    dcc.Store(id='services-store')

], fluid=True)

# Callbacks

@app.callback(
    Output('device-selector', 'options'),
    Input('interval-component', 'n_intervals')
)
def update_device_list(n):
    try:
        df = db_manager.execute_query("SELECT DISTINCT serial FROM Devices ORDER BY serial")
        options = [{"label": serial, "value": serial} for serial in df['serial']]
        return options
    except Exception as e:
        logger.error(f"Error updating device list: {e}")
        return []

# Correction pour gui.py - Fonction update_device_data ligne 314
# Remplacer la fonction existante par cette version corrigée

@app.callback(
    [Output('device-data-store', 'data'),
     Output('alert-banner', 'children')],
    [Input('interval-component', 'n_intervals'),
     Input('refresh-btn', 'n_clicks')]
)
def update_device_data(n_intervals, refresh_clicks):
    try:
        # Use the new method with proper timestamp handling
        df = db_manager.get_recent_device_data(hours=24)

        # Check for critical alerts
        alert_banner = None
        critical_devices = []

        for _, device in df.iterrows():
            # FIX: Gestion robuste des valeurs NULL pour battery
            battery_value = device.get('battery')

            # Convertir en numérique et gérer les valeurs NULL/invalides
            try:
                battery = float(battery_value) if battery_value is not None else 100
            except (ValueError, TypeError):
                battery = 100  # Valeur par défaut si conversion impossible

            # Vérifier batterie faible (seulement si valeur valide)
            if battery_value is not None and battery < 20:
                critical_devices.append(f"{device['device_serial']} (Battery: {battery:.0f}%)")

            # Vérifier problème défibrillateur
            if device.get('led_defibrillator') == 'Red':
                critical_devices.append(f"{device['device_serial']} (Defibrillator Issue)")

        if critical_devices:
            alert_banner = dbc.Alert([
                html.H4([html.I(className="bi bi-exclamation-triangle me-2"), "Critical Alerts"], className="alert-heading"),
                html.P(f"Issues detected on: {', '.join(critical_devices[:3])}" +
                      ("..." if len(critical_devices) > 3 else ""))
            ], color="danger", dismissable=True)

        return df.to_dict('records'), alert_banner

    except Exception as e:
        logger.error(f"Error updating device data: {e}")
        return [], None

@app.callback(
    Output('locations-store', 'data'),
    Input('get-locations-btn', 'n_clicks')
)
def update_locations_data(n_clicks):
    if not n_clicks:
        return []

    try:
        df = db_manager.get_device_locations()
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"Error getting locations: {e}")
        return []

@app.callback(
    Output('services-store', 'data'),
    Input('check-services-btn', 'n_clicks')
)
def update_services_data(n_clicks):
    if not n_clicks:
        return []

    try:
        # Get all devices
        devices_df = db_manager.execute_query("SELECT DISTINCT serial FROM Devices")
        services_data = []

        for _, row in devices_df.iterrows():
            device_serial = row['serial']
            service_df = db_manager.get_service_status(device_serial)

            if not service_df.empty:
                services_data.append({
                    'device_serial': device_serial,
                    'monitoring': service_df.iloc[0]['monitoring'],
                    'assistance': service_df.iloc[0]['assistance'],
                    'last_updated': service_df.iloc[0]['created_at']
                })
            else:
                services_data.append({
                    'device_serial': device_serial,
                    'monitoring': 'Unknown',
                    'assistance': 'Unknown',
                    'last_updated': None
                })

        return services_data
    except Exception as e:
        logger.error(f"Error getting services data: {e}")
        return []

@app.callback(
    Output('device-status-cards', 'children'),
    Input('device-data-store', 'data')
)
def update_status_cards(device_data):
    if not device_data:
        return html.P("No device data available")

    cards = []
    for device in device_data:
        try:
            # LED status indicators
            led_indicators = []
            led_types = ['power', 'defibrillator', 'monitoring', 'assistance', 'mqtt', 'environmental']

            for led_type in led_types:
                led_key = f'led_{led_type}'
                status = device.get(led_key, 'Off')
                if status == 'Green':
                    color, icon = 'success', 'bi-check-circle-fill'
                elif status == 'Red':
                    color, icon = 'danger', 'bi-x-circle-fill'
                else:
                    color, icon = 'secondary', 'bi-circle'

                led_indicators.append(
                    dbc.Badge([
                        html.I(className=f"bi {icon} me-1"),
                        f"{led_type.title()}: {status}"
                    ], color=color, className="me-1 mb-1")
                )

            # FIX: Battery level color and icon avec gestion NULL
            battery_raw = device.get('battery')

            # Conversion robuste avec gestion des valeurs NULL
            try:
                battery = float(battery_raw) if battery_raw is not None else 0
            except (ValueError, TypeError):
                battery = 0

            # Logique de couleur/icône selon batterie
            if battery is None or battery == 0:
                battery_color, battery_icon = 'secondary', 'bi-battery'
                battery_display = "N/A"
            elif battery < 20:
                battery_color, battery_icon = 'danger', 'bi-battery'
                battery_display = f"{battery:.0f}%"
            elif battery < 50:
                battery_color, battery_icon = 'warning', 'bi-battery-half'
                battery_display = f"{battery:.0f}%"
            else:
                battery_color, battery_icon = 'success', 'bi-battery-full'
                battery_display = f"{battery:.0f}%"

            # Last update time
            last_update = device.get('created_at')
            if last_update and pd.notna(last_update):
                try:
                    last_update_str = pd.to_datetime(last_update).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    last_update_str = 'Unknown'
            else:
                last_update_str = 'Unknown'

            card = dbc.Card([
                dbc.CardHeader([
                    html.H5([
                        html.I(className="bi bi-device-hdd me-2"),
                        f"Device {device['device_serial']}"
                    ], className="mb-0")
                ]),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.P([
                                html.I(className=f"bi {battery_icon} me-2"),
                                html.Strong("Battery: "),
                                dbc.Badge(battery_display, color=battery_color)
                            ]),
                            html.P([
                                html.I(className="bi bi-wifi me-2"),
                                html.Strong("Connection: "),
                                device.get('connection', 'Unknown')
                            ]),
                            html.P([
                                html.I(className="bi bi-power me-2"),
                                html.Strong("Power Source: "),
                                device.get('power_source', 'Unknown')
                            ]),
                            html.P([
                                html.I(className="bi bi-heart-pulse me-2"),
                                html.Strong("Defibrillator: "),
                                device.get('defibrillator', 'Unknown')
                            ])
                        ], width=6),
                        dbc.Col([
                            html.P([
                                html.I(className="bi bi-lightbulb me-2"),
                                html.Strong("LED Status:")
                            ], className="fw-bold"),
                            html.Div(led_indicators),
                            html.P([
                                html.I(className="bi bi-clock me-2"),
                                html.Strong("Last Update: "),
                                html.Small(last_update_str)
                            ], className="mt-2")
                        ], width=6)
                    ])
                ])
            ], className="mb-3")

            cards.append(dbc.Col(card, width=12, lg=6, xl=4))

        except Exception as e:
            logger.error(f"Error creating card for device {device.get('device_serial', 'Unknown')}: {e}")
            # Créer une carte d'erreur au lieu d'ignorer
            error_card = dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.I(className="bi bi-exclamation-triangle me-2", style={"color": "red"}),
                            html.Strong(f"Error loading device {device.get('device_serial', 'Unknown')}"),
                            html.Br(),
                            html.Small(f"Error: {str(e)}", className="text-muted")
                        ])
                    ])
                ], style={"border-left": "4px solid red"})
            ], width=12, lg=6, xl=4, className="mb-3")
            cards.append(error_card)
            continue

    return dbc.Row(cards) if cards else html.P("No valid device data to display")

@app.callback(
    Output('tab-content', 'children'),
    [Input('main-tabs', 'active_tab'),
     Input('device-data-store', 'data'),
     Input('locations-store', 'data'),
     Input('services-store', 'data')]
)
def render_tab_content(active_tab, device_data, locations_data, services_data):
    if active_tab == "monitoring":
        return render_monitoring_tab(device_data)
    elif active_tab == "data":
        return render_data_tab()
    elif active_tab == "alerts":
        return render_alerts_tab()
    elif active_tab == "commands":
        return render_commands_tab()
    elif active_tab == "config":
        return render_config_tab()
    elif active_tab == "locations":
        return render_locations_tab(locations_data)
    elif active_tab == "services":
        return render_services_tab(services_data)
    elif active_tab == "analytics":
        return render_analytics_tab()
    return html.Div("Select a tab")

def render_monitoring_tab(device_data):
    if not device_data:
        return html.P("No device data available for monitoring")

    df = pd.DataFrame(device_data)

    # FIX: Nettoyage des données battery avant utilisation
    if 'battery' in df.columns:
        # Convertir les valeurs battery en numérique, remplacer NULL par NaN
        df['battery'] = pd.to_numeric(df['battery'], errors='coerce')
        # Filtrer les lignes avec des valeurs battery valides pour le graphique
        df_battery_valid = df.dropna(subset=['battery'])
    else:
        df_battery_valid = pd.DataFrame()

    # Create battery level chart (seulement avec valeurs valides)
    if not df_battery_valid.empty:
        fig_battery = px.bar(
            df_battery_valid,
            x='device_serial',
            y='battery',
            title='Battery Levels by Device',
            color='battery',
            color_continuous_scale=['red', 'yellow', 'green'],
            labels={'device_serial': 'Device Serial', 'battery': 'Battery Level (%)'}
        )
        fig_battery.update_layout(height=400, showlegend=False)
    else:
        fig_battery = go.Figure()
        fig_battery.update_layout(title="No valid battery data available", height=400)

    # Create LED status summary (reste inchangé)
    led_cols = ['led_power', 'led_defibrillator', 'led_monitoring', 'led_assistance', 'led_mqtt', 'led_environmental']
    led_status_data = []

    for col in led_cols:
        if col in df.columns:
            status_counts = df[col].value_counts()
            for status, count in status_counts.items():
                led_status_data.append({
                    'LED_Type': col.replace('led_', '').title(),
                    'Status': status,
                    'Count': count
                })

    if led_status_data:
        led_df = pd.DataFrame(led_status_data)
        fig_led = px.bar(
            led_df,
            x='LED_Type',
            y='Count',
            color='Status',
            title='LED Status Distribution',
            color_discrete_map={'Green': 'green', 'Red': 'red', 'Off': 'gray'}
        )
        fig_led.update_layout(height=400)
    else:
        fig_led = go.Figure()
        fig_led.update_layout(title="No LED data available", height=400)

    # Device connectivity status (reste inchangé)
    connection_counts = df['connection'].value_counts() if 'connection' in df.columns else pd.Series()
    if not connection_counts.empty:
        fig_connection = px.pie(
            values=connection_counts.values,
            names=connection_counts.index,
            title='Device Connectivity Distribution'
        )
        fig_connection.update_layout(height=300)
    else:
        fig_connection = go.Figure()
        fig_connection.update_layout(title="No connection data available", height=300)

    # FIX: Statistiques avec gestion des valeurs NULL
    total_devices = len(df)

    # Compter batterie faible (seulement valeurs valides)
    low_battery_count = 0
    if 'battery' in df.columns:
        battery_valid = pd.to_numeric(df['battery'], errors='coerce')
        low_battery_count = len(battery_valid[(battery_valid < 20) & (battery_valid.notna())])

    # Compter dispositifs offline
    offline_count = len(df[df['led_mqtt'] == 'Red']) if 'led_mqtt' in df.columns else 0

    # Compter problèmes défibrillateur
    defib_issues_count = len(df[df['led_defibrillator'] == 'Red']) if 'led_defibrillator' in df.columns else 0

    return dbc.Row([
        dbc.Col([
            dcc.Graph(figure=fig_battery)
        ], width=6),
        dbc.Col([
            dcc.Graph(figure=fig_led)
        ], width=6),
        dbc.Col([
            dcc.Graph(figure=fig_connection)
        ], width=6),
        dbc.Col([
            html.H5("System Status Summary"),
            html.Div([
                dbc.Badge(f"Total Devices: {total_devices}", color="primary", className="me-2 mb-2"),
                dbc.Badge(f"Low Battery: {low_battery_count}",
                         color="danger" if low_battery_count > 0 else "success",
                         className="me-2 mb-2"),
                dbc.Badge(f"Offline: {offline_count}",
                         color="warning" if offline_count > 0 else "success",
                         className="me-2 mb-2"),
                dbc.Badge(f"Defibrillator Issues: {defib_issues_count}",
                         color="danger" if defib_issues_count > 0 else "success",
                         className="me-2 mb-2")
            ])
        ], width=6)
    ])

def render_data_tab():
    try:
        # Use the new method
        df = db_manager.get_recent_device_data(hours=24)

        if df.empty:
            return html.P("No recent device data available")

        # Convert timestamp for display
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # Format columns for better display
        display_columns = [
            {"name": "Device", "id": "device_serial"},
            {"name": "Battery (%)", "id": "battery"},
            {"name": "Connection", "id": "connection"},
            {"name": "Defibrillator", "id": "defibrillator"},
            {"name": "Latitude", "id": "latitude"},
            {"name": "Longitude", "id": "longitude"},
            {"name": "Power Source", "id": "power_source"},
            {"name": "LED Power", "id": "led_power"},
            {"name": "LED Defibrillator", "id": "led_defibrillator"},
            {"name": "LED Monitoring", "id": "led_monitoring"},
            {"name": "LED Assistance", "id": "led_assistance"},
            {"name": "LED MQTT", "id": "led_mqtt"},
            {"name": "LED Environmental", "id": "led_environmental"},
            {"name": "Timestamp", "id": "created_at"}
        ]

        return dash_table.DataTable(
            data=df.to_dict('records'),
            columns=display_columns,
            page_size=20,
            sort_action="native",
            filter_action="native",
            style_table={'overflowX': 'auto'},
            style_cell={
                'textAlign': 'left',
                'padding': '10px',
                'fontSize': '12px'
            },
            style_header={
                'backgroundColor': 'rgb(230, 230, 230)',
                'fontWeight': 'bold'
            },
            style_data_conditional=[
                {
                    'if': {
                        'filter_query': '{battery} < 20',
                        'column_id': 'battery'
                    },
                    'backgroundColor': '#ffcccc',
                    'color': 'black',
                },
                {
                    'if': {
                        'filter_query': '{led_defibrillator} = Red',
                        'column_id': 'led_defibrillator'
                    },
                    'backgroundColor': '#ffcccc',
                    'color': 'black',
                },
                {
                    'if': {
                        'filter_query': '{led_mqtt} = Red',
                        'column_id': 'led_mqtt'
                    },
                    'backgroundColor': '#ffe6cc',
                    'color': 'black',
                }
            ]
        )
    except Exception as e:
        return html.P(f"Error loading device data: {str(e)}")

def render_alerts_tab():
    try:
        # Use the new method
        df = db_manager.get_device_alerts(hours=24*7)  # Last 7 days

        if df.empty:
            return dbc.Row([
                dbc.Col([
                    dbc.Alert([
                        html.I(className="bi bi-check-circle me-2"),
                        "No recent alerts - All systems operating normally"
                    ], color="success")
                ])
            ])

        # Add alert description
        df['alert_description'] = df['alert_id'].map(ALERT_CODES)
        if 'alert_time' in df.columns:
            df['alert_time'] = pd.to_datetime(df['alert_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # Create alert frequency chart
        alert_counts = df['alert_description'].value_counts()
        fig_alerts = px.bar(
            x=alert_counts.index,
            y=alert_counts.values,
            title='Alert Frequency (Last 7 Days)',
            labels={'x': 'Alert Type', 'y': 'Count'},
            color=alert_counts.values,
            color_continuous_scale='Reds'
        )
        fig_alerts.update_layout(height=400, showlegend=False)
        fig_alerts.update_xaxes(tickangle=45)

        # Alert severity distribution
        critical_alerts = [2, 3, 6, 7, 9]  # Power cut, Defibrillator fault, Critical temp, Lost connection, Device removed
        df['severity'] = df['alert_id'].apply(lambda x: 'Critical' if x in critical_alerts else 'Warning')
        severity_counts = df['severity'].value_counts()

        fig_severity = px.pie(
            values=severity_counts.values,
            names=severity_counts.index,
            title='Alert Severity Distribution',
            color_discrete_map={'Critical': 'red', 'Warning': 'orange'}
        )
        fig_severity.update_layout(height=300)

        return dbc.Row([
            dbc.Col([
                dcc.Graph(figure=fig_alerts)
            ], width=8),
            dbc.Col([
                dcc.Graph(figure=fig_severity)
            ], width=4),
            dbc.Col([
                html.H4([
                    html.I(className="bi bi-exclamation-triangle me-2"),
                    "Recent Alerts"
                ]),
                dash_table.DataTable(
                    data=df.to_dict('records'),
                    columns=[
                        {"name": "Device", "id": "device_serial"},
                        {"name": "Alert ID", "id": "alert_id"},
                        {"name": "Description", "id": "alert_description"},
                        {"name": "Message", "id": "alert_message"},
                        {"name": "Severity", "id": "severity"},
                        {"name": "Timestamp", "id": "alert_time"}
                    ],
                    page_size=15,
                    sort_action="native",
                    style_cell={'textAlign': 'left', 'padding': '10px', 'fontSize': '12px'},
                    style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
                    style_data_conditional=[
                        {
                            'if': {'filter_query': '{severity} = Critical'},
                            'backgroundColor': '#ffcccc',
                            'color': 'black'
                        },
                        {
                            'if': {'filter_query': '{severity} = Warning'},
                            'backgroundColor': '#ffe6cc',
                            'color': 'black'
                        }
                    ]
                )
            ], width=12)
        ])
    except Exception as e:
        return html.P(f"Error loading alerts: {str(e)}")

def render_commands_tab():
    try:
        # Use the new method
        df = db_manager.get_commands_and_results(hours=24)

        if df.empty:
            return dbc.Row([
                dbc.Col([
                    dbc.Alert([
                        html.I(className="bi bi-info-circle me-2"),
                        "No recent commands executed"
                    ], color="info")
                ])
            ])

        # Format timestamps
        if 'command_time' in df.columns:
            df['command_time'] = pd.to_datetime(df['command_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        if 'result_time' in df.columns and not df['result_time'].isna().all():
            df['result_time'] = pd.to_datetime(df['result_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # Command success rate
        success_rate = len(df[df['result_status'] == '0']) / len(df[df['result_status'].notna()]) * 100 if len(df[df['result_status'].notna()]) > 0 else 0

        # Command type distribution
        df['command_type'] = df['topic'].str.split('/').str[-1]
        command_counts = df['command_type'].value_counts()

        fig_commands = px.bar(
            x=command_counts.index,
            y=command_counts.values,
            title='Command Type Distribution (Last 24 Hours)',
            labels={'x': 'Command Type', 'y': 'Count'}
        )
        fig_commands.update_layout(height=300)

        return dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4([
                            html.I(className="bi bi-graph-up me-2"),
                            "Command Statistics"
                        ]),
                        html.P(f"Total Commands: {len(df)}"),
                        html.P(f"Success Rate: {success_rate:.1f}%"),
                        html.P(f"Pending Results: {len(df[df['result_status'].isna()])}")
                    ])
                ])
            ], width=4),
            dbc.Col([
                dcc.Graph(figure=fig_commands)
            ], width=8),
            dbc.Col([
                html.H4([
                    html.I(className="bi bi-terminal me-2"),
                    "Recent Commands & Results"
                ]),
                dash_table.DataTable(
                    data=df.to_dict('records'),
                    columns=[
                        {"name": "Device", "id": "device_serial"},
                        {"name": "Operation", "id": "operation_id"},
                        {"name": "Command Time", "id": "command_time"},
                        {"name": "Result Status", "id": "result_status"},
                        {"name": "Result Message", "id": "result_message"},
                        {"name": "Result Time", "id": "result_time"}
                    ],
                    page_size=20,
                    sort_action="native",
                    style_cell={'textAlign': 'left', 'padding': '10px', 'fontSize': '12px'},
                    style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
                    style_data_conditional=[
                        {
                            'if': {'filter_query': '{result_status} != 0 && {result_status} != null'},
                            'backgroundColor': '#ffcccc',
                        },
                        {
                            'if': {'filter_query': '{result_status} = null'},
                            'backgroundColor': '#fff3cd',
                        }
                    ]
                )
            ], width=12)
        ])
    except Exception as e:
        return html.P(f"Error loading commands: {str(e)}")

def render_config_tab():
    return dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    html.I(className="bi bi-sliders me-2"),
                    "Device Configuration Management"
                ]),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Device Serial:"),
                            dcc.Dropdown(id="config-device-selector", placeholder="Select device...")
                        ], width=6),
                        dbc.Col([
                            dbc.Label("Configuration Template:"),
                            dcc.Dropdown(
                                id="config-template",
                                options=[
                                    {"label": "Standard Configuration", "value": "standard"},
                                    {"label": "High Performance", "value": "high_performance"},
                                    {"label": "Power Saving", "value": "power_saving"},
                                    {"label": "Custom", "value": "custom"}
                                ],
                                placeholder="Select template..."
                            )
                        ], width=6)
                    ]),
                    html.Hr(),
                    dbc.Row([
                        dbc.Col([
                            html.H5("Service Configuration"),
                            dbc.Label("Monitoring Service:"),
                            dbc.Switch(id="monitoring-switch", value=True, className="mb-3"),
                            dbc.Label("Assistance Service:"),
                            dbc.Switch(id="assistance-switch", value=True, className="mb-3")
                        ], width=6),
                        dbc.Col([
                            html.H5("System Settings"),
                            dbc.Label("Firmware Version:"),
                            dbc.Input(id="firmware-version", placeholder="e.g., 1.2.3", className="mb-3"),
                            dbc.Label("Audio Message:"),
                            dcc.Dropdown(
                                id="audio-message",
                                options=[
                                    {"label": "Message 1", "value": "message_1"},
                                    {"label": "Message 2", "value": "message_2"}
                                ],
                                placeholder="Select audio message...",
                                className="mb-3"
                            )
                        ], width=6)
                    ]),
                    html.Hr(),
                    dbc.Row([
                        dbc.Col([
                            dbc.Button([
                                html.I(className="bi bi-gear me-1"),
                                "Update Config"
                            ], id="update-config-btn", color="primary", className="me-2"),
                            dbc.Button([
                                html.I(className="bi bi-download me-1"),
                                "Update Firmware"
                            ], id="update-firmware-btn", color="warning", className="me-2"),
                            dbc.Button([
                                html.I(className="bi bi-volume-up me-1"),
                                "Play Audio"
                            ], id="play-audio-btn", color="info", className="me-2"),
                            dbc.Button([
                                html.I(className="bi bi-arrow-clockwise me-1"),
                                "Get Current Config"
                            ], id="get-config-btn", color="secondary")
                        ])
                    ]),
                    html.Div(id="config-response", className="mt-3")
                ])
            ])
        ], width=12)
    ])

def render_locations_tab(locations_data):
    if not locations_data:
        return dbc.Row([
            dbc.Col([
                dbc.Alert([
                    html.I(className="bi bi-geo-alt me-2"),
                    "No location data available. Click 'Get All Locations' to refresh."
                ], color="warning")
            ])
        ])

    df = pd.DataFrame(locations_data)

    # Create map visualization
    if not df.empty and 'latitude' in df.columns and 'longitude' in df.columns:
        fig_map = px.scatter_mapbox(
            df,
            lat="latitude",
            lon="longitude",
            color="device_serial",
            hover_name="device_serial",
            hover_data=["location_time"],
            title="Device Locations",
            zoom=10
        )
        fig_map.update_layout(
            mapbox_style="open-street-map",
            height=500,
            margin={"r":0,"t":30,"l":0,"b":0}
        )

        # Location history table
        display_df = df.copy()
        if 'location_time' in display_df.columns:
            display_df['location_time'] = pd.to_datetime(display_df['location_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

        return dbc.Row([
            dbc.Col([
                dcc.Graph(figure=fig_map)
            ], width=12),
            dbc.Col([
                html.H4([
                    html.I(className="bi bi-table me-2"),
                    "Location History"
                ]),
                dash_table.DataTable(
                    data=display_df.to_dict('records'),
                    columns=[
                        {"name": "Device", "id": "device_serial"},
                        {"name": "Latitude", "id": "latitude", "type": "numeric", "format": {"specifier": ".6f"}},
                        {"name": "Longitude", "id": "longitude", "type": "numeric", "format": {"specifier": ".6f"}},
                        {"name": "Location Time", "id": "location_time"}
                    ],
                    page_size=15,
                    sort_action="native",
                    style_cell={'textAlign': 'left', 'padding': '10px', 'fontSize': '12px'},
                    style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'}
                )
            ], width=12)
        ])
    else:
        return dbc.Row([
            dbc.Col([
                dbc.Alert([
                    html.I(className="bi bi-exclamation-triangle me-2"),
                    "Location data is incomplete or invalid."
                ], color="warning")
            ])
        ])

def render_services_tab(services_data):
    if not services_data:
        return dbc.Row([
            dbc.Col([
                dbc.Alert([
                    html.I(className="bi bi-info-circle me-2"),
                    "No service data available. Click 'Check Services' to refresh."
                ], color="info")
            ])
        ])

    df = pd.DataFrame(services_data)

    # Service status distribution
    monitoring_counts = df['monitoring'].value_counts()
    assistance_counts = df['assistance'].value_counts()

    fig_monitoring = px.pie(
        values=monitoring_counts.values,
        names=monitoring_counts.index,
        title='Monitoring Service Status',
        color_discrete_map={'true': 'green', 'false': 'red', 'Unknown': 'gray'}
    )
    fig_monitoring.update_layout(height=300)

    fig_assistance = px.pie(
        values=assistance_counts.values,
        names=assistance_counts.index,
        title='Assistance Service Status',
        color_discrete_map={'true': 'green', 'false': 'red', 'Unknown': 'gray'}
    )
    fig_assistance.update_layout(height=300)

    # Format last updated time
    display_df = df.copy()
    if 'last_updated' in display_df.columns:
        display_df['last_updated'] = pd.to_datetime(display_df['last_updated']).dt.strftime('%Y-%m-%d %H:%M:%S')

    return dbc.Row([
        dbc.Col([
            dcc.Graph(figure=fig_monitoring)
        ], width=6),
        dbc.Col([
            dcc.Graph(figure=fig_assistance)
        ], width=6),
        dbc.Col([
            html.H4([
                html.I(className="bi bi-list-check me-2"),
                "Service Status by Device"
            ]),
            dash_table.DataTable(
                data=display_df.to_dict('records'),
                columns=[
                    {"name": "Device", "id": "device_serial"},
                    {"name": "Monitoring", "id": "monitoring"},
                    {"name": "Assistance", "id": "assistance"},
                    {"name": "Last Updated", "id": "last_updated"}
                ],
                page_size=15,
                sort_action="native",
                style_cell={'textAlign': 'left', 'padding': '10px', 'fontSize': '12px'},
                style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
                style_data_conditional=[
                    {
                        'if': {
                            'filter_query': '{monitoring} = false',
                            'column_id': 'monitoring'
                        },
                        'backgroundColor': '#ffcccc',
                        'color': 'black',
                    },
                    {
                        'if': {
                            'filter_query': '{assistance} = false',
                            'column_id': 'assistance'
                        },
                        'backgroundColor': '#ffcccc',
                        'color': 'black',
                    },
                    {
                        'if': {
                            'filter_query': '{monitoring} = Unknown || {assistance} = Unknown'
                        },
                        'backgroundColor': '#f8f9fa',
                        'color': 'gray',
                    }
                ]
            )
        ], width=12)
    ])

def render_analytics_tab():
    try:
        # Use the new method
        df = db_manager.get_analytics_data(days=7)

        if df.empty:
            return html.P("No data available for analytics")

        # Battery trends over time
        df_battery_trend = df.groupby(['device_serial', 'created_at'])['battery'].mean().reset_index()
        fig_battery_trend = px.line(
            df_battery_trend,
            x='created_at',
            y='battery',
            color='device_serial',
            title='Battery Level Trends (Last 7 Days)',
            labels={'created_at': 'Time', 'battery': 'Battery Level (%)'}
        )
        fig_battery_trend.update_layout(height=400)

        # Average battery by hour of day
        hourly_battery = df.groupby('hour_of_day')['battery'].mean().reset_index()
        fig_hourly = px.bar(
            hourly_battery,
            x='hour_of_day',
            y='battery',
            title='Average Battery Level by Hour of Day',
            labels={'hour_of_day': 'Hour of Day', 'battery': 'Average Battery Level (%)'}
        )
        fig_hourly.update_layout(height=400)

        # Battery level distribution
        fig_battery_dist = px.histogram(
            df,
            x='battery',
            nbins=20,
            title='Battery Level Distribution',
            labels={'battery': 'Battery Level (%)', 'count': 'Frequency'}
        )
        fig_battery_dist.update_layout(height=300)

        # Day of week analysis
        day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
        weekly_battery = df.groupby('day_of_week')['battery'].mean().reset_index()
        weekly_battery['day_name'] = weekly_battery['day_of_week'].map(lambda x: day_names[int(x)])

        fig_weekly = px.bar(
            weekly_battery,
            x='day_name',
            y='battery',
            title='Average Battery Level by Day of Week',
            labels={'day_name': 'Day of Week', 'battery': 'Average Battery Level (%)'}
        )
        fig_weekly.update_layout(height=300)

        return dbc.Row([
            dbc.Col([
                dcc.Graph(figure=fig_battery_trend)
            ], width=12),
            dbc.Col([
                dcc.Graph(figure=fig_hourly)
            ], width=6),
            dbc.Col([
                dcc.Graph(figure=fig_weekly)
            ], width=6),
            dbc.Col([
                dcc.Graph(figure=fig_battery_dist)
            ], width=6),
            dbc.Col([
                html.H5("Analytics Summary"),
                html.Div([
                    html.P(f"Data Points Analyzed: {len(df):,}"),
                    html.P(f"Average Battery Level: {df['battery'].mean():.1f}%"),
                    html.P(f"Lowest Battery Reading: {df['battery'].min():.1f}%"),
                    html.P(f"Highest Battery Reading: {df['battery'].max():.1f}%"),
                    html.P(f"Devices Monitored: {df['device_serial'].nunique()}"),
                    html.P(f"Time Range: {df['created_at'].min().strftime('%Y-%m-%d')} to {df['created_at'].max().strftime('%Y-%m-%d')}")
                ])
            ], width=6)
        ])
    except Exception as e:
        return html.P(f"Error loading analytics: {str(e)}")

# Command execution callbacks
@app.callback(
    Output('command-response', 'children'),
    [Input('execute-btn', 'n_clicks')],
    [State('device-selector', 'value'),
     State('command-type', 'value')]
)
def execute_command(n_clicks, device_serial, command_type):
    if not n_clicks or not device_serial or not command_type:
        return ""

    try:
        op_id = str(uuid.uuid4())
        payload = {"opId": op_id}

        success = mqtt_publisher.publish_command(device_serial, command_type, payload)

        if success:
            return dbc.Alert([
                html.I(className="bi bi-check-circle me-2"),
                f"Command '{command_type}' sent to device {device_serial} with opId {op_id}"
            ], color="success", dismissable=True)
        else:
            return dbc.Alert([
                html.I(className="bi bi-x-circle me-2"),
                f"Failed to send command '{command_type}' to device {device_serial}"
            ], color="danger", dismissable=True)

    except Exception as e:
        return dbc.Alert([
            html.I(className="bi bi-exclamation-triangle me-2"),
            f"Error executing command: {str(e)}"
        ], color="danger", dismissable=True)

# Configuration callbacks - REMOVED DUPLICATE (handled by sync_config_device_selector below)

@app.callback(
    Output('config-response', 'children'),
    [Input('update-config-btn', 'n_clicks'),
     Input('update-firmware-btn', 'n_clicks'),
     Input('play-audio-btn', 'n_clicks'),
     Input('get-config-btn', 'n_clicks')],
    [State('config-device-selector', 'value'),
     State('monitoring-switch', 'value'),
     State('assistance-switch', 'value'),
     State('firmware-version', 'value'),
     State('audio-message', 'value')]
)
def handle_config_actions(config_clicks, firmware_clicks, audio_clicks, get_config_clicks,
                         device_serial, monitoring, assistance, firmware_version, audio_message):
    if not device_serial:
        return ""

    ctx = callback_context
    if not ctx.triggered:
        return ""

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    try:
        if button_id == 'update-config-btn' and config_clicks:
            config_data = {
                "services": {
                    "monitoring": str(monitoring).lower(),
                    "assistance": str(assistance).lower()
                }
            }
            success = mqtt_publisher.set_config(device_serial, config_data)

            if success:
                return dbc.Alert([
                    html.I(className="bi bi-check-circle me-2"),
                    "Configuration update sent successfully!"
                ], color="success", dismissable=True)
            else:
                return dbc.Alert([
                    html.I(className="bi bi-x-circle me-2"),
                    "Failed to send configuration update!"
                ], color="danger", dismissable=True)

        elif button_id == 'update-firmware-btn' and firmware_clicks and firmware_version:
            success = mqtt_publisher.update_firmware(device_serial, firmware_version)

            if success:
                return dbc.Alert([
                    html.I(className="bi bi-download me-2"),
                    f"Firmware update to version {firmware_version} initiated!"
                ], color="warning", dismissable=True)
            else:
                return dbc.Alert([
                    html.I(className="bi bi-x-circle me-2"),
                    "Failed to send firmware update command!"
                ], color="danger", dismissable=True)

        elif button_id == 'play-audio-btn' and audio_clicks and audio_message:
            success = mqtt_publisher.play_audio(device_serial, audio_message)

            if success:
                return dbc.Alert([
                    html.I(className="bi bi-volume-up me-2"),
                    f"Audio message '{audio_message}' sent to device!"
                ], color="info", dismissable=True)
            else:
                return dbc.Alert([
                    html.I(className="bi bi-x-circle me-2"),
                    "Failed to send audio command!"
                ], color="danger", dismissable=True)

        elif button_id == 'get-config-btn' and get_config_clicks:
            success = mqtt_publisher.request_config(device_serial)

            if success:
                return dbc.Alert([
                    html.I(className="bi bi-arrow-clockwise me-2"),
                    "Configuration request sent! Check the Commands tab for results."
                ], color="secondary", dismissable=True)
            else:
                return dbc.Alert([
                    html.I(className="bi bi-x-circle me-2"),
                    "Failed to send configuration request!"
                ], color="danger", dismissable=True)

    except Exception as e:
        return dbc.Alert([
            html.I(className="bi bi-exclamation-triangle me-2"),
            f"Error: {str(e)}"
        ], color="danger", dismissable=True)

    return ""

# Additional utility callbacks for enhanced functionality
@app.callback(
    Output('selected-device-store', 'data'),
    Input('device-selector', 'value')
)
def store_selected_device(device_serial):
    return device_serial

# Callback for template configuration loading
@app.callback(
    [Output('monitoring-switch', 'value'),
     Output('assistance-switch', 'value')],
    [Input('config-template', 'value')],
    prevent_initial_call=True
)
def load_config_template(template):
    if template == "standard":
        return True, True
    elif template == "high_performance":
        return True, True
    elif template == "power_saving":
        return False, False
    else:  # custom
        return True, True

# Enhanced location request callback
@app.callback(
    Output('locations-store', 'data', allow_duplicate=True),
    Input('get-locations-btn', 'n_clicks'),
    prevent_initial_call=True
)
def request_and_update_locations(n_clicks):
    if not n_clicks:
        return no_update

    try:
        # Get existing location data first
        df = db_manager.get_device_locations()
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"Error getting locations: {e}")
        return []

# Enhanced service checking callback
@app.callback(
    Output('services-store', 'data', allow_duplicate=True),
    Input('check-services-btn', 'n_clicks'),
    prevent_initial_call=True
)
def check_and_update_services(n_clicks):
    if not n_clicks:
        return no_update

    try:
        # Get all devices
        devices_df = db_manager.execute_query("SELECT DISTINCT serial FROM Devices")
        services_data = []

        for _, row in devices_df.iterrows():
            device_serial = row['serial']
            service_df = db_manager.get_service_status(device_serial)

            if not service_df.empty:
                services_data.append({
                    'device_serial': device_serial,
                    'monitoring': service_df.iloc[0]['monitoring'],
                    'assistance': service_df.iloc[0]['assistance'],
                    'last_updated': service_df.iloc[0]['created_at']
                })
            else:
                services_data.append({
                    'device_serial': device_serial,
                    'monitoring': 'Unknown',
                    'assistance': 'Unknown',
                    'last_updated': None
                })

        return services_data
    except Exception as e:
        logger.error(f"Error getting services data: {e}")
        return []

# Real-time alerts callback
@app.callback(
    Output('alert-banner', 'children', allow_duplicate=True),
    Input('show-alerts-btn', 'n_clicks'),
    prevent_initial_call=True
)
def show_recent_alerts(n_clicks):
    if not n_clicks:
        return None

    try:
        # Use the new method
        df = db_manager.get_recent_alerts(hours=1)

        if df.empty:
            return dbc.Alert([
                html.I(className="bi bi-check-circle me-2"),
                "No critical alerts in the last hour"
            ], color="success", dismissable=True)

        alert_items = []
        for _, alert in df.iterrows():
            alert_desc = ALERT_CODES.get(alert['alert_id'], f"Unknown alert {alert['alert_id']}")
            alert_time = pd.to_datetime(alert['alert_time']).strftime('%H:%M:%S')
            alert_items.append(
                html.Li(f"{alert['device_serial']}: {alert_desc} at {alert_time}")
            )

        return dbc.Alert([
            html.H4([
                html.I(className="bi bi-exclamation-triangle me-2"),
                "Recent Critical Alerts"
            ], className="alert-heading"),
            html.Ul(alert_items)
        ], color="danger", dismissable=True)

    except Exception as e:
        return dbc.Alert([
            html.I(className="bi bi-x-circle me-2"),
            f"Error loading alerts: {str(e)}"
        ], color="danger", dismissable=True)

# Additional helper functions for enhanced functionality
def get_device_summary():
    """Get a summary of all devices and their current status"""
    try:
        query = """
        SELECT
            COUNT(DISTINCT device_serial) as total_devices,
            COUNT(CASE WHEN battery < 20 THEN 1 END) as low_battery_devices,
            COUNT(CASE WHEN led_mqtt = 'Red' THEN 1 END) as offline_devices,
            COUNT(CASE WHEN led_defibrillator = 'Red' THEN 1 END) as defibrillator_issues,
            AVG(battery) as avg_battery
        FROM (
            SELECT DISTINCT ON (device_serial)
                device_serial, battery, led_mqtt, led_defibrillator
            FROM device_data
            WHERE CASE
                    WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                    ELSE CURRENT_TIMESTAMP
                  END >= NOW() - INTERVAL '1 hour'
            ORDER BY device_serial,
                     CASE
                        WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                        ELSE CURRENT_TIMESTAMP
                     END DESC
        ) latest_data
        """
        return db_manager.execute_query(query)
    except Exception as e:
        logger.error(f"Error getting device summary: {e}")
        return pd.DataFrame()

def get_firmware_status():
    """Get firmware version information for all devices"""
    try:
        query = """
        SELECT
            device_serial,
            payload->>'version' as firmware_version,
            CASE
                WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                ELSE CURRENT_TIMESTAMP
            END as created_at
        FROM device_data
        WHERE topic LIKE '%/event/version'
        AND payload->>'version' IS NOT NULL
        ORDER BY device_serial,
                 CASE
                    WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                    ELSE CURRENT_TIMESTAMP
                 END DESC
        """
        return db_manager.execute_query(query)
    except Exception as e:
        logger.error(f"Error getting firmware status: {e}")
        return pd.DataFrame()

# Enhanced error handling and logging
def log_tab_changes(active_tab):
    logger.info(f"User switched to tab: {active_tab}")
    return None

# Add a health check callback
@app.callback(
    Output('device-data-store', 'data', allow_duplicate=True),
    Input('interval-component', 'n_intervals'),
    prevent_initial_call=True
)
def health_check(n_intervals):
    """Perform periodic health checks every 5 minutes"""
    if n_intervals % 10 == 0:  # Every 10th interval (5 minutes with 30s intervals)
        try:
            # Check database connectivity
            test_query = "SELECT 1"
            db_manager.execute_query(test_query)

            # Check MQTT connectivity
            if not mqtt_publisher.client or not mqtt_publisher.client.is_connected():
                logger.warning("MQTT client is not connected - attempting reconnection")
                mqtt_publisher.connect()

            logger.info(f"Health check completed at interval {n_intervals}")

        except Exception as e:
            logger.error(f"Health check failed: {e}")

    return no_update

# Additional callback for handling device selection and command execution
@app.callback(
    [Output('command-type', 'disabled'),
     Output('execute-btn', 'disabled')],
    [Input('device-selector', 'value')]
)
def update_command_controls(device_serial):
    """Enable/disable command controls based on device selection"""
    disabled = device_serial is None
    return disabled, disabled

# Callback for real-time status updates on device cards
@app.callback(
    Output('device-status-cards', 'children', allow_duplicate=True),
    [Input('device-data-store', 'data'),
     Input('interval-component', 'n_intervals')],
    prevent_initial_call=True
)
def update_status_cards_realtime(device_data, n_intervals):
    """Update device status cards with real-time data"""
    return update_status_cards(device_data)

# Callback for updating device dropdown in config tab - FIXED
@app.callback(
    Output('config-device-selector', 'options'),
    Input('device-selector', 'options')
)
def sync_config_device_selector(device_options):
    """Sync device selector in config tab with main device selector"""
    return device_options if device_options else []

# Emergency shutdown callback (for development/debugging)
@app.callback(
    Output('alert-banner', 'children', allow_duplicate=True),
    Input('interval-component', 'n_intervals'),
    prevent_initial_call=True
)
def check_emergency_conditions(n_intervals):
    """Check for emergency conditions every interval"""
    try:
        # Check if any devices have critical issues
        df = db_manager.get_recent_device_data(hours=1)

        if df.empty:
            return dash.no_update

        # Count critical issues
        critical_battery = len(df[df['battery'] < 10]) if 'battery' in df.columns else 0
        defibrillator_failures = len(df[df['led_defibrillator'] == 'Red']) if 'led_defibrillator' in df.columns else 0
        offline_devices = len(df[df['led_mqtt'] == 'Red']) if 'led_mqtt' in df.columns else 0

        # If more than 50% of devices have issues, show emergency banner
        total_devices = len(df)
        critical_ratio = (critical_battery + defibrillator_failures + offline_devices) / (total_devices * 3) if total_devices > 0 else 0

        if critical_ratio > 0.5:
            return dbc.Alert([
                html.H4([
                    html.I(className="bi bi-exclamation-octagon me-2"),
                    "EMERGENCY: Multiple System Failures Detected"
                ], className="alert-heading"),
                html.P(f"Critical issues detected across {int(critical_ratio*100)}% of monitored systems. Immediate attention required."),
                html.Ul([
                    html.Li(f"Devices with critical battery: {critical_battery}"),
                    html.Li(f"Defibrillator failures: {defibrillator_failures}"),
                    html.Li(f"Offline devices: {offline_devices}")
                ])
            ], color="danger", dismissable=False)

    except Exception as e:
        logger.error(f"Error in emergency check: {e}")

    return dash.no_update

if __name__ == '__main__':
    try:
        logger.info("Starting NYXEOS MQTT Dashboard...")
        app.run(debug=True, host='0.0.0.0', port=8050)
    except Exception as e:
        logger.error(f"Failed to start dashboard: {e}")
        raise
