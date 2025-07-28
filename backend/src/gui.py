import dash
from dash import html, dcc, dash_table, ctx
from dash.dependencies import Input, Output, State
import plotly.express as px
import plotly.graph_objects as go
import dash_leaflet as dl
import pandas as pd
import json
from paho.mqtt.client import Client as MQTTClient
from decouple import config
import logging
import queue
import threading
import datetime
import uuid
import os
import sys
import dash_bootstrap_components as dbc
from collections import defaultdict, deque
import psutil
from dateutil.parser import parse as parse_date

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.utils import get_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('logs/gui.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# MQTT settings
MQTT_BROKER = config("MQTT_BROKER", default="mqtt.locacoeur.com")
MQTT_PORT = int(config("MQTT_PORT", default=8883))
MQTT_USERNAME = config("MQTT_USERNAME", default="locacoeur")
MQTT_PASSWORD = config("MQTT_PASSWORD")
MQTT_CA_CERT = config("MQTT_CA_CERT", default="certs/ca.crt")
MQTT_CLIENT_CERT = config("MQTT_CLIENT_CERT", default="certs/client.crt")
MQTT_CLIENT_KEY = config("MQTT_CLIENT_KEY", default="certs/client.key")

# Initialize Dash app with Bootstrap
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css",
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap"
    ],
    suppress_callback_exceptions=True
)

# Global state management
class DashboardState:
    def __init__(self):
        self.device_cache = {}
        self.alert_cache = deque(maxlen=1000)
        self.metrics_cache = defaultdict(lambda: deque(maxlen=100))
        self.last_update = datetime.datetime.now(datetime.timezone.utc)
        self.system_stats = {}

dashboard_state = DashboardState()

# MQTT setup
message_queue = queue.Queue()
device_status_cache = {}
mqtt_client = MQTTClient(client_id=f"gui-client-{uuid.uuid4()}", protocol=4)

def setup_mqtt():
    """Setup MQTT client with reconnection logic"""
    try:
        mqtt_client.tls_set(
            ca_certs=MQTT_CA_CERT,
            certfile=MQTT_CLIENT_CERT,
            keyfile=MQTT_CLIENT_KEY
        )
        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        mqtt_client.on_connect = on_connect
        mqtt_client.on_message = on_message
        mqtt_client.on_disconnect = on_disconnect
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
        mqtt_client.loop_start()
        logger.info("MQTT client initialized")
    except Exception as e:
        logger.error(f"MQTT setup failed: {e}")
        threading.Timer(5.0, setup_mqtt).start()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("GUI MQTT client connected")
        topics = [
            ("LC1/+/event/alert", 0),
            ("LC1/+/event/status", 0),
            ("LC1/+/event/location", 0),
            ("LC1/+/event/version", 0),
            ("LC1/+/event/log", 0),
            ("LC1/+/command/#", 0),
            ("LC1/+/result", 0)
        ]
        for topic, qos in topics:
            client.subscribe(topic, qos)
            logger.info(f"Subscribed to {topic}")
    else:
        logger.error(f"GUI MQTT connection failed: {rc}")
        threading.Timer(5.0, setup_mqtt).start()

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        message_info = {
            "topic": msg.topic,
            "payload": payload,
            "timestamp": datetime.datetime.now(datetime.timezone.utc)
        }
        message_queue.put(message_info)
        topic_parts = msg.topic.split("/")
        if len(topic_parts) >= 2:
            device_serial = topic_parts[1]
            event_type = topic_parts[3] if len(topic_parts) > 3 else "result"
            if device_serial not in device_status_cache:
                device_status_cache[device_serial] = {}
            device_status_cache[device_serial][event_type] = {
                "payload": payload,
                "timestamp": datetime.datetime.now(datetime.timezone.utc)
            }
            dashboard_state.device_cache[device_serial] = device_status_cache[device_serial]
            dashboard_state.last_update = datetime.datetime.now(datetime.timezone.utc)
            if event_type == "alert":
                dashboard_state.alert_cache.append(message_info)
        logger.debug(f"Received MQTT message: {msg.topic} - {payload}")
    except Exception as e:
        logger.error(f"Error processing MQTT message: {e}")

def on_disconnect(client, userdata, rc):
    logger.warning(f"MQTT client disconnected: {rc}")
    threading.Timer(5.0, setup_mqtt).start()

# Database queries
def get_enhanced_device_data():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT d.serial, dd.battery, dd.led_power, dd.led_defibrillator, 
                   dd.led_monitoring, dd.led_assistance, dd.led_mqtt, dd.led_environmental,
                   dd.latitude, dd.longitude, dd.received_at,
                   CASE 
                       WHEN dd.received_at > NOW() - INTERVAL '5 minutes' THEN 'Online'
                       WHEN dd.received_at > NOW() - INTERVAL '1 hour' THEN 'Recently Active'
                       ELSE 'Offline'
                   END as connection_status,
                   EXTRACT(EPOCH FROM (NOW() - dd.received_at))/60 as minutes_since_update
            FROM Devices d
            LEFT JOIN device_data dd ON d.serial = dd.device_serial
            WHERE dd.received_at = (
                SELECT MAX(received_at)
                FROM device_data
                WHERE device_serial = d.serial
            )
            ORDER BY dd.received_at DESC
        """)
        rows = cur.fetchall()
        logger.info(f"Retrieved {len(rows)} device records from database")
        enhanced_rows = []
        for row in rows:
            enhanced_row = list(row)
            serial = row[0]
            # Ensure Last_Update (index 10) is a datetime object
            if isinstance(row[10], str):
                try:
                    enhanced_row[10] = parse_date(row[10]).replace(tzinfo=datetime.timezone.utc)
                except:
                    logger.warning(f"Invalid datetime format for {serial}: {row[10]}")
                    enhanced_row[10] = None
            if serial in device_status_cache and 'status' in device_status_cache[serial]:
                mqtt_data = device_status_cache[serial]['status']['payload']
                logger.debug(f"Using MQTT data for {serial}: {mqtt_data}")
                enhanced_row[1] = mqtt_data.get('battery', row[1])
                enhanced_row[2] = mqtt_data.get('led_power', row[2]) or 'Off'
                enhanced_row[3] = mqtt_data.get('led_defibrillator', row[3]) or 'Off'
                enhanced_row[4] = mqtt_data.get('led_monitoring', row[4]) or 'Off'
                enhanced_row[5] = mqtt_data.get('led_assistance', row[5]) or 'Off'
                enhanced_row[6] = mqtt_data.get('led_mqtt', row[6]) or 'Off'
                enhanced_row[7] = mqtt_data.get('led_environmental', row[7]) or 'Off'
            else:
                logger.warning(f"No MQTT data for {serial}, using database values")
            firmware_version = 'Unknown'
            if serial in device_status_cache and 'version' in device_status_cache[serial]:
                firmware_version = device_status_cache[serial]['version']['payload'].get('firmware_version', 'Unknown')
            enhanced_row.append(firmware_version)
            health_score = calculate_device_health_score(enhanced_row)
            enhanced_row.append(health_score)
            enhanced_rows.append(enhanced_row)
        columns = ["Serial", "Battery", "Power_LED", "Defibrillator_LED", "Monitoring_LED",
                   "Assistance_LED", "MQTT_LED", "Environmental_LED", "Latitude", "Longitude", 
                   "Last_Update", "Connection_Status", "Minutes_Since_Update", "Firmware", "Health_Score"]
        df = pd.DataFrame(enhanced_rows, columns=columns)
        logger.info(f"Enhanced device data: {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Error fetching device data: {e}")
        return pd.DataFrame()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def calculate_device_health_score(device_row):
    """Calculate a health score for a device based on multiple factors"""
    score = 100
    battery = device_row[1]
    if pd.notnull(battery):
        if battery < 10:
            score -= 40
        elif battery < 20:
            score -= 30
        elif battery < 30:
            score -= 20
        elif battery < 50:
            score -= 10
    led_weights = {'Power_LED': 15, 'Defibrillator_LED': 20, 'MQTT_LED': 10, 'Environmental_LED': 5}
    for i, (led, weight) in enumerate(led_weights.items(), start=2):
        if device_row[i] == 'Red':
            score -= weight
    if device_row[11] == 'Offline':
        score -= 10
    elif device_row[11] == 'Recently Active':
        score -= 5
    return max(0, min(100, score))

def get_alert_data():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT device_serial, alert_id, alert_message, received_at
            FROM device_data
            WHERE alert_id IS NOT NULL
            ORDER BY received_at DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        df = pd.DataFrame(
            rows,
            columns=["Device_Serial", "Alert_ID", "Message", "Timestamp"]
        )
        logger.info(f"Retrieved {len(df)} alert records")
        return df
    except Exception as e:
        logger.error(f"Error fetching alerts: {e}")
        return pd.DataFrame()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def get_event_data():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT device_serial, topic, payload, created_at
            FROM Events
            ORDER BY created_at DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        events = []
        for row in rows:
            device_serial = row[0]
            if not device_serial:
                logger.warning(f"Missing device_serial in event: {row}")
                device_serial = "Unknown"
            topic_parts = row[1].split('/')
            event_type = topic_parts[-1] if len(topic_parts) > 3 else 'unknown'
            events.append({
                "Device_Serial": device_serial,
                "Event_Type": event_type.title(),
                "Details": json.dumps(row[2], indent=2)[:200] + "..." if len(json.dumps(row[2])) > 200 else json.dumps(row[2]),
                "Timestamp": row[3] if isinstance(row[3], datetime.datetime) else parse_date(row[3]) if isinstance(row[3], str) else None
            })
        df = pd.DataFrame(events)
        logger.info(f"Retrieved {len(df)} event records")
        return df
    except Exception as e:
        logger.error(f"Error fetching events: {e}")
        return pd.DataFrame()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def get_command_data():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT c.device_serial, c.operation_id, c.payload, c.created_at,
                   r.result_status, r.result_message
            FROM Commands c
            LEFT JOIN Results r ON c.command_id = r.command_id
            ORDER BY c.created_at DESC
            LIMIT 50
        """)
        rows = cur.fetchall()
        data = []
        for row in rows:
            # Convert payload to string
            payload_str = json.dumps(row[2], indent=2) if isinstance(row[2], dict) else str(row[2])
            # Ensure Status and Result are strings
            status_str = str(row[4]) if row[4] is not None else "Pending"
            result_str = str(row[5]) if row[5] is not None else ""
            data.append({
                "Device_Serial": row[0],
                "Operation_ID": row[1],
                "Payload": payload_str,
                "Sent_At": row[3] if isinstance(row[3], datetime.datetime) else parse_date(row[3]) if isinstance(row[3], str) else None,
                "Status": status_str,
                "Result": result_str
            })
        df = pd.DataFrame(data)
        logger.info(f"Retrieved {len(df)} command records")
        return df
    except Exception as e:
        logger.error(f"Error fetching commands: {e}")
        return pd.DataFrame()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def get_log_data():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT device_serial, topic, payload->'log_message' AS log_message, created_at
            FROM Events
            WHERE topic LIKE '%/event/log'
            ORDER BY created_at DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        df = pd.DataFrame(
            rows,
            columns=["Device_Serial", "Topic", "Log_Message", "Timestamp"]
        )
        logger.info(f"Retrieved {len(df)} log records")
        return df
    except Exception as e:
        logger.error(f"Error fetching logs: {e}")
        return pd.DataFrame()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def get_system_metrics():
    """Get comprehensive system metrics"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                COUNT(*) as total_devices,
                COUNT(CASE WHEN dd.battery < 20 THEN 1 END) as low_battery,
                COUNT(CASE WHEN dd.led_power = 'Red' THEN 1 END) as power_issues,
                COUNT(CASE WHEN dd.led_defibrillator = 'Red' THEN 1 END) as defibrillator_issues,
                AVG(dd.battery) as avg_battery,
                COUNT(CASE WHEN dd.received_at > NOW() - INTERVAL '5 minutes' THEN 1 END) as online_devices
            FROM Devices d
            LEFT JOIN device_data dd ON d.serial = dd.device_serial
            WHERE dd.received_at = (
                SELECT MAX(received_at) FROM device_data WHERE device_serial = d.serial
            )
        """)
        stats = cur.fetchone()
        cur.execute("""
            SELECT 
                COUNT(*) as total_alerts,
                COUNT(CASE WHEN received_at > NOW() - INTERVAL '1 hour' THEN 1 END) as recent_alerts,
                COUNT(CASE WHEN received_at > NOW() - INTERVAL '24 hours' THEN 1 END) as daily_alerts
            FROM device_data 
            WHERE alert_id IS NOT NULL
        """)
        alert_stats = cur.fetchone()
        cur.execute("""
            SELECT 
                COUNT(*) as total_events,
                COUNT(CASE WHEN created_at > NOW() - INTERVAL '1 hour' THEN 1 END) as hourly_events
            FROM Events
            WHERE created_at > NOW() - INTERVAL '24 hours'
        """)
        perf_stats = cur.fetchone()
        metrics = {
            'total_devices': stats[0] or 0,
            'low_battery': stats[1] or 0,
            'power_issues': stats[2] or 0,
            'defibrillator_issues': stats[3] or 0,
            'avg_battery': round(stats[4] or 0, 1),
            'online_devices': stats[5] or 0,
            'total_alerts': alert_stats[0] or 0,
            'recent_alerts': alert_stats[1] or 0,
            'daily_alerts': alert_stats[2] or 0,
            'total_events': perf_stats[0] or 0,
            'hourly_events': perf_stats[1] or 0,
            'system_uptime': get_system_uptime(),
            'memory_usage': psutil.virtual_memory().percent,
            'cpu_usage': psutil.cpu_percent()
        }
        logger.info(f"System metrics: {metrics}")
        return metrics
    except Exception as e:
        logger.error(f"Error fetching system metrics: {e}")
        return {}
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def get_system_uptime():
    """Get system uptime in hours"""
    try:
        uptime_seconds = datetime.datetime.now().timestamp() - psutil.boot_time()
        return round(uptime_seconds / 3600, 1)
    except Exception as e:
        logger.error(f"Error getting uptime: {e}")
        return 0

def get_device_analytics():
    """Get advanced device analytics"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                device_serial,
                DATE_TRUNC('hour', received_at) as hour,
                AVG(battery) as avg_battery,
                MIN(battery) as min_battery,
                MAX(battery) as max_battery
            FROM device_data 
            WHERE battery IS NOT NULL 
            AND received_at > NOW() - INTERVAL '24 hours'
            GROUP BY device_serial, hour
            ORDER BY hour DESC
        """)
        battery_trends = cur.fetchall()
        cur.execute("""
            SELECT 
                device_serial,
                alert_message,
                COUNT(*) as frequency,
                MAX(received_at) as last_occurrence
            FROM device_data 
            WHERE alert_id IS NOT NULL
            AND received_at > NOW() - INTERVAL '7 days'
            GROUP BY device_serial, alert_message
            ORDER BY frequency DESC
        """)
        alert_patterns = cur.fetchall()
        cur.execute("""
            SELECT 
                device_serial,
                latitude,
                longitude,
                received_at
            FROM device_data 
            WHERE latitude IS NOT NULL 
            AND longitude IS NOT NULL
            AND received_at > NOW() - INTERVAL '24 hours'
            ORDER BY received_at DESC
        """)
        locations = cur.fetchall()
        analytics = {
            'battery_trends': battery_trends,
            'alert_patterns': alert_patterns,
            'locations': locations
        }
        logger.info(f"Device analytics retrieved: {len(battery_trends)} trends, {len(alert_patterns)} patterns, {len(locations)} locations")
        return analytics
    except Exception as e:
        logger.error(f"Error fetching device analytics: {e}")
        return {}
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

# UI Components
def create_metric_card(title, value, subtitle, icon, color):
    """Create a professional metric card"""
    return dbc.Card([
        dbc.CardBody([
            html.Div([
                html.Div([
                    html.I(className=f"fas {icon} fa-2x", style={'color': color}),
                ], className="col-auto"),
                html.Div([
                    html.H3(value, className="mb-0 fw-bold", style={'color': color}),
                    html.P(title, className="mb-0 text-muted small"),
                    html.Small(subtitle, className="text-muted")
                ], className="col")
            ], className="row align-items-center")
        ])
    ], className="h-100 border-0 shadow-sm", style={'borderLeft': f'4px solid {color}'})

def create_status_indicator(status, size="small"):
    """Create a status indicator with color coding"""
    colors = {
        'Online': '#28a745',
        'Recently Active': '#ffc107', 
        'Offline': '#dc3545',
        'Green': '#28a745',
        'Red': '#dc3545',
        'Off': '#6c757d'
    }
    size_map = {'small': '8px', 'medium': '12px', 'large': '16px'}
    return html.Span(
        className="d-inline-block rounded-circle",
        style={
            'width': size_map.get(size, '8px'),
            'height': size_map.get(size, '8px'),
            'backgroundColor': colors.get(status, '#6c757d'),
            'marginRight': '5px'
        }
    )

# Layout
app.layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.Div([
                html.Div([
                    html.Img(src="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='%23e74c3c'%3E%3Cpath d='M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z'/%3E%3C/svg%3E", 
                             className="me-3", style={'width': '40px', 'height': '40px'}),
                    html.Div([
                        html.H2("Locacoeur Command Center", className="mb-0 fw-bold text-dark"),
                        html.P("Advanced Medical Device Management Platform", className="mb-0 text-muted small")
                    ])
                ], className="d-flex align-items-center"),
                html.Div([
                    html.Div(id='system-status', className="d-flex align-items-center"),
                    html.Small(id='last-update', className="text-muted ms-3")
                ], className="d-flex align-items-center")
            ], className="d-flex justify-content-between align-items-center py-3")
        ])
    ], className="mb-4"),
    
    # System Metrics Row
    dbc.Row([
        dbc.Col([
            html.Div(id='metrics-cards')
        ])
    ], className="mb-4"),
    
    # Main Content Tabs
    dbc.Tabs([
        dbc.Tab(label="🏠 Dashboard Overview", tab_id="overview", 
                active_label_style={"color": "#007bff", "font-weight": "bold"}),
        dbc.Tab(label="🔧 Device Management", tab_id="devices",
                active_label_style={"color": "#007bff", "font-weight": "bold"}),
        dbc.Tab(label="📊 Analytics & Insights", tab_id="analytics",
                active_label_style={"color": "#007bff", "font-weight": "bold"}),
        dbc.Tab(label="🚨 Alerts & Monitoring", tab_id="alerts",
                active_label_style={"color": "#007bff", "font-weight": "bold"}),
        dbc.Tab(label="⚡ Real-time Operations", tab_id="operations",
                active_label_style={"color": "#007bff", "font-weight": "bold"}),
        dbc.Tab(label="🛠️ System Configuration", tab_id="config",
                active_label_style={"color": "#007bff", "font-weight": "bold"}),
        dbc.Tab(label="📜 Logs & Diagnostics", tab_id="logs",
                active_label_style={"color": "#007bff", "font-weight": "bold"})
    ], id="main-tabs", active_tab="overview", className="mb-4"),
    
    # Tab Content
    html.Div(id='tab-content', className="min-vh-50"),
    
    # Auto-refresh interval and stores
    dcc.Interval(id='interval-component', interval=3000, n_intervals=0),
    dcc.Store(id='device-data-store'),
    dcc.Store(id='metrics-store')
], fluid=True, className="py-3", style={'backgroundColor': '#f8f9fa', 'minHeight': '100vh'})

# Callbacks
@app.callback(
    [Output('metrics-cards', 'children'),
     Output('system-status', 'children'),
     Output('last-update', 'children'),
     Output('metrics-store', 'data')],
    Input('interval-component', 'n_intervals')
)
def update_system_metrics(n):
    metrics = get_system_metrics()
    dashboard_state.system_stats = metrics
    cards = dbc.Row([
        dbc.Col([
            create_metric_card(
                "Total Devices", 
                str(metrics.get('total_devices', 0)),
                f"{metrics.get('online_devices', 0)} online",
                "fa-heartbeat",
                "#007bff"
            )
        ], width=2),
        dbc.Col([
            create_metric_card(
                "System Health",
                f"{100 - metrics.get('power_issues', 0) - metrics.get('defibrillator_issues', 0)}%",
                f"{metrics.get('power_issues', 0)} issues detected",
                "fa-shield-alt",
                "#28a745" if metrics.get('power_issues', 0) == 0 else "#dc3545"
            )
        ], width=2),
        dbc.Col([
            create_metric_card(
                "Avg Battery",
                f"{metrics.get('avg_battery', 0)}%",
                f"{metrics.get('low_battery', 0)} devices low",
                "fa-battery-three-quarters",
                "#28a745" if metrics.get('avg_battery', 0) > 50 else "#ffc107"
            )
        ], width=2),
        dbc.Col([
            create_metric_card(
                "Active Alerts",
                str(metrics.get('recent_alerts', 0)),
                f"{metrics.get('daily_alerts', 0)} today",
                "fa-exclamation-triangle",
                "#dc3545" if metrics.get('recent_alerts', 0) > 0 else "#28a745"
            )
        ], width=2),
        dbc.Col([
            create_metric_card(
                "Events/Hour",
                str(metrics.get('hourly_events', 0)),
                f"{metrics.get('total_events', 0)} total today",
                "fa-chart-line",
                "#6f42c1"
            )
        ], width=2),
        dbc.Col([
            create_metric_card(
                "System Load",
                f"{metrics.get('cpu_usage', 0):.1f}%",
                f"Memory: {metrics.get('memory_usage', 0):.1f}%",
                "fa-server",
                "#17a2b8"
            )
        ], width=2)
    ])
    mqtt_status = "Connected" if mqtt_client.is_connected() else "Disconnected"
    status_color = "#28a745" if mqtt_client.is_connected() else "#dc3545"
    status = [
        create_status_indicator(mqtt_status, "medium"),
        html.Span(f"MQTT {mqtt_status}", className="me-3"),
        html.Span(f"Uptime: {metrics.get('system_uptime', 0)}h", className="text-muted small")
    ]
    last_update_text = f"Last updated: {datetime.datetime.now(datetime.timezone.utc).astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}"
    return cards, status, last_update_text, metrics

@app.callback(
    Output('device-data-store', 'data'),
    Input('interval-component', 'n_intervals')
)
def update_device_data(n):
    if n % 20 == 0:  # Clear cache every 20 intervals (~60 seconds)
        dashboard_state.device_cache.clear()
        device_status_cache.clear()
        logger.info("Cleared device caches")
    df = get_enhanced_device_data()
    if df.empty:
        logger.warning("No device data retrieved")
        return []
    logger.info(f"Updated device-data-store with {len(df)} records")
    return df.to_dict('records')

@app.callback(
    Output('tab-content', 'children'),
    [Input('main-tabs', 'active_tab'),
     Input('device-data-store', 'data'),
     Input('metrics-store', 'data')]
)
def render_tab_content(active_tab, device_data, metrics_data):
    logger.info(f"Rendering tab: {active_tab}, device_data: {len(device_data) if device_data else 0} records, metrics_data: {metrics_data}")
    try:
        if active_tab == "overview":
            return render_overview_tab(device_data, metrics_data)
        elif active_tab == "devices":
            return render_devices_tab(device_data)
        elif active_tab == "analytics":
            return render_analytics_tab(device_data)
        elif active_tab == "alerts":
            return render_alerts_tab(device_data)
        elif active_tab == "operations":
            return render_operations_tab(device_data)
        elif active_tab == "config":
            return render_config_tab()
        elif active_tab == "logs":
            return render_logs_tab()
        return html.Div("Select a tab to view content")
    except Exception as e:
        logger.error(f"Error rendering tab {active_tab}: {str(e)}")
        return dbc.Alert(f"Error rendering tab: {str(e)}", color="danger")

def render_overview_tab(device_data, metrics_data):
    if not device_data:
        logger.warning("No device data for overview tab")
        return dbc.Alert("No device data available", color="warning")
    df = pd.DataFrame(device_data)
    logger.info(f"Overview tab data: {df[['Serial', 'Battery', 'Health_Score']].to_dict()}")
    health_fig = px.histogram(
        df, x='Health_Score', nbins=20,
        title="Device Health Score Distribution",
        color_discrete_sequence=['#007bff']
    )
    health_fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_family="Inter"
    )
    location_df = df.dropna(subset=['Latitude', 'Longitude'])
    if not location_df.empty:
        map_component = dl.Map([
            dl.TileLayer(url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"),
            *[
                dl.CircleMarker(
                    center=[row['Latitude'], row['Longitude']],
                    radius=8,
                    color='#007bff' if row['Connection_Status'] == 'Online' else '#dc3545',
                    fillOpacity=0.7,
                    children=[
                        dl.Tooltip(f"""
                        Device: {row['Serial']}
                        Status: {row['Connection_Status']}
                        Battery: {row['Battery']}%
                        Health: {row['Health_Score']}/100
                        """)
                    ]
                ) for _, row in location_df.iterrows()
            ]
        ], center=[48.8566, 2.3522], zoom=6, style={'height': '400px'})
    else:
        map_component = dbc.Alert("No location data available", color="info")
    battery_fig = go.Figure()
    battery_colors = ['#dc3545' if b < 20 else '#ffc107' if b < 50 else '#28a745' 
                     for b in df['Battery'].fillna(0)]
    battery_fig.add_trace(go.Bar(
        x=df['Serial'],
        y=df['Battery'].fillna(0),
        marker_color=battery_colors,
        text=[f'{int(b)}%' for b in df['Battery'].fillna(0)],
        textposition='auto',
        name='Battery Level'
    ))
    battery_fig.update_layout(
        title="Device Battery Levels",
        xaxis_title="Device Serial",
        yaxis_title="Battery (%)",
        yaxis_range=[0, 100],
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_family="Inter"
    )
    status_counts = df['Connection_Status'].value_counts()
    status_fig = px.pie(
        values=status_counts.values,
        names=status_counts.index,
        title="Device Connection Status",
        color_discrete_map={
            'Online': '#28a745',
            'Recently Active': '#ffc107',
            'Offline': '#dc3545'
        }
    )
    status_fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        font_family="Inter"
    )
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Device Health Distribution", className="mb-0")),
                    dbc.CardBody([
                        dcc.Graph(figure=health_fig, config={'displayModeBar': False})
                    ])
                ], className="shadow-sm")
            ], width=6),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Connection Status", className="mb-0")),
                    dbc.CardBody([
                        dcc.Graph(figure=status_fig, config={'displayModeBar': False})
                    ])
                ], className="shadow-sm")
            ], width=6)
        ], className="mb-4"),
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Geographic Distribution", className="mb-0")),
                    dbc.CardBody([
                        map_component
                    ])
                ], className="shadow-sm")
            ], width=7),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Battery Levels", className="mb-0")),
                    dbc.CardBody([
                        dcc.Graph(figure=battery_fig, config={'displayModeBar': False})
                    ])
                ], className="shadow-sm")
            ], width=5)
        ])
    ], fluid=True)

def render_devices_tab(device_data):
    if not device_data:
        logger.warning("No device data for devices tab")
        return dbc.Alert("No device data available", color="warning")
    df = pd.DataFrame(device_data)
    logger.info(f"Rendering devices tab with {len(df)} records")
    table_data = []
    for _, row in df.iterrows():
        # Handle Last_Update as datetime or string
        last_update = row['Last_Update']
        if isinstance(last_update, str):
            try:
                last_update = parse_date(last_update).replace(tzinfo=datetime.timezone.utc)
            except:
                logger.warning(f"Invalid datetime format for Last_Update: {last_update}")
                last_update = None
        table_row = {
            'Serial': row['Serial'],
            'Status': row['Connection_Status'],
            'Battery': f"{int(row['Battery']) if pd.notnull(row['Battery']) else 'N/A'}%",
            'Health_Score': f"{int(row['Health_Score']) if pd.notnull(row['Health_Score']) else 'N/A'}/100",
            'Power_LED': row['Power_LED'],
            'Defibrillator_LED': row['Defibrillator_LED'],
            'Monitoring_LED': row['Monitoring_LED'],
            'Assistance_LED': row['Assistance_LED'],
            'MQTT_LED': row['MQTT_LED'],
            'Environmental_LED': row['Environmental_LED'],
            'Firmware': row['Firmware'],
            'Last_Update': last_update.strftime('%Y-%m-%d %H:%M:%S %Z') if last_update else 'N/A',
            'Minutes_Since_Update': f"{int(row['Minutes_Since_Update']) if pd.notnull(row['Minutes_Since_Update']) else 'N/A'} min"
        }
        table_data.append(table_row)
    columns = [
        {'name': ['Status', ''], 'id': 'Status', 'type': 'text', 
         'presentation': 'markdown', 'format': {'cellTemplate': '[![status]({Status})](#)'}},
        {'name': 'Serial', 'id': 'Serial', 'type': 'text'},
        {'name': 'Battery', 'id': 'Battery', 'type': 'text'},
        {'name': 'Health Score', 'id': 'Health_Score', 'type': 'text'},
        {'name': 'Power LED', 'id': 'Power_LED', 'type': 'text'},
        {'name': 'Defibrillator LED', 'id': 'Defibrillator_LED', 'type': 'text'},
        {'name': 'Monitoring LED', 'id': 'Monitoring_LED', 'type': 'text'},
        {'name': 'Assistance LED', 'id': 'Assistance_LED', 'type': 'text'},
        {'name': 'MQTT LED', 'id': 'MQTT_LED', 'type': 'text'},
        {'name': 'Environmental LED', 'id': 'Environmental_LED', 'type': 'text'},
        {'name': 'Firmware', 'id': 'Firmware', 'type': 'text'},
        {'name': 'Last Update', 'id': 'Last_Update', 'type': 'text'},  # Changed to text to avoid datetime issues
        {'name': 'Time Since Update', 'id': 'Minutes_Since_Update', 'type': 'text'}
    ]
    style_data_conditional = [
        {
            'if': {'filter_query': '{Battery} contains "N/A" || {Battery} contains "<20"'},
            'backgroundColor': '#fef2f2',
            'color': '#dc2626'
        },
        {
            'if': {'filter_query': '{Health_Score} contains "<50"'},
            'backgroundColor': '#fff3cd',
            'color': '#856404'
        },
        {
            'if': {'filter_query': '{Power_LED} = Red || {Defibrillator_LED} = Red || {MQTT_LED} = Red'},
            'backgroundColor': '#fef2f2',
            'color': '#dc2626'
        },
        {
            'if': {'filter_query': '{Status} = Offline'},
            'backgroundColor': '#f8d7da',
            'color': '#721c24'
        }
    ]
    return dbc.Container([
        dbc.Card([
            dbc.CardHeader([
                html.H5("Device Management", className="mb-0"),
                html.Div([
                    dcc.Input(
                        id='search-device',
                        placeholder='Search devices...',
                        className="form-control me-2",
                        style={'width': '200px', 'display': 'inline-block'}
                    ),
                    dcc.Dropdown(
                        id='status-filter',
                        options=[
                            {'label': 'All Status', 'value': 'all'},
                            {'label': 'Online', 'value': 'Online'},
                            {'label': 'Recently Active', 'value': 'Recently Active'},
                            {'label': 'Offline', 'value': 'Offline'}
                        ],
                        value='all',
                        clearable=False,
                        style={'width': '200px', 'display': 'inline-block', 'verticalAlign': 'middle'},
                        className="me-2"
                    ),
                    dbc.Button(
                        "Export CSV",
                        id='export-devices',
                        color="primary",
                        size="sm",
                        className="ms-2"
                    )
                ], className="d-flex align-items-center float-end")
            ]),
            dbc.CardBody([
                dash_table.DataTable(
                    id='device-table',
                    columns=columns,
                    data=table_data,
                    style_table={'overflowX': 'auto'},
                    style_cell={
                        'textAlign': 'center',
                        'padding': '10px',
                        'fontFamily': 'Inter',
                        'fontSize': '14px'
                    },
                    style_header={
                        'backgroundColor': '#f1f3f5',
                        'fontWeight': 'bold'
                    },
                    style_data_conditional=style_data_conditional,
                    sort_action='native',
                    filter_action='native',
                    page_size=10,
                    export_format='csv',
                    export_headers='display'
                )
            ])
        ], className="shadow-sm")
    ], fluid=True)

def render_analytics_tab(device_data):
    analytics = get_device_analytics()
    if not analytics or not device_data:
        logger.warning("No analytics data for analytics tab")
        return dbc.Alert("No analytics data available", color="warning")
    
    battery_trends = pd.DataFrame(
        analytics['battery_trends'],
        columns=['Device_Serial', 'Hour', 'Avg_Battery', 'Min_Battery', 'Max_Battery']
    )
    
    battery_fig = go.Figure()
    for device in battery_trends['Device_Serial'].unique():
        device_data = battery_trends[battery_trends['Device_Serial'] == device]
        battery_fig.add_trace(go.Scatter(
            x=device_data['Hour'],
            y=device_data['Avg_Battery'],
            mode='lines+markers',
            name=device
        ))
    battery_fig.update_layout(
        title="Battery Level Trends (Last 24 Hours)",
        xaxis_title="Time",
        yaxis_title="Battery (%)",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_family="Inter"
    )
    
    alert_patterns = pd.DataFrame(
        analytics['alert_patterns'],
        columns=['Device_Serial', 'Alert_Message', 'Frequency', 'Last_Occurrence']
    )
    
    alert_fig = px.bar(
        alert_patterns,
        x='Device_Serial',
        y='Frequency',
        color='Alert_Message',
        title="Alert Patterns (Last 7 Days)",
        barmode='group'
    )
    alert_fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_family="Inter"
    )
    
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Battery Trends", className="mb-0")),
                    dbc.CardBody([
                        dcc.Graph(figure=battery_fig, config={'displayModeBar': False})
                    ])
                ], className="shadow-sm")
            ], width=6),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Alert Patterns", className="mb-0")),
                    dbc.CardBody([
                        dcc.Graph(figure=alert_fig, config={'displayModeBar': False})
                    ])
                ], className="shadow-sm")
            ], width=6)
        ])
    ], fluid=True)

def render_alerts_tab(device_data):
    alert_df = get_alert_data()
    event_df = get_event_data()
    if alert_df.empty and event_df.empty:
        logger.warning("No alerts or events for alerts tab")
        return dbc.Alert("No alerts or events available", color="warning")
    
    return dbc.Container([
        dbc.Card([
            dbc.CardHeader([
                html.H5("Recent Alerts", className="mb-0"),
                dbc.Button(
                    "Export Alerts CSV",
                    id='export-alerts',
                    color="primary",
                    size="sm",
                    className="float-end"
                )
            ]),
            dbc.CardBody([
                dash_table.DataTable(
                    id='alert-table',
                    columns=[
                        {'name': 'Device Serial', 'id': 'Device_Serial', 'type': 'text'},
                        {'name': 'Alert ID', 'id': 'Alert_ID', 'type': 'numeric'},
                        {'name': 'Message', 'id': 'Message', 'type': 'text'},
                        {'name': 'Timestamp', 'id': 'Timestamp', 'type': 'datetime'}
                    ],
                    data=alert_df.to_dict('records'),
                    style_cell={'textAlign': 'left', 'padding': '10px', 'fontFamily': 'Inter'},
                    style_header={'backgroundColor': '#f1f3f5', 'fontWeight': 'bold'},
                    style_data_conditional=[
                        {'if': {'filter_query': '{Message} contains "Low battery"'}, 
                         'backgroundColor': '#fef2f2', 'color': '#dc2626'}
                    ],
                    sort_action='native',
                    filter_action='native',
                    page_size=15,
                    export_format='csv'
                )
            ])
        ], className="shadow-sm mb-4"),
        dbc.Card([
            dbc.CardHeader(html.H5("System Events", className="mb-0")),
            dbc.CardBody([
                dash_table.DataTable(
                    id='events-table',
                    columns=[
                        {'name': 'Device Serial', 'id': 'Device_Serial', 'type': 'text'},
                        {'name': 'Event Type', 'id': 'Event_Type', 'type': 'text'},
                        {'name': 'Details', 'id': 'Details', 'type': 'text'},
                        {'name': 'Timestamp', 'id': 'Timestamp', 'type': 'datetime'}
                    ],
                    data=event_df.to_dict('records'),
                    style_cell={'textAlign': 'left', 'padding': '10px', 'fontFamily': 'Inter'},
                    style_header={'backgroundColor': '#f1f3f5', 'fontWeight': 'bold'},
                    sort_action='native',
                    filter_action='native',
                    page_size=15
                )
            ])
        ], className="shadow-sm")
    ], fluid=True)

def render_operations_tab(device_data):
    df = pd.DataFrame(device_data) if device_data else pd.DataFrame()
    command_df = get_command_data()
    logger.info(f"Rendering operations tab with {len(df)} devices, {len(command_df)} commands")
    return dbc.Container([
        dbc.Card([
            dbc.CardHeader(html.H5("Send Command to Device", className="mb-0")),
            dbc.CardBody([
                html.Label('Device Serial:', className="form-label"),
                dcc.Dropdown(
                    id='device-serial',
                    options=[{'label': serial, 'value': serial} for serial in df['Serial']] if not df.empty else [{'label': 'TEST-001', 'value': 'TEST-001'}],
                    value=df['Serial'][0] if not df.empty else 'TEST-001',
                    className="form-select mb-3"
                ),
                html.Label('Operation:', className="form-label"),
                dcc.Dropdown(
                    id='operation',
                    options=[
                        {'label': 'Get Configuration', 'value': 'config'},
                        {'label': 'Set Configuration', 'value': 'config_set'},
                        {'label': 'Get Version', 'value': 'version'},
                        {'label': 'Get Location', 'value': 'location'},
                        {'label': 'Get Logs', 'value': 'log'},
                        {'label': 'Firmware Update', 'value': 'update'},
                        {'label': 'Play Audio', 'value': 'play'}
                    ],
                    value='config',
                    className="form-select mb-3"
                ),
                html.Label('QoS:', className="form-label"),
                dcc.Dropdown(
                    id='qos',
                    options=[
                        {'label': 'At most once (0)', 'value': 0},
                        {'label': 'At least once (1)', 'value': 1},
                        {'label': 'Exactly once (2)', 'value': 2}
                    ],
                    value=0,
                    className="form-select mb-3"
                ),
                html.Label('Command Payload (JSON):', className="form-label"),
                dcc.Textarea(
                    id='payload',
                    value=json.dumps({
                        "operation_id": str(uuid.uuid4()),
                        "timestamp": int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
                    }, indent=2),
                    className="form-control mb-3",
                    style={'height': '150px', 'fontFamily': 'monospace'}
                ),
                html.Div([
                    dbc.Button('Send Command', id='send-command', color="primary", n_clicks=0, className="me-2"),
                    dbc.Button('Generate New Operation ID', id='generate-id', color="success", n_clicks=0)
                ], className="d-flex"),
                html.Div(id='command-output', className="mt-3")
            ])
        ], className="shadow-sm mb-4"),
        dbc.Card([
            dbc.CardHeader([
                html.H5("Command History", className="mb-0"),
                dbc.Button(
                    "Export Commands CSV",
                    id='export-commands',
                    color="primary",
                    size="sm",
                    className="float-end"
                )
            ]),
            dbc.CardBody([
                dash_table.DataTable(
                    id='command-table',
                    columns=[
                        {'name': 'Device Serial', 'id': 'Device_Serial', 'type': 'text'},
                        {'name': 'Operation ID', 'id': 'Operation_ID', 'type': 'text'},
                        {'name': 'Payload', 'id': 'Payload', 'type': 'text'},
                        {'name': 'Sent At', 'id': 'Sent_At', 'type': 'datetime'},
                        {'name': 'Status', 'id': 'Status', 'type': 'text'},
                        {'name': 'Result', 'id': 'Result', 'type': 'text'}
                    ],
                    data=command_df.to_dict('records'),
                    style_cell={'textAlign': 'left', 'padding': '10px', 'fontFamily': 'Inter'},
                    style_header={'backgroundColor': '#f1f3f5', 'fontWeight': 'bold'},
                    sort_action='native',
                    filter_action='native',
                    page_size=15,
                    export_format='csv'
                )
            ])
        ], className="shadow-sm")
    ], fluid=True)

def render_config_tab():
    device_data = get_enhanced_device_data()
    logger.info(f"Rendering config tab with {len(device_data)} devices")
    return dbc.Container([
        dbc.Card([
            dbc.CardHeader(html.H5("Device Configuration", className="mb-0")),
            dbc.CardBody([
                html.Label('Device Serial:', className="form-label"),
                dcc.Dropdown(
                    id='config-device',
                    options=[{'label': serial, 'value': serial} for serial in device_data['Serial']] if not device_data.empty else [{'label': 'TEST-001', 'value': 'TEST-001'}],
                    value=device_data['Serial'][0] if not device_data.empty else 'TEST-001',
                    className="form-select mb-3"
                ),
                html.Label('Monitoring Interval (seconds):', className="form-label"),
                dcc.Input(
                    id='monitoring-interval',
                    type='number',
                    value=30,
                    className="form-control mb-3"
                ),
                html.Label('Battery Alert Threshold (%):', className="form-label"),
                dcc.Input(
                    id='battery-threshold',
                    type='number',
                    value=20,
                    min=5,
                    max=50,
                    className="form-control mb-3"
                ),
                dbc.Button('Apply Configuration', id='apply-config', color="primary", n_clicks=0),
                html.Div(id='config-output', className="mt-3")
            ])
        ], className="shadow-sm")
    ], fluid=True)

def render_logs_tab():
    log_df = get_log_data()
    if log_df.empty:
        logger.warning("No log data for logs tab")
        return dbc.Alert("No log data available", color="warning")
    
    return dbc.Container([
        dbc.Card([
            dbc.CardHeader([
                html.H5("Logs & Diagnostics", className="mb-0"),
                dbc.Button(
                    "Export Logs CSV",
                    id='export-logs',
                    color="primary",
                    size="sm",
                    className="float-end"
                )
            ]),
            dbc.CardBody([
                dash_table.DataTable(
                    id='log-table',
                    columns=[
                        {'name': 'Device Serial', 'id': 'Device_Serial', 'type': 'text'},
                        {'name': 'Log Message', 'id': 'Log_Message', 'type': 'text'},
                        {'name': 'Timestamp', 'id': 'Timestamp', 'type': 'datetime'}
                    ],
                    data=log_df.to_dict('records'),
                    style_cell={'textAlign': 'left', 'padding': '10px', 'fontFamily': 'Inter'},
                    style_header={'backgroundColor': '#f1f3f5', 'fontWeight': 'bold'},
                    sort_action='native',
                    filter_action='native',
                    page_size=20,
                    export_format='csv'
                )
            ])
        ], className="shadow-sm")
    ], fluid=True)

@app.callback(
    Output('device-table', 'data'),
    [Input('search-device', 'value'),
     Input('status-filter', 'value'),
     Input('interval-component', 'n_intervals')],
    State('device-data-store', 'data')
)
def filter_device_table(search, status, n, device_data):
    if not device_data:
        logger.warning("No device data for device table")
        return []
    df = pd.DataFrame(device_data)
    logger.info(f"Filtering device table: {len(df)} records, search={search}, status={status}")
    if search:
        df = df[df['Serial'].str.contains(search, case=False, na=False)]
    if status != 'all':
        df = df[df['Connection_Status'] == status]
    table_data = []
    for _, row in df.iterrows():
        # Handle Last_Update as datetime or string
        last_update = row['Last_Update']
        if isinstance(last_update, str):
            try:
                last_update = parse_date(last_update).replace(tzinfo=datetime.timezone.utc)
            except:
                logger.warning(f"Invalid datetime format for Last_Update: {last_update}")
                last_update = None
        table_row = {
            'Serial': row['Serial'],
            'Status': row['Connection_Status'],
            'Battery': f"{int(row['Battery']) if pd.notnull(row['Battery']) else 'N/A'}%",
            'Health_Score': f"{int(row['Health_Score']) if pd.notnull(row['Health_Score']) else 'N/A'}/100",
            'Power_LED': row['Power_LED'],
            'Defibrillator_LED': row['Defibrillator_LED'],
            'Monitoring_LED': row['Monitoring_LED'],
            'Assistance_LED': row['Assistance_LED'],
            'MQTT_LED': row['MQTT_LED'],
            'Environmental_LED': row['Environmental_LED'],
            'Firmware': row['Firmware'],
            'Last_Update': last_update.strftime('%Y-%m-%d %H:%M:%S %Z') if last_update else 'N/A',
            'Minutes_Since_Update': f"{int(row['Minutes_Since_Update']) if pd.notnull(row['Minutes_Since_Update']) else 'N/A'} min"
        }
        table_data.append(table_row)
    logger.info(f"Device table filtered to {len(table_data)} records")
    return table_data

@app.callback(
    Output('payload', 'value'),
    Input('generate-id', 'n_clicks'),
    State('payload', 'value')
)
def generate_new_operation_id(n_clicks, current_payload):
    if n_clicks > 0:
        try:
            payload_dict = json.loads(current_payload)
            payload_dict['operation_id'] = str(uuid.uuid4())
            payload_dict['timestamp'] = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
            logger.info("Generated new operation ID")
            return json.dumps(payload_dict, indent=2)
        except Exception as e:
            logger.error(f"Error generating operation ID: {e}")
            return json.dumps({
                'operation_id': str(uuid.uuid4()),
                'timestamp': int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
            }, indent=2)
    return current_payload

@app.callback(
    Output('command-output', 'children'),
    Input('send-command', 'n_clicks'),
    [State('device-serial', 'value'), State('operation', 'value'), 
     State('payload', 'value'), State('qos', 'value')]
)
def send_enhanced_command(n_clicks, serial, operation, payload, qos):
    if n_clicks > 0:
        try:
            payload_dict = json.loads(payload)
            if 'operation_id' not in payload_dict:
                payload_dict['operation_id'] = str(uuid.uuid4())
            if 'timestamp' not in payload_dict:
                payload_dict['timestamp'] = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
            topic = f"LC1/{serial}/command/{operation}" if operation == 'config_set' else f"LC1/{serial}/command/{operation}/get"
            mqtt_client.publish(topic, json.dumps(payload_dict), qos=int(qos))
            try:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO Commands (device_serial, server_id, operation_id, topic, message_id, payload, created_at, is_get)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        serial[:50],
                        1,
                        payload_dict['operation_id'][:50],
                        topic[:255],
                        payload_dict['operation_id'][:50],
                        json.dumps(payload_dict),
                        datetime.datetime.now(datetime.timezone.utc),
                        operation != 'config_set'
                    )
                )
                conn.commit()
                logger.info(f"Stored command in database: {topic}, operation_id={payload_dict['operation_id']}")
            except Exception as e:
                logger.error(f"Error storing command: {e}")
            finally:
                if 'cur' in locals():
                    cur.close()
                if 'conn' in locals():
                    conn.close()
            logger.info(f"Sent command to {topic}: {payload_dict['operation_id']}")
            return dbc.Alert([
                html.P('✅ Command sent successfully!'),
                html.P(f'Topic: {topic}'),
                html.P(f'Operation ID: {payload_dict["operation_id"]}'),
                html.P(f'QoS: {qos}')
            ], color="success")
        except json.JSONDecodeError:
            logger.error("Invalid JSON payload in command")
            return dbc.Alert('❌ Invalid JSON payload', color="danger")
        except Exception as e:
            logger.error(f"Error sending command: {e}")
            return dbc.Alert(f'❌ Error sending command: {str(e)}', color="danger")
    return ''

@app.callback(
    Output('config-output', 'children'),
    Input('apply-config', 'n_clicks'),
    [State('config-device', 'value'), State('monitoring-interval', 'value'), 
     State('battery-threshold', 'value')]
)
def apply_configuration(n_clicks, serial, interval, threshold):
    if n_clicks > 0:
        try:
            payload = {
                'operation_id': str(uuid.uuid4()),
                'timestamp': int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000),
                'monitoring_interval': int(interval),
                'alert_thresholds': {'battery': int(threshold)}
            }
            topic = f'LC1/{serial}/command/config_set'
            mqtt_client.publish(topic, json.dumps(payload), qos=1)
            try:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO Commands (device_serial, server_id, operation_id, topic, message_id, payload, created_at, is_get)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        serial[:50],
                        1,
                        payload['operation_id'][:50],
                        topic[:255],
                        payload['operation_id'][:50],
                        json.dumps(payload),
                        datetime.datetime.now(datetime.timezone.utc),
                        False
                    )
                )
                conn.commit()
                logger.info(f"Stored configuration in database: {topic}, operation_id={payload['operation_id']}")
            except Exception as e:
                logger.error(f"Error storing config: {e}")
            finally:
                if 'cur' in locals():
                    cur.close()
                if 'conn' in locals():
                    conn.close()
            logger.info(f"Sent configuration to {topic}")
            return dbc.Alert([
                html.P('✅ Configuration sent successfully!'),
                html.P(f'Device: {serial}'),
                html.P(f'Monitoring Interval: {interval}s'),
                html.P(f'Battery Threshold: {threshold}%')
            ], color="success")
        except Exception as e:
            logger.error(f"Error sending config: {e}")
            return dbc.Alert(f'❌ Error sending configuration: {str(e)}', color="danger")
    return ''

@app.callback(
    [Output('alert-table', 'data'), Output('events-table', 'data')],
    Input('interval-component', 'n_intervals'),
    State('alert-table', 'data'),
    State('events-table', 'data')
)
def update_alerts_and_events(n, current_alerts, current_events):
    alert_df = get_alert_data()
    existing_events = pd.DataFrame(current_events) if current_events else pd.DataFrame(columns=["Device_Serial", "Event_Type", "Details", "Timestamp"])
    new_events = []
    while not message_queue.empty():
        update = message_queue.get()
        topic = update['topic']
        payload = update['payload']
        serial = topic.split('/')[1] if len(topic.split('/')) > 1 else "Unknown"
        timestamp = update['timestamp']
        if isinstance(timestamp, str):
            try:
                timestamp = parse_date(timestamp).replace(tzinfo=datetime.timezone.utc)
            except:
                logger.warning(f"Invalid timestamp in MQTT message: {timestamp}")
                timestamp = datetime.datetime.now(datetime.timezone.utc)
        if topic.endswith('/event/alert'):
            new_alert = {
                'Device_Serial': serial,
                'Alert_ID': payload.get('id', 'N/A'),
                'Message': payload.get('message', 'Unknown'),
                'Timestamp': timestamp
            }
            alert_df = pd.concat([pd.DataFrame([new_alert]), alert_df], ignore_index=True)
        elif '/event/' in topic:
            event_type = topic.split('/')[-1]
            new_events.append({
                'Device_Serial': serial,
                'Event_Type': event_type.title(),
                'Details': json.dumps(payload, indent=2)[:200] + '...' if len(json.dumps(payload)) > 200 else json.dumps(payload),
                'Timestamp': timestamp
            })
    new_events_df = pd.DataFrame(new_events)
    events_df = pd.concat([new_events_df, existing_events], ignore_index=True).drop_duplicates(subset=['Device_Serial', 'Timestamp', 'Event_Type'], keep='first')
    logger.info(f"Updated alerts: {len(alert_df)} records, events: {len(events_df)} records")
    return alert_df.to_dict('records'), events_df.to_dict('records')

# Initialize MQTT
setup_mqtt()

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=8050)
