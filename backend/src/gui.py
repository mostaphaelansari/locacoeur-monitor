import dash
from dash import dcc, html, Input, Output, State, dash_table, callback_context
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
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
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Custom color scheme
COLORS = {
    'sapphire_green': '#0F4C75',  # Deep sapphire green
    'sapphire_light': '#1B5E85',  # Lighter sapphire
    'sapphire_accent': '#3282B8',  # Accent sapphire
    'blood_red': '#8B0000',  # Dark blood red
    'blood_light': '#B22222',  # Lighter blood red
    'blood_accent': '#DC143C',  # Crimson accent
    'neutral_dark': '#1A1A1A',
    'neutral_light': '#F5F5F5',
    'success_green': '#0E7C0E',
    'warning_amber': '#FF6B6B'
}

# Database configuration
DB_CONFIG = {
    "dbname": config("DB_NAME", default="mqtt_db"),
    "user": config("DB_USER", default="mqtt_user"),
    "password": config("DB_PASSWORD", default=""),
    "host": config("DB_HOST", default="91.134.90.10"),
    "port": config("DB_PORT", default="5432")
}

# MQTT Configuration
MQTT_BROKER = config("MQTT_BROKER", default="mqtt.locacoeur.com")
MQTT_PORT = int(config("MQTT_PORT", default=8883))
MQTT_CA_CERT = config("MQTT_CA_CERT", default="certs/ca.crt")
MQTT_CLIENT_CERT = config("MQTT_CLIENT_CERT", default="certs/client.crt")
MQTT_CLIENT_KEY = config("MQTT_CLIENT_KEY", default="certs/client.key")
MQTT_USERNAME = config("MQTT_USERNAME", default="locacoeur")
MQTT_PASSWORD = config("MQTT_PASSWORD", default="")

# Constants
TOPIC_PREFIX = "LC1"
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

VALID_AUDIO_MESSAGES = ["message_1", "message_2"]

# Available tables in the database
DB_TABLES = [
    'alerts', 'commands', 'device_data', 'device_data_backup',
    'devices', 'events', 'leds', 'locations', 'results',
    'servers', 'status', 'versions'
]

class DatabaseManager:
    def __init__(self):
        try:
            self.db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **DB_CONFIG)
            self.connected = True
            logger.info("✅ Database connection pool created successfully")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            self.connected = False
            self.db_pool = None

    def get_connection(self):
        if not self.connected or not self.db_pool:
            return None
        try:
            return self.db_pool.getconn()
        except Exception as e:
            logger.error(f"Error getting database connection: {e}")
            return None

    def release_connection(self, conn):
        if self.db_pool and conn:
            try:
                self.db_pool.putconn(conn)
            except Exception as e:
                logger.error(f"Error releasing connection: {e}")

    def execute_query(self, query, params=None):
        conn = self.get_connection()
        if not conn:
            logger.warning("No database connection available")
            return pd.DataFrame()

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
            logger.error(f"Database query error: {e}")
            return pd.DataFrame() if query.strip().upper().startswith('SELECT') else False
        finally:
            cur.close()
            self.release_connection(conn)

    def get_table_info(self, table_name):
        """Get column information for a specific table"""
        query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
        """
        return self.execute_query(query, (table_name,))

    def get_table_data(self, table_name, limit=100):
        """Get sample data from a table"""
        query = f"SELECT * FROM {table_name} ORDER BY 1 DESC LIMIT %s"
        return self.execute_query(query, (limit,))

class MQTTPublisher:
    def __init__(self):
        self.client = None
        self.connected = False
        self.setup_client()

    def setup_client(self):
        try:
            self.client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"enhanced-dashboard-{uuid.uuid4()}",
                protocol=mqtt.MQTTv5
            )

            cert_files = [MQTT_CA_CERT, MQTT_CLIENT_CERT, MQTT_CLIENT_KEY]
            missing_certs = [f for f in cert_files if not os.path.exists(f)]

            if missing_certs:
                logger.warning(f"⚠️  Missing certificate files: {missing_certs}")
                logger.info("🔧 MQTT commands will work in demo mode")
                return

            self.client.tls_set(
                ca_certs=MQTT_CA_CERT,
                certfile=MQTT_CLIENT_CERT,
                keyfile=MQTT_CLIENT_KEY,
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLSv1_2
            )

            if MQTT_USERNAME and MQTT_PASSWORD:
                self.client.username_pw_set(username=MQTT_USERNAME, password=MQTT_PASSWORD)

            logger.info("✅ MQTT client configured successfully")

        except Exception as e:
            logger.error(f"❌ MQTT client setup failed: {e}")

    def connect(self):
        if not self.client:
            logger.warning("MQTT client not configured")
            return False

        try:
            self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            self.client.loop_start()
            self.connected = True
            logger.info("✅ MQTT client connected")
            return True
        except Exception as e:
            logger.error(f"❌ MQTT connection failed: {e}")
            self.connected = False
            return False

    def publish_command(self, device_serial, command_type, payload):
        if not self.connected:
            logger.info(f"🔧 Demo mode: Would send {command_type} to {device_serial}: {payload}")
            return True

        try:
            topic = f"{TOPIC_PREFIX}/{device_serial}/command/{command_type}"
            self.client.publish(topic, json.dumps(payload), qos=1)
            logger.info(f"✅ Command sent: {topic}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to publish command: {e}")
            return False

# Initialize components
db_manager = DatabaseManager()
mqtt_publisher = MQTTPublisher()
mqtt_connection_status = mqtt_publisher.connect()

# Initialize Dash app with custom CSS
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME])
app.title = "Locacoeur Control Center"

# Initialize Dash app with custom CSS
external_stylesheets = [
    dbc.themes.BOOTSTRAP,
    dbc.icons.FONT_AWESOME,
    {
        'href': 'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap',
        'rel': 'stylesheet'
    }
]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "Locacoeur Control Center"

# Custom CSS styles
custom_styles = {
    'header': {
        'background': f"linear-gradient(135deg, {COLORS['sapphire_green']} 0%, {COLORS['sapphire_accent']} 100%)",
        'padding': '2rem',
        'borderRadius': '15px',
        'marginBottom': '2rem',
        'boxShadow': '0 8px 16px rgba(0,0,0,0.2)'
    },
    'btn_sapphire': {
        'backgroundColor': COLORS['sapphire_accent'],
        'borderColor': COLORS['sapphire_accent'],
        'color': 'white'
    },
    'btn_blood': {
        'backgroundColor': COLORS['blood_red'],
        'borderColor': COLORS['blood_red'],
        'color': 'white'
    },
    'card_shadow': {
        'boxShadow': '0 4px 6px rgba(0, 0, 0, 0.1)',
        'transition': 'all 0.3s ease'
    }
}

# Layout
app.layout = html.Div([

    dbc.Container([
        # Header
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.H1([
                        html.I(className="fas fa-heartbeat me-3", style={"color": COLORS['blood_accent']}),
                        "Locacoeur Control Center"
                    ], className="text-center mb-2", style={"color": "white"}),
                    html.P("Advanced IoT Device Management & Real-time Monitoring",
                           className="text-center mb-3", style={"color": "rgba(255,255,255,0.8)"}),

                    html.Div([
                        dbc.Badge([
                            html.I(className="fas fa-database me-1"),
                            "Database: Connected" if db_manager.connected else "Database: Disconnected"
                        ], color="light" if db_manager.connected else "danger", className="me-2"),

                        dbc.Badge([
                            html.I(className="fas fa-wifi me-1"),
                            "MQTT: Connected" if mqtt_connection_status else "MQTT: Demo Mode"
                        ], color="light" if mqtt_connection_status else "warning", className="me-2"),

                        dbc.Badge([
                            html.I(className="fas fa-clock me-1"),
                            html.Span(id="current-time")
                        ], color="light")
                    ], className="text-center mb-3")
                ], style=custom_styles['header'])
            ])
        ]),

        # Quick Stats
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.I(className="fas fa-microchip fa-2x mb-2", style={"color": COLORS['sapphire_accent']}),
                            html.H4(id="total-devices", children="0", className="mb-1"),
                            html.P("Total Devices", className="text-muted mb-0")
                        ], className="text-center")
                    ])
                ], style={**custom_styles['card_shadow'], "borderTop": f"3px solid {COLORS['sapphire_accent']}"})
            ], width=3),

            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.I(className="fas fa-check-circle fa-2x mb-2", style={"color": COLORS['success_green']}),
                            html.H4(id="online-devices", children="0", className="mb-1"),
                            html.P("Online Devices", className="text-muted mb-0")
                        ], className="text-center")
                    ])
                ], style={**custom_styles['card_shadow'], "borderTop": f"3px solid {COLORS['success_green']}"})
            ], width=3),

            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.I(className="fas fa-exclamation-triangle fa-2x mb-2", style={"color": COLORS['blood_accent']}),
                            html.H4(id="critical-alerts", children="0", className="mb-1"),
                            html.P("Critical Alerts", className="text-muted mb-0")
                        ], className="text-center")
                    ])
                ], style={**custom_styles['card_shadow'], "borderTop": f"3px solid {COLORS['blood_accent']}"})
            ], width=3),

            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.I(className="fas fa-battery-half fa-2x mb-2", style={"color": COLORS['warning_amber']}),
                            html.H4(id="avg-battery", children="0%", className="mb-1"),
                            html.P("Avg Battery", className="text-muted mb-0")
                        ], className="text-center")
                    ])
                ], style={**custom_styles['card_shadow'], "borderTop": f"3px solid {COLORS['warning_amber']}"})
            ], width=3)
        ], className="mb-4"),

        # Control Panel
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="fas fa-terminal me-2"),
                            "Device Command Center"
                        ], className="mb-0", style={"color": COLORS['sapphire_green']})
                    ], style={"backgroundColor": "rgba(15,76,117,0.1)"}),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                dbc.Label("Select Device:", className="fw-bold"),
                                dcc.Dropdown(
                                    id="device-selector",
                                    placeholder="Choose a device to control...",
                                    style={"marginBottom": "1rem"}
                                )
                            ], width=6),
                            dbc.Col([
                                dbc.Label("Quick Commands:", className="fw-bold"),
                                html.Div([
                                    dbc.ButtonGroup([
                                        dbc.Button([html.I(className="fas fa-sync me-1"), "Refresh"],
                                                 id="refresh-btn", color="primary", size="sm"),
                                        dbc.Button([html.I(className="fas fa-map-marker-alt me-1"), "Location"],
                                                 id="quick-location-btn", color="info", size="sm"),
                                        dbc.Button([html.I(className="fas fa-cog me-1"), "Config"],
                                                 id="quick-config-btn", color="primary", size="sm"),
                                    ], className="w-100")
                                ])
                            ], width=6)
                        ]),

                        html.Hr(),

                        dbc.Row([
                            dbc.Col([
                                dbc.Label("Command Type:", className="fw-bold"),
                                dcc.Dropdown(
                                    id="command-type",
                                    options=[
                                        {"label": "📋 Get Configuration", "value": "config/get"},
                                        {"label": "⚙️ Set Configuration", "value": "config"},
                                        {"label": "📱 Get Firmware Version", "value": "version/get"},
                                        {"label": "🔄 Update Firmware", "value": "update"},
                                        {"label": "📍 Get Location", "value": "location"},
                                        {"label": "🔊 Play Audio Message", "value": "play"},
                                        {"label": "📝 Get Device Logs", "value": "log/get"}
                                    ],
                                    placeholder="Select command to execute...",
                                    style={"marginBottom": "1rem"}
                                )
                            ], width=8),
                            dbc.Col([
                                dbc.Label("Action:", className="fw-bold"),
                                dbc.Button([
                                    html.I(className="fas fa-paper-plane me-2"),
                                    "Execute Command"
                                ], id="execute-btn", color="danger", className="w-100")
                            ], width=4)
                        ]),

                        html.Div(id="command-response", className="mt-3")
                    ])
                ])
            ])
        ], className="mb-4"),

        # Device Status Cards
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.H4([
                        html.I(className="fas fa-tv me-2"),
                        "Device Status Overview"
                    ], className="mb-3", style={"color": COLORS['sapphire_green']}),
                    html.Div(id="device-status-cards")
                ])
            ])
        ], className="mb-4"),

        # Tabs
        dbc.Card([
            dbc.CardHeader([
                dbc.Tabs([
                    dbc.Tab(label="📊 Real-time Monitoring", tab_id="monitoring"),
                    dbc.Tab(label="📈 Analytics Dashboard", tab_id="analytics"),
                    dbc.Tab(label="🗄️ Database Tables", tab_id="database"),
                    dbc.Tab(label="🚨 Alerts & Events", tab_id="alerts"),
                    dbc.Tab(label="📡 Commands & Results", tab_id="commands"),
                    dbc.Tab(label="⚙️ Configuration", tab_id="config")
                ], id="main-tabs", active_tab="monitoring")
            ], style={"backgroundColor": COLORS['neutral_light']}),
            dbc.CardBody([
                html.Div(id="tab-content")
            ])
        ]),

        # Auto-refresh
        dcc.Interval(id='fast-interval', interval=10*1000, n_intervals=0),
        dcc.Interval(id='slow-interval', interval=60*1000, n_intervals=0),
        dcc.Store(id='device-data-store'),
        dcc.Store(id='selected-table-store'),

    ], fluid=True)
])

# Callbacks
@app.callback(
    Output('current-time', 'children'),
    Input('fast-interval', 'n_intervals')
)
def update_current_time(n):
    return datetime.now().strftime('%H:%M:%S')

@app.callback(
    Output('device-selector', 'options'),
    [Input('fast-interval', 'n_intervals'),
     Input('refresh-btn', 'n_clicks')]
)
def update_device_list(n, refresh_clicks):
    try:
        df = db_manager.execute_query("SELECT DISTINCT serial FROM devices ORDER BY serial")
        if df.empty:
            return []
        options = [{"label": f"🔌 {serial}", "value": serial} for serial in df['serial']]
        return options
    except Exception as e:
        logger.error(f"Error updating device list: {e}")
        return []

@app.callback(
    Output('device-data-store', 'data'),
    [Input('fast-interval', 'n_intervals'),
     Input('refresh-btn', 'n_clicks')]
)
def update_device_data_store(n_intervals, refresh_clicks):
    try:
        query = """
        SELECT DISTINCT ON (device_serial)
            device_serial, battery, connection, defibrillator, latitude, longitude,
            power_source, led_power, led_defibrillator, led_monitoring,
            led_assistance, led_mqtt, led_environmental, timestamp,
            COALESCE(to_timestamp(timestamp/1000.0), CURRENT_TIMESTAMP) as created_at
        FROM device_data
        WHERE COALESCE(to_timestamp(timestamp/1000.0), CURRENT_TIMESTAMP) >= NOW() - INTERVAL '24 hours'
        ORDER BY device_serial,
                 COALESCE(to_timestamp(timestamp/1000.0), CURRENT_TIMESTAMP) DESC
        """
        df = db_manager.execute_query(query)
        return df.to_dict('records') if not df.empty else []
    except Exception as e:
        logger.error(f"Error updating device data: {e}")
        return []

@app.callback(
    [Output('total-devices', 'children'),
     Output('online-devices', 'children'),
     Output('critical-alerts', 'children'),
     Output('avg-battery', 'children')],
    Input('device-data-store', 'data')
)
def update_quick_stats(device_data):
    if not device_data:
        return "0", "0", "0", "0%"

    df = pd.DataFrame(device_data)
    total = len(df)
    online = len(df[df['battery'] > 20]) if 'battery' in df.columns else 0

    critical = 0
    try:
        alerts_df = db_manager.execute_query("""
            SELECT COUNT(*) as count FROM device_data
            WHERE alert_id IN (2, 5, 6, 7)
            AND COALESCE(to_timestamp(timestamp/1000.0), CURRENT_TIMESTAMP) >= NOW() - INTERVAL '1 hour'
        """)
        if not alerts_df.empty:
            critical = int(alerts_df.iloc[0]['count'])
    except:
        pass

    avg_battery = f"{df['battery'].mean():.0f}%" if 'battery' in df.columns and not df['battery'].isna().all() else "N/A"

    return str(total), str(online), str(critical), avg_battery

@app.callback(
    Output('device-status-cards', 'children'),
    Input('device-data-store', 'data')
)
def update_device_status_cards(device_data):
    if not device_data:
        return dbc.Alert([
            html.I(className="fas fa-info-circle me-2"),
            "No device data available. Check database connection."
        ], color="info")

    cards = []
    for device in device_data:
        try:
            battery = device.get('battery', 0) or 0
            device_serial = device.get('device_serial', 'Unknown')

            if battery > 80:
                status_color = COLORS['success_green']
                status_text, status_icon = "Excellent", "fas fa-check-circle"
            elif battery > 50:
                status_color = COLORS['sapphire_accent']
                status_text, status_icon = "Good", "fas fa-info-circle"
            elif battery > 20:
                status_color = COLORS['warning_amber']
                status_text, status_icon = "Low Battery", "fas fa-exclamation-triangle"
            else:
                status_color = COLORS['blood_accent']
                status_text, status_icon = "Critical", "fas fa-exclamation-circle"

            led_indicators = []
            led_types = [
                ('power', 'Power'), ('defibrillator', 'Defib'), ('monitoring', 'Monitor'),
                ('assistance', 'Assist'), ('mqtt', 'MQTT'), ('environmental', 'Temp')
            ]

            for led_type, label in led_types:
                led_key = f'led_{led_type}'
                status = device.get(led_key, 'Off')
                if status == 'Green':
                    color = COLORS['success_green']
                    icon = "fas fa-circle"
                elif status == 'Red':
                    color = COLORS['blood_accent']
                    icon = "fas fa-circle"
                else:
                    color = "#95a5a6"
                    icon = "far fa-circle"

                led_indicators.append(
                    html.Span([
                        html.I(className=f"{icon} me-1", style={"fontSize": "0.8rem", "color": color}),
                        label
                    ], className="badge bg-light text-dark me-1 mb-1", style={"fontSize": "0.7rem"})
                )

            card = dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.Div([
                            html.H6([
                                html.I(className="fas fa-microchip me-2"),
                                f"Device {device_serial}"
                            ], className="mb-0"),
                            html.Span([
                                html.I(className=f"{status_icon} me-1"),
                                status_text
                            ], style={"color": status_color, "fontWeight": "bold"})
                        ], className="d-flex justify-content-between align-items-center")
                    ], style={"backgroundColor": "rgba(15,76,117,0.05)"}),
                    dbc.CardBody([
                        html.Div([
                            html.Small("Battery Level", className="text-muted"),
                            dbc.Progress(
                                value=battery,
                                label=f"{battery}%",
                                style={"backgroundColor": "#e0e0e0"},
                                color="success" if battery > 50 else "warning" if battery > 20 else "danger",
                                className="mb-2"
                            )
                        ]),

                        html.Div([
                            html.Small([
                                html.Strong("Connection: "),
                                (device.get('connection') or 'Unknown').upper()
                            ], className="d-block"),
                            html.Small([
                                html.Strong("Power: "),
                                (device.get('power_source') or 'Unknown').title()
                            ], className="d-block"),
                            html.Small([
                                html.Strong("Defibrillator: "),
                                str(device.get('defibrillator') or 'Unknown')
                            ], className="d-block mb-2")
                        ]),

                        html.Div([
                            html.Small("LED Status:", className="text-muted d-block mb-1"),
                            html.Div(led_indicators)
                        ])
                    ])
                ], className="h-100", style={"borderLeft": f"4px solid {status_color}"})
            ], width=12, lg=6, xl=4, className="mb-3")

            cards.append(card)

        except Exception as e:
            logger.error(f"Error creating card for device {device.get('device_serial', 'Unknown')}: {e}")
            continue

    return cards

@app.callback(
    Output('command-response', 'children'),
    [Input('execute-btn', 'n_clicks'),
     Input('quick-location-btn', 'n_clicks'),
     Input('quick-config-btn', 'n_clicks')],
    [State('device-selector', 'value'),
     State('command-type', 'value')]
)
def execute_command(execute_clicks, location_clicks, config_clicks, device_serial, command_type):
    ctx = callback_context
    if not ctx.triggered or not device_serial:
        return ""

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    try:
        op_id = str(uuid.uuid4())
        payload = {"opId": op_id}

        if button_id == 'quick-location-btn':
            command_type = "location"
        elif button_id == 'quick-config-btn':
            command_type = "config/get"
        elif button_id == 'execute-btn' and not command_type:
            return dbc.Alert("⚠️ Please select a command type first", color="warning", dismissable=True)

        if not command_type:
            return ""

        success = mqtt_publisher.publish_command(device_serial, command_type, payload)

        if success:
            return dbc.Alert([
                html.I(className="fas fa-check-circle me-2"),
                f"✅ Command '{command_type}' sent to device {device_serial}",
                html.Br(),
                html.Small(f"Operation ID: {op_id}", className="text-muted")
            ], color="success", dismissable=True)
        else:
            return dbc.Alert([
                html.I(className="fas fa-exclamation-triangle me-2"),
                f"⚠️ Command sent in demo mode to {device_serial}"
            ], color="info", dismissable=True)

    except Exception as e:
        return dbc.Alert([
            html.I(className="fas fa-times-circle me-2"),
            f"❌ Failed to send command: {str(e)}"
        ], color="danger", dismissable=True)

@app.callback(
    Output('tab-content', 'children'),
    [Input('main-tabs', 'active_tab'),
     Input('device-data-store', 'data')]
)
def render_tab_content(active_tab, device_data):
    if active_tab == "monitoring":
        return render_monitoring_tab(device_data)
    elif active_tab == "analytics":
        return render_analytics_tab()
    elif active_tab == "database":
        return render_database_tab()
    elif active_tab == "alerts":
        return render_alerts_tab()
    elif active_tab == "commands":
        return render_commands_tab()
    elif active_tab == "config":
        return render_config_tab()
    return html.Div("Select a tab to view content")

def render_monitoring_tab(device_data):
    if not device_data:
        return dbc.Alert("No device data available for monitoring", color="info")

    df = pd.DataFrame(device_data)

    # Custom color schemes for the charts
    fig_battery = px.bar(
        df,
        x='device_serial',
        y='battery',
        title='📊 Battery Levels by Device',
        color='battery',
        color_continuous_scale=[COLORS['blood_accent'], COLORS['warning_amber'], COLORS['success_green']],
        range_color=[0, 100]
    )
    fig_battery.update_layout(
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color=COLORS['sapphire_green'])
    )

    led_cols = ['led_power', 'led_defibrillator', 'led_monitoring', 'led_assistance', 'led_mqtt', 'led_environmental']
    led_data = []
    for col in led_cols:
        if col in df.columns:
            status_counts = df[col].value_counts()
            for status, count in status_counts.items():
                led_data.append({
                    'LED_Type': col.replace('led_', '').title(),
                    'Status': status,
                    'Count': count
                })

    if led_data:
        led_df = pd.DataFrame(led_data)
        fig_led = px.bar(
            led_df,
            x='LED_Type',
            y='Count',
            color='Status',
            title='💡 LED Status Distribution',
            color_discrete_map={
                'Green': COLORS['success_green'],
                'Red': COLORS['blood_accent'],
                'Off': '#95a5a6'
            }
        )
        fig_led.update_layout(
            height=400,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color=COLORS['sapphire_green'])
        )
    else:
        fig_led = go.Figure()
        fig_led.update_layout(title="No LED data available", height=400)

    return dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([dcc.Graph(figure=fig_battery)])
            ])
        ], width=6),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([dcc.Graph(figure=fig_led)])
            ])
        ], width=6)
    ])

def render_analytics_tab():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="fas fa-chart-line me-2"),
                            "Device Analytics"
                        ], style={"color": COLORS['sapphire_green']})
                    ]),
                    dbc.CardBody([
                        html.P("Advanced analytics features coming soon!", className="text-muted"),
                        html.P("This section will include:"),
                        html.Ul([
                            html.Li("Historical trend analysis"),
                            html.Li("Predictive maintenance insights"),
                            html.Li("Performance metrics"),
                            html.Li("Alert frequency analysis")
                        ])
                    ])
                ])
            ])
        ])
    ])

def render_database_tab():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H4([
                    html.I(className="fas fa-database me-2"),
                    "Database Tables Explorer"
                ], className="mb-3", style={"color": COLORS['sapphire_green']}),

                dbc.Card([
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                dbc.Label("Select Table:", className="fw-bold"),
                                dcc.Dropdown(
                                    id="table-selector",
                                    options=[{"label": f"📊 {table}", "value": table} for table in DB_TABLES],
                                    placeholder="Choose a table to explore...",
                                    value="devices"
                                )
                            ], width=6),
                            dbc.Col([
                                dbc.Label("Actions:", className="fw-bold"),
                                dbc.ButtonGroup([
                                    dbc.Button([
                                        html.I(className="fas fa-sync me-1"),
                                        "Refresh"
                                    ], id="refresh-table-btn", color="primary", size="sm"),
                                    dbc.Button([
                                        html.I(className="fas fa-info-circle me-1"),
                                        "Schema"
                                    ], id="schema-btn", color="info", size="sm"),
                                    dbc.Button([
                                        html.I(className="fas fa-download me-1"),
                                        "Export"
                                    ], id="export-btn", color="danger", size="sm", disabled=True),
                                ])
                            ], width=6)
                        ], className="mb-3"),

                        html.Div(id="table-info", className="mb-3"),
                        html.Div(id="table-content")
                    ])
                ])
            ])
        ])
    ])

def render_alerts_tab():
    try:
        # Fetch recent alerts
        alerts_query = """
        SELECT
            dd.device_serial,
            dd.alert_id,
            dd.timestamp,
            COALESCE(to_timestamp(dd.timestamp/1000.0), CURRENT_TIMESTAMP) as alert_time
        FROM device_data dd
        WHERE dd.alert_id IS NOT NULL
        ORDER BY dd.timestamp DESC
        LIMIT 100
        """
        alerts_df = db_manager.execute_query(alerts_query)

        if alerts_df.empty:
            return dbc.Alert("No alerts recorded yet", color="info")

        # Add alert descriptions
        alerts_df['alert_description'] = alerts_df['alert_id'].map(ALERT_CODES).fillna('Unknown Alert')

        # Create alert cards
        alert_cards = []
        for _, alert in alerts_df.iterrows():
            alert_color = COLORS['blood_accent'] if alert['alert_id'] in [2, 5, 6, 7] else COLORS['warning_amber']

            card = dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.Div([
                            html.I(className="fas fa-exclamation-triangle me-2", style={"color": alert_color}),
                            html.Strong(f"Device: {alert['device_serial']}")
                        ], className="d-flex align-items-center"),
                        html.P(alert['alert_description'], className="mb-1"),
                        html.Small(f"Time: {alert['alert_time']}", className="text-muted")
                    ])
                ])
            ], className="mb-2", style={"borderLeft": f"4px solid {alert_color}"})

            alert_cards.append(card)

        return dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H4([
                        html.I(className="fas fa-bell me-2"),
                        "Recent Alerts"
                    ], className="mb-3", style={"color": COLORS['sapphire_green']}),

                    html.Div(alert_cards[:10])  # Show only recent 10
                ])
            ])
        ])

    except Exception as e:
        logger.error(f"Error in alerts tab: {e}")
        return dbc.Alert(f"Error loading alerts: {str(e)}", color="danger")

def render_commands_tab():
    try:
        # Fetch recent commands
        commands_query = """
        SELECT
            device_serial,
            command_type,
            op_id,
            timestamp,
            COALESCE(to_timestamp(timestamp/1000.0), CURRENT_TIMESTAMP) as command_time
        FROM commands
        ORDER BY timestamp DESC
        LIMIT 50
        """
        commands_df = db_manager.execute_query(commands_query)

        if commands_df.empty:
            return dbc.Alert("No commands recorded yet", color="info")

        # Create commands table
        commands_table = dash_table.DataTable(
            id='commands-table',
            columns=[
                {"name": "Device", "id": "device_serial"},
                {"name": "Command Type", "id": "command_type"},
                {"name": "Operation ID", "id": "op_id"},
                {"name": "Time", "id": "command_time"}
            ],
            data=commands_df.to_dict('records'),
            style_cell={
                'textAlign': 'left',
                'padding': '10px',
                'fontFamily': 'sans-serif'
            },
            style_header={
                'backgroundColor': COLORS['sapphire_green'],
                'color': 'white',
                'fontWeight': 'bold'
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': 'rgba(15, 76, 117, 0.05)'
                }
            ],
            page_size=10
        )

        return dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H4([
                        html.I(className="fas fa-terminal me-2"),
                        "Command History"
                    ], className="mb-3", style={"color": COLORS['sapphire_green']}),

                    dbc.Card([
                        dbc.CardBody([commands_table])
                    ])
                ])
            ])
        ])

    except Exception as e:
        logger.error(f"Error in commands tab: {e}")
        return dbc.Alert(f"Error loading commands: {str(e)}", color="danger")

def render_config_tab():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="fas fa-cogs me-2"),
                            "System Configuration"
                        ], style={"color": COLORS['sapphire_green']})
                    ]),
                    dbc.CardBody([
                        html.H6("Database Settings", className="mb-3"),
                        html.Ul([
                            html.Li(f"Host: {DB_CONFIG['host']}"),
                            html.Li(f"Port: {DB_CONFIG['port']}"),
                            html.Li(f"Database: {DB_CONFIG['dbname']}"),
                            html.Li(f"Status: {'Connected' if db_manager.connected else 'Disconnected'}")
                        ]),

                        html.Hr(),

                        html.H6("MQTT Settings", className="mb-3"),
                        html.Ul([
                            html.Li(f"Broker: {MQTT_BROKER}"),
                            html.Li(f"Port: {MQTT_PORT}"),
                            html.Li(f"Status: {'Connected' if mqtt_publisher.connected else 'Demo Mode'}")
                        ]),

                        html.Hr(),

                        html.H6("Alert Codes Reference", className="mb-3"),
                        dbc.Table([
                            html.Thead([
                                html.Tr([
                                    html.Th("Code", style={"color": COLORS['sapphire_green']}),
                                    html.Th("Description", style={"color": COLORS['sapphire_green']})
                                ])
                            ]),
                            html.Tbody([
                                html.Tr([
                                    html.Td(code),
                                    html.Td(desc)
                                ]) for code, desc in ALERT_CODES.items()
                            ])
                        ], striped=True, bordered=True, hover=True, size="sm")
                    ])
                ])
            ])
        ])
    ])

# Callbacks for database tab
@app.callback(
    [Output('table-info', 'children'),
     Output('table-content', 'children')],
    [Input('table-selector', 'value'),
     Input('refresh-table-btn', 'n_clicks'),
     Input('schema-btn', 'n_clicks')]
)
def update_table_view(selected_table, refresh_clicks, schema_clicks):
    ctx = callback_context

    if not selected_table:
        return "", dbc.Alert("Please select a table to view", color="info")

    try:
        # Get table info
        info_df = db_manager.get_table_info(selected_table)
        data_df = db_manager.get_table_data(selected_table)

        # Create info badge
        info_badge = dbc.Badge(
            f"Table: {selected_table} | Rows shown: {len(data_df)} (limited to 100)",
            color="info",
            className="mb-3"
        )

        # Create data table
        if not data_df.empty:
            columns = [{"name": col, "id": col} for col in data_df.columns]

            data_table = dash_table.DataTable(
                id='data-table',
                columns=columns,
                data=data_df.to_dict('records'),
                style_cell={
                    'textAlign': 'left',
                    'padding': '10px',
                    'maxWidth': '200px',
                    'overflow': 'hidden',
                    'textOverflow': 'ellipsis'
                },
                style_header={
                    'backgroundColor': COLORS['sapphire_green'],
                    'color': 'white',
                    'fontWeight': 'bold'
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgba(15, 76, 117, 0.05)'
                    }
                ],
                page_size=20,
                filter_action="native",
                sort_action="native",
                tooltip_data=[
                    {
                        column: {'value': str(value), 'type': 'markdown'}
                        for column, value in row.items()
                    } for row in data_df.to_dict('records')
                ],
                tooltip_duration=None
            )

            content = dbc.Card([
                dbc.CardBody([data_table])
            ])
        else:
            content = dbc.Alert("No data available in this table", color="warning")

        return info_badge, content

    except Exception as e:
        logger.error(f"Error loading table data: {e}")
        return "", dbc.Alert(f"Error loading table: {str(e)}", color="danger")

# Run the application
if __name__ == '__main__':
    print("🚀 Starting Locacoeur Control Center...")
    print(f"📊 Database: {'✅ Connected' if db_manager.connected else '❌ Disconnected'}")
    print(f"📡 MQTT: {'✅ Connected' if mqtt_connection_status else '🔧 Demo Mode'}")
    print("🌐 Dashboard will be available at: http://0.0.0.0:8050")

    app.run(debug=False, host='0.0.0.0', port=8050)
