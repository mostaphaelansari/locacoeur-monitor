
2 directories, 30 files
ubuntu@vps-d4be9002:/opt/locacoeur-monitor/backend/src$ cat gui_v0.py
import dash
from dash import dcc, html, Input, Output, State, dash_table, callback_context
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
            raise e
        finally:
            cur.close()
            self.release_connection(conn)

class MQTTPublisher:
    def __init__(self):
        self.client = None
        self.setup_client()

    def setup_client(self):
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"dashboard-{uuid.uuid4()}",
            protocol=mqtt.MQTTv5
        )

        # Configure TLS
        self.client.tls_set(
            ca_certs=MQTT_CA_CERT,
            certfile=MQTT_CLIENT_CERT,
            keyfile=MQTT_CLIENT_KEY,
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLSv1_2
        )

        if MQTT_USERNAME and MQTT_PASSWORD:
            self.client.username_pw_set(username=MQTT_USERNAME, password=MQTT_PASSWORD)

    def connect(self):
        try:
            self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            self.client.loop_start()
            return True
        except Exception as e:
            print(f"MQTT connection failed: {e}")
            return False

    def publish_command(self, device_serial, command_type, payload):
        topic = f"{TOPIC_PREFIX}/{device_serial}/command/{command_type}"
        self.client.publish(topic, json.dumps(payload), qos=1)

# Initialize components
db_manager = DatabaseManager()
mqtt_publisher = MQTTPublisher()
mqtt_publisher.connect()

# Initialize Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "LOCACOEUR MQTT Dashboard"

# Layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("LOCACOEUR MQTT Dashboard", className="text-center mb-4"),
            html.Hr()
        ])
    ]),

    # Control Panel
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Device Control Panel"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Select Device:"),
                            dcc.Dropdown(
                                id="device-selector",
                                placeholder="Select a device..."
                            )
                        ], width=6),
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
                                placeholder="Select command..."
                            )
                        ], width=6)
                    ]),
                    html.Br(),
                    dbc.Row([
                        dbc.Col([
                            dbc.Button("Execute Command", id="execute-btn", color="primary", className="me-2"),
                            dbc.Button("Refresh Data", id="refresh-btn", color="secondary")
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
            html.H3("Device Status Overview"),
            html.Div(id="device-status-cards")
        ])
    ], className="mb-4"),

    # Tabs for different views
    dbc.Tabs([
        dbc.Tab(label="Real-time Monitoring", tab_id="monitoring"),
        dbc.Tab(label="Device Data", tab_id="data"),
        dbc.Tab(label="Alerts & Events", tab_id="alerts"),
        dbc.Tab(label="Commands & Results", tab_id="commands"),
        dbc.Tab(label="Configuration", tab_id="config"),
        dbc.Tab(label="Analytics", tab_id="analytics")
    ], id="main-tabs", active_tab="monitoring"),

    html.Div(id="tab-content", className="mt-4"),

    # Auto-refresh interval
    dcc.Interval(
        id='interval-component',
        interval=30*1000,  # 30 seconds
        n_intervals=0
    ),

    # Store for device data
    dcc.Store(id='device-data-store'),
    dcc.Store(id='selected-device-store')

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
    except:
        return []

@app.callback(
    Output('device-data-store', 'data'),
    [Input('interval-component', 'n_intervals'),
     Input('refresh-btn', 'n_clicks')]
)
def update_device_data(n_intervals, refresh_clicks):
    try:
        # Get latest device data
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
              END >= NOW() - INTERVAL '24 hours'
        ORDER BY device_serial,
                 CASE
                    WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                    ELSE CURRENT_TIMESTAMP
                 END DESC
        """
        df = db_manager.execute_query(query)
        return df.to_dict('records')
    except Exception as e:
        print(f"Error updating device data: {e}")
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
        # LED status indicators
        led_indicators = []
        led_types = ['power', 'defibrillator', 'monitoring', 'assistance', 'mqtt', 'environmental']

        for led_type in led_types:
            led_key = f'led_{led_type}'
            status = device.get(led_key, 'Off')
            color = 'success' if status == 'Green' else 'danger' if status == 'Red' else 'secondary'
            led_indicators.append(
                dbc.Badge(f"{led_type.title()}: {status}", color=color, className="me-1")
            )

        # Battery level color
        battery = device.get('battery', 0)
        battery_color = 'danger' if battery < 20 else 'warning' if battery < 50 else 'success'

        card = dbc.Card([
            dbc.CardHeader([
                html.H5(f"Device {device['device_serial']}", className="mb-0")
            ]),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.P([
                            html.Strong("Battery: "),
                            dbc.Badge(f"{battery}%", color=battery_color)
                        ]),
                        html.P([
                            html.Strong("Connection: "),
                            device.get('connection', 'Unknown')
                        ]),
                        html.P([
                            html.Strong("Power Source: "),
                            device.get('power_source', 'Unknown')
                        ]),
                        html.P([
                            html.Strong("Defibrillator: "),
                            device.get('defibrillator', 'Unknown')
                        ])
                    ], width=6),
                    dbc.Col([
                        html.P("LED Status:", className="fw-bold"),
                        html.Div(led_indicators),
                        html.P([
                            html.Strong("Last Update: "),
                            pd.to_datetime(device.get('created_at', '')).strftime('%Y-%m-%d %H:%M:%S') if device.get('created_at') and pd.notna(device.get('created_at')) else 'Unknown'
                        ], className="mt-2")
                    ], width=6)
                ])
            ])
        ], className="mb-3")

        cards.append(dbc.Col(card, width=12, lg=6, xl=4))

    return dbc.Row(cards)

@app.callback(
    Output('tab-content', 'children'),
    [Input('main-tabs', 'active_tab'),
     Input('device-data-store', 'data')]
)
def render_tab_content(active_tab, device_data):
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
    elif active_tab == "analytics":
        return render_analytics_tab()
    return html.Div("Select a tab")

def render_monitoring_tab(device_data):
    if not device_data:
        return html.P("No device data available for monitoring")

    # Create battery level chart
    df = pd.DataFrame(device_data)

    fig_battery = px.bar(
        df,
        x='device_serial',
        y='battery',
        title='Battery Levels by Device',
        color='battery',
        color_continuous_scale=['red', 'yellow', 'green']
    )
    fig_battery.update_layout(height=400)

    # Create LED status summary
    led_cols = ['led_power', 'led_defibrillator', 'led_monitoring', 'led_assistance', 'led_mqtt', 'led_environmental']
    led_status_data = []

    for col in led_cols:
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

    return dbc.Row([
        dbc.Col([
            dcc.Graph(figure=fig_battery)
        ], width=6),
        dbc.Col([
            dcc.Graph(figure=fig_led)
        ], width=6)
    ])

def render_data_tab():
    try:
        # Get recent device data
        query = """
        SELECT device_serial, topic, battery, connection, defibrillator,
               latitude, longitude, power_source, timestamp,
               CASE
                   WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                   ELSE CURRENT_TIMESTAMP
               END as created_at
        FROM device_data
        WHERE CASE
                WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                ELSE CURRENT_TIMESTAMP
              END >= NOW() - INTERVAL '24 hours'
        ORDER BY CASE
                   WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                   ELSE CURRENT_TIMESTAMP
                 END DESC
        LIMIT 100
        """
        df = db_manager.execute_query(query)

        if df.empty:
            return html.P("No recent device data available")

        # Convert timestamp for display
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')

        return dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{"name": i, "id": i} for i in df.columns],
            page_size=20,
            sort_action="native",
            filter_action="native",
            style_table={'overflowX': 'auto'},
            style_cell={'textAlign': 'left', 'padding': '10px'},
            style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'}
        )
    except Exception as e:
        return html.P(f"Error loading device data: {str(e)}")

def render_alerts_tab():
    try:
        # Get recent alerts
        query = """
        SELECT device_serial, alert_id, alert_message, timestamp,
               CASE
                   WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                   ELSE CURRENT_TIMESTAMP
               END as created_at
        FROM device_data
        WHERE alert_id IS NOT NULL
        AND CASE
                WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                ELSE CURRENT_TIMESTAMP
            END >= NOW() - INTERVAL '7 days'
        ORDER BY CASE
                   WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                   ELSE CURRENT_TIMESTAMP
                 END DESC
        """
        df = db_manager.execute_query(query)

        if df.empty:
            return html.P("No recent alerts")

        # Add alert description
        df['alert_description'] = df['alert_id'].map(ALERT_CODES)
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # Create alert frequency chart
        alert_counts = df['alert_description'].value_counts()
        fig_alerts = px.bar(
            x=alert_counts.index,
            y=alert_counts.values,
            title='Alert Frequency (Last 7 Days)',
            labels={'x': 'Alert Type', 'y': 'Count'}
        )
        fig_alerts.update_layout(height=400)

        return dbc.Row([
            dbc.Col([
                dcc.Graph(figure=fig_alerts)
            ], width=12),
            dbc.Col([
                html.H4("Recent Alerts"),
                dash_table.DataTable(
                    data=df.to_dict('records'),
                    columns=[
                        {"name": "Device", "id": "device_serial"},
                        {"name": "Alert ID", "id": "alert_id"},
                        {"name": "Description", "id": "alert_description"},
                        {"name": "Message", "id": "alert_message"},
                        {"name": "Timestamp", "id": "created_at"}
                    ],
                    page_size=15,
                    sort_action="native",
                    style_cell={'textAlign': 'left', 'padding': '10px'},
                    style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
                    style_data_conditional=[
                        {
                            'if': {'filter_query': '{alert_id} = 7'},  # Defibrillator fault
                            'backgroundColor': '#ffcccc',
                        },
                        {
                            'if': {'filter_query': '{alert_id} = 5'},  # Power cut
                            'backgroundColor': '#ffe6cc',
                        }
                    ]
                )
            ], width=12)
        ])
    except Exception as e:
        return html.P(f"Error loading alerts: {str(e)}")

def render_commands_tab():
    try:
        # Get recent commands and results
        query = """
        SELECT c.device_serial, c.operation_id, c.topic, c.created_at as command_time,
               r.result_status, r.result_message, r.created_at as result_time
        FROM Commands c
        LEFT JOIN Results r ON c.command_id = r.command_id
        WHERE c.created_at >= NOW() - INTERVAL '24 hours'
        ORDER BY c.created_at DESC
        LIMIT 50
        """
        df = db_manager.execute_query(query)

        if df.empty:
            return html.P("No recent commands")

        # Format timestamps
        if 'command_time' in df.columns:
            df['command_time'] = pd.to_datetime(df['command_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        if 'result_time' in df.columns:
            df['result_time'] = pd.to_datetime(df['result_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

        return dbc.Row([
            dbc.Col([
                html.H4("Recent Commands & Results"),
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
                    style_cell={'textAlign': 'left', 'padding': '10px'},
                    style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
                    style_data_conditional=[
                        {
                            'if': {'filter_query': '{result_status} != 0'},
                            'backgroundColor': '#ffcccc',
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
                dbc.CardHeader("Device Configuration"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Device Serial:"),
                            dcc.Dropdown(id="config-device-selector", placeholder="Select device...")
                        ], width=6)
                    ]),
                    html.Hr(),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Monitoring Service:"),
                            dbc.Switch(id="monitoring-switch", value=True)
                        ], width=6),
                        dbc.Col([
                            dbc.Label("Assistance Service:"),
                            dbc.Switch(id="assistance-switch", value=True)
                        ], width=6)
                    ]),
                    html.Br(),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Firmware Version:"),
                            dbc.Input(id="firmware-version", placeholder="e.g., 1.2.3")
                        ], width=6),
                        dbc.Col([
                            dbc.Label("Audio Message:"),
                            dcc.Dropdown(
                                id="audio-message",
                                options=[
                                    {"label": "Message 1", "value": "message_1"},
                                    {"label": "Message 2", "value": "message_2"}
                                ],
                                placeholder="Select audio message..."
                            )
                        ], width=6)
                    ]),
                    html.Br(),
                    dbc.Row([
                        dbc.Col([
                            dbc.Button("Update Config", id="update-config-btn", color="primary", className="me-2"),
                            dbc.Button("Update Firmware", id="update-firmware-btn", color="warning", className="me-2"),
                            dbc.Button("Play Audio", id="play-audio-btn", color="info")
                        ])
                    ]),
                    html.Div(id="config-response", className="mt-3")
                ])
            ])
        ], width=12)
    ])

def render_analytics_tab():
    try:
        # Get historical data for analytics
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
               ) as hour_of_day
        FROM device_data
        WHERE battery IS NOT NULL
        AND CASE
                WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                ELSE CURRENT_TIMESTAMP
            END >= NOW() - INTERVAL '7 days'
        ORDER BY CASE
                   WHEN timestamp IS NOT NULL THEN to_timestamp(timestamp/1000.0)
                   ELSE CURRENT_TIMESTAMP
                 END
        """
        df = db_manager.execute_query(query)

        if df.empty:
            return html.P("No data available for analytics")

        # Battery trends over time
        fig_battery_trend = px.line(
            df.groupby(['device_serial', 'created_at'])['battery'].mean().reset_index(),
            x='created_at',
            y='battery',
            color='device_serial',
            title='Battery Level Trends (Last 7 Days)'
        )
        fig_battery_trend.update_layout(height=400)

        # Average battery by hour of day
        hourly_battery = df.groupby('hour_of_day')['battery'].mean().reset_index()
        fig_hourly = px.bar(
            hourly_battery,
            x='hour_of_day',
            y='battery',
            title='Average Battery Level by Hour of Day'
        )
        fig_hourly.update_layout(height=400)

        return dbc.Row([
            dbc.Col([
                dcc.Graph(figure=fig_battery_trend)
            ], width=12),
            dbc.Col([
                dcc.Graph(figure=fig_hourly)
            ], width=12)
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

        mqtt_publisher.publish_command(device_serial, command_type, payload)

        return dbc.Alert(
            f"Command '{command_type}' sent to device {device_serial} with opId {op_id}",
            color="success",
            dismissable=True
        )
    except Exception as e:
        return dbc.Alert(
            f"Failed to send command: {str(e)}",
            color="danger",
            dismissable=True
        )

# Configuration callbacks
@app.callback(
    Output('config-device-selector', 'options'),
    Input('interval-component', 'n_intervals')
)
def update_config_device_list(n):
    try:
        df = db_manager.execute_query("SELECT DISTINCT serial FROM Devices ORDER BY serial")
        return [{"label": serial, "value": serial} for serial in df['serial']]
    except:
        return []

@app.callback(
    Output('config-response', 'children'),
    [Input('update-config-btn', 'n_clicks'),
     Input('update-firmware-btn', 'n_clicks'),
     Input('play-audio-btn', 'n_clicks')],
    [State('config-device-selector', 'value'),
     State('monitoring-switch', 'value'),
     State('assistance-switch', 'value'),
     State('firmware-version', 'value'),
     State('audio-message', 'value')]
)
def handle_config_actions(config_clicks, firmware_clicks, audio_clicks, device_serial,
                         monitoring, assistance, firmware_version, audio_message):
    if not device_serial:
        return ""

    ctx = callback_context
    if not ctx.triggered:
        return ""

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    try:
        op_id = str(uuid.uuid4())

        if button_id == 'update-config-btn' and config_clicks:
            config_data = {
                "services": {
                    "monitoring": str(monitoring).lower(),
                    "assistance": str(assistance).lower()
                }
            }
            payload = {"opId": op_id, "config": config_data}
            mqtt_publisher.publish_command(device_serial, "config", payload)
            return dbc.Alert("Configuration update sent successfully!", color="success", dismissable=True)

        elif button_id == 'update-firmware-btn' and firmware_clicks and firmware_version:
            payload = {"opId": op_id, "version": firmware_version}
            mqtt_publisher.publish_command(device_serial, "update", payload)
            return dbc.Alert(f"Firmware update to version {firmware_version} initiated!", color="warning", dismissable=True)

        elif button_id == 'play-audio-btn' and audio_clicks and audio_message:
            payload = {"opId": op_id, "audioMessage": audio_message}
            mqtt_publisher.publish_command(device_serial, "play", payload)
            return dbc.Alert(f"Audio message '{audio_message}' sent to device!", color="info", dismissable=True)

    except Exception as e:
        return dbc.Alert(f"Error: {str(e)}", color="danger", dismissable=True)

    return ""

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8050)
