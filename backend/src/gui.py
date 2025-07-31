import dash
from dash import dcc, html, Input, Output, State, callback_context, dash_table
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import json
import threading
import time
import queue
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
import logging
import uuid
import sys
import os

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import configuration
try:
    from config import MQTT_CONFIG, DATABASE_CONFIG, APP_CONFIG, print_config_status, validate_config
    print_config_status()
except ImportError:
    print("Warning: config.py not found. Using default configuration.")
    MQTT_CONFIG = {"BROKER_HOST": "localhost", "BROKER_PORT": 1883, "USE_TLS": False}
    DATABASE_CONFIG = {"DB_HOST": "localhost", "DB_PORT": 5432, "DB_NAME": "locacoeur"}
    APP_CONFIG = {"DEBUG": True, "HOST": "0.0.0.0", "PORT": 8050}

# Import the MQTT service with error handling
try:
    from mqtt import MQTTService, VALID_AUDIO_MESSAGES, ALERT_CODES, RESULT_CODES
    MQTT_AVAILABLE = True
    print("✅ MQTT service imported successfully")
except ImportError as e:
    print(f"⚠️  Warning: mqtt.py not found or has errors ({e}). Using mock implementation.")
    MQTT_AVAILABLE = False
    
    # Enhanced mock implementation
    class MQTTService:
        def __init__(self):
            self.running = True
            self.devices = {
                "LC001": {"battery": 85, "status": "online", "lat": 42.8911, "lon": 23.9157},
                "LC002": {"battery": 92, "status": "online", "lat": 42.8925, "lon": 23.9170},
                "LC003": {"battery": 67, "status": "warning", "lat": 42.8897, "lon": 23.9144}
            }
            self.connection_status = "Mock Mode"
            
        def connect_db(self):
            return None
            
        def release_db(self, conn):
            pass
            
        def request_config(self, device_serial): 
            print(f"Mock: Requesting config for {device_serial}")
        def request_location(self, device_serial): 
            print(f"Mock: Requesting location for {device_serial}")
        def request_firmware_version(self, device_serial): 
            print(f"Mock: Requesting firmware version for {device_serial}")
        def request_log(self, device_serial): 
            print(f"Mock: Requesting logs for {device_serial}")
        def play_audio(self, device_serial, message): 
            print(f"Mock: Playing audio '{message}' on {device_serial}")
        def update_firmware(self, device_serial, version, url=None): 
            print(f"Mock: Updating firmware to {version} on {device_serial}")
        def run(self): 
            print("Mock MQTT service running...")
            while self.running:
                time.sleep(1)
    
    VALID_AUDIO_MESSAGES = ["message_1", "message_2", "emergency_alert", "test_tone"]
    ALERT_CODES = {
        1: "Device moving", 
        2: "Power cut", 
        3: "Defibrillator fault",
        4: "Low battery",
        5: "Connection lost",
        6: "Environmental alert",
        7: "System error"
    }
    RESULT_CODES = {
        0: "Success", 
        -1: "Missing opId", 
        -2: "Null reference", 
        -3: "Audio file not found",
        -4: "Device offline",
        -5: "Command timeout"
    }

# Configure logging
log_level = getattr(logging, APP_CONFIG.get("LOG_LEVEL", "INFO").upper())
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dash_app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DashMQTTService(MQTTService):
    """Extended MQTT service for Dash integration with better error handling"""
    
    def __init__(self, message_queue: queue.Queue):
        try:
            super().__init__()
            self.message_queue = message_queue
            self.connection_status = "Initializing..."
            logger.info("MQTT service initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize MQTT service: {e}")
            # Initialize with mock behavior
            self.running = True
            self.devices = {}
            self.message_queue = message_queue
            self.connection_status = f"Error: {str(e)}"
        
    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        try:
            super().on_connect(client, userdata, flags, reason_code, properties)
            self.connection_status = "Connected" if reason_code == 0 else f"Disconnected (Code: {reason_code})"
            self.message_queue.put({
                "type": "connection_status",
                "status": self.connection_status,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            logger.error(f"Error in on_connect: {e}")
            self.connection_status = f"Connection Error: {str(e)}"
        
    def on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        try:
            if hasattr(super(), 'on_disconnect'):
                super().on_disconnect(client, userdata, flags, reason_code, properties)
            self.connection_status = f"Disconnected (Code: {reason_code})"
            self.message_queue.put({
                "type": "connection_status", 
                "status": self.connection_status,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            logger.error(f"Error in on_disconnect: {e}")
        
    def insert_device_data(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        try:
            if hasattr(super(), 'insert_device_data'):
                super().insert_device_data(device_serial, topic, data)
            self.message_queue.put({
                "type": "device_data",
                "device_serial": device_serial,
                "topic": topic,
                "data": data,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            logger.error(f"Error inserting device data: {e}")
        
    def insert_result(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        try:
            if hasattr(super(), 'insert_result'):
                super().insert_result(device_serial, topic, data)
            self.message_queue.put({
                "type": "command_result",
                "device_serial": device_serial,
                "topic": topic,
                "data": data,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            logger.error(f"Error inserting result: {e}")
        
    def send_alert_email(self, subject: str, message: str, priority: str = "normal"):
        try:
            if hasattr(super(), 'send_alert_email'):
                super().send_alert_email(subject, message, priority)
            self.message_queue.put({
                "type": "alert",
                "subject": subject,
                "message": message,
                "priority": priority,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            logger.error(f"Error sending alert email: {e}")

class LocacoeurDashApp:
    def __init__(self):
        logger.info("Initializing Locacoeur Dash Application...")
        
        # Initialize Dash app
        self.app = dash.Dash(__name__, external_stylesheets=[
            'https://codepen.io/chriddyp/pen/bWLwgP.css',
            'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css'
        ])
        
        # Initialize message queue and MQTT service
        self.message_queue = queue.Queue()
        
        try:
            self.mqtt_service = DashMQTTService(self.message_queue)
            logger.info("MQTT service wrapper created successfully")
        except Exception as e:
            logger.error(f"Failed to create MQTT service: {e}")
            # Create a minimal mock service
            self.mqtt_service = type('MockMQTT', (), {
                'connection_status': f'Error: {str(e)}',
                'connect_db': lambda: None,
                'release_db': lambda conn: None,
                'request_config': lambda device: None,
                'request_location': lambda device: None,
                'request_firmware_version': lambda device: None,
                'request_log': lambda device: None,
                'play_audio': lambda device, msg: None,
                'update_firmware': lambda device, ver, url=None: None,
                'run': lambda: None
            })()
        
        # Data storage
        self.devices_data = {}
        self.alerts_data = []
        self.connection_logs = []
        self.command_history = []
        
        # Start MQTT service in background thread
        self.mqtt_thread = threading.Thread(target=self.run_mqtt_service, daemon=True)
        self.mqtt_thread.start()
        
        # Setup layout and callbacks
        self.setup_layout()
        self.setup_callbacks()
        
        logger.info("Dash application initialized successfully")
        
    def run_mqtt_service(self):
        """Run MQTT service in background with error handling"""
        try:
            logger.info("Starting MQTT service thread...")
            if hasattr(self.mqtt_service, 'run') and callable(self.mqtt_service.run):
                self.mqtt_service.run()
            else:
                logger.info("MQTT service run method not available, using mock mode")
                while True:
                    time.sleep(5)
                    # Generate some mock data for testing
                    if not MQTT_AVAILABLE:
                        self.generate_mock_data()
        except Exception as e:
            logger.error(f"MQTT service error: {e}")
            
    def generate_mock_data(self):
        """Generate mock data for testing when MQTT is not available"""
        try:
            import random
            for device_id in ["LC001", "LC002", "LC003"]:
                # Simulate battery level changes
                battery = max(20, min(100, random.randint(60, 95)))
                self.message_queue.put({
                    "type": "device_data",
                    "device_serial": device_id,
                    "topic": f"LC1/{device_id}/event/status",
                    "data": {
                        "battery": battery,
                        "power_source": "AC" if battery > 80 else "Battery",
                        "connection": "Good"
                    },
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
        except Exception as e:
            logger.error(f"Error generating mock data: {e}")
            
    def get_devices_from_db(self) -> List[str]:
        """Get list of devices from database with fallback"""
        try:
            if not MQTT_AVAILABLE:
                return ["LC001", "LC002", "LC003"]  # Mock devices
                
            conn = self.mqtt_service.connect_db()
            if not conn:
                return ["LC001", "LC002", "LC003"]  # Fallback devices
                
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT serial FROM Devices ORDER BY serial")
            devices = [row[0] for row in cur.fetchall()]
            return devices if devices else ["LC001", "LC002", "LC003"]
        except Exception as e:
            logger.error(f"Failed to get devices: {e}")
            return ["LC001", "LC002", "LC003"]  # Always return some devices
        finally:
            try:
                if 'cur' in locals():
                    cur.close()
                if 'conn' in locals() and conn:
                    self.mqtt_service.release_db(conn)
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")
                
    def get_device_status(self, device_serial: str) -> Dict[str, Any]:
        """Get latest device status from database with mock fallback"""
        try:
            if not MQTT_AVAILABLE:
                # Return mock status
                import random
                return {
                    "battery": random.randint(60, 95),
                    "power_source": random.choice(["AC", "Battery"]),
                    "defibrillator": "Ready",
                    "connection": "Good",
                    "latitude": 42.8911 + random.uniform(-0.01, 0.01),
                    "longitude": 23.9157 + random.uniform(-0.01, 0.01),
                    "leds": {
                        "Power": "Green",
                        "Defibrillator": "Green", 
                        "Monitoring": "Green",
                        "Assistance": "Green",
                        "MQTT": "Green",
                        "Environmental": "Green"
                    },
                    "timestamp": int(datetime.now().timestamp() * 1000),
                    "last_seen": datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
                }
                
            conn = self.mqtt_service.connect_db()
            if not conn:
                return {}
                
            cur = conn.cursor()
            cur.execute("""
                SELECT battery, power_source, defibrillator, connection, latitude, longitude,
                       led_power, led_defibrillator, led_monitoring, led_assistance, 
                       led_mqtt, led_environmental, timestamp, payload
                FROM device_data
                WHERE device_serial = %s AND topic LIKE %s
                ORDER BY timestamp DESC LIMIT 1
            """, (device_serial, f"LC1/{device_serial}/event/status"))
            
            result = cur.fetchone()
            if result:
                (battery, power_source, defibrillator, connection, lat, lon,
                 led_power, led_defib, led_mon, led_assist, led_mqtt, led_env, 
                 timestamp, payload) = result
                
                return {
                    "battery": battery,
                    "power_source": power_source,
                    "defibrillator": defibrillator,
                    "connection": connection,
                    "latitude": lat,
                    "longitude": lon,
                    "leds": {
                        "Power": led_power,
                        "Defibrillator": led_defib,
                        "Monitoring": led_mon,
                        "Assistance": led_assist,
                        "MQTT": led_mqtt,
                        "Environmental": led_env
                    },
                    "timestamp": timestamp,
                    "last_seen": datetime.fromtimestamp(timestamp/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                }
        except Exception as e:
            logger.error(f"Failed to get device status for {device_serial}: {e}")
        finally:
            try:
                if 'cur' in locals():
                    cur.close()
                if 'conn' in locals() and conn:
                    self.mqtt_service.release_db(conn)
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")
        return {}
        
    def get_recent_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent alerts from database with mock fallback"""
        try:
            if not MQTT_AVAILABLE:
                # Return mock alerts
                import random
                mock_alerts = []
                for i in range(3):
                    mock_alerts.append({
                        "device": f"LC00{i+1}",
                        "alert_id": random.choice([1, 2, 3, 4]),
                        "description": random.choice(list(ALERT_CODES.values())),
                        "message": "Mock alert message",
                        "timestamp": (datetime.now() - timedelta(hours=i)).strftime('%Y-%m-%d %H:%M:%S'),
                        "severity": random.choice(["Warning", "Critical"])
                    })
                return mock_alerts
                
            conn = self.mqtt_service.connect_db()
            if not conn:
                return []
                
            cur = conn.cursor()
            cur.execute("""
                SELECT device_serial, alert_id, alert_message, timestamp
                FROM device_data
                WHERE alert_id IS NOT NULL
                ORDER BY timestamp DESC LIMIT %s
            """, (limit,))
            
            alerts = []
            for row in cur.fetchall():
                device_serial, alert_id, alert_message, timestamp = row
                alerts.append({
                    "device": device_serial,
                    "alert_id": alert_id,
                    "description": ALERT_CODES.get(alert_id, f"Unknown alert {alert_id}"),
                    "message": alert_message,
                    "timestamp": datetime.fromtimestamp(timestamp/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                    "severity": "Critical" if alert_id in [2, 3, 6, 7] else "Warning"
                })
            return alerts
        except Exception as e:
            logger.error(f"Failed to get alerts: {e}")
            return []
        finally:
            try:
                if 'cur' in locals():
                    cur.close()
                if 'conn' in locals() and conn:
                    self.mqtt_service.release_db(conn)
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")
                
    def setup_layout(self):
        """Setup the Dash app layout"""
        self.app.layout = html.Div([
            # Header with system status
            html.Div([
                html.H1("🏥 Locacoeur Cloud API Control Panel", 
                       style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': 10}),
                html.Div([
                    html.Span("🔧 Mode: "),
                    html.Span("Production" if MQTT_AVAILABLE else "Demo/Testing", 
                             style={'color': '#27ae60' if MQTT_AVAILABLE else '#f39c12', 'fontWeight': 'bold'})
                ], style={'textAlign': 'center', 'marginBottom': 20}),
                html.Div(id='connection-status', 
                        style={'textAlign': 'center', 'marginBottom': 20})
            ]),
            
            # Device selection and refresh
            html.Div([
                html.Div([
                    html.Label('Select Device:', style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='device-dropdown',
                        placeholder="Select a device...",
                        style={'width': '300px'}
                    )
                ], style={'display': 'inline-block', 'marginRight': 20}),
                
                html.Button('🔄 Refresh Devices', id='refresh-devices-btn', 
                           style={'backgroundColor': '#3498db', 'color': 'white', 'border': 'none', 
                                 'padding': '10px 20px', 'borderRadius': '5px', 'cursor': 'pointer'})
            ], style={'marginBottom': 30, 'textAlign': 'center'}),
            
            # Device status cards
            html.Div(id='device-status-cards', style={'marginBottom': 30}),
            
            # Command buttons
            html.Div([
                html.H3("🎛️ Device Commands", style={'color': '#2c3e50'}),
                html.Div([
                    html.Button('📋 Request Config', id='cmd-config', className='command-button'),
                    html.Button('📍 Request Location', id='cmd-location', className='command-button'),
                    html.Button('🔧 Request Version', id='cmd-version', className='command-button'),
                    html.Button('📜 Request Logs', id='cmd-logs', className='command-button'),
                ], style={'marginBottom': 10}),
                html.Div([
                    html.Button('🔊 Play Message 1', id='cmd-audio1', className='command-button audio-button'),
                    html.Button('📢 Play Message 2', id='cmd-audio2', className='command-button audio-button'),
                    html.Button('⬆️ Update Firmware', id='cmd-firmware', className='command-button firmware-button'),
                ], style={'marginBottom': 20}),
                
                # Command feedback
                html.Div(id='command-feedback', style={'marginBottom': 20})
            ], style={'marginBottom': 30}),
            
            # Charts and data display
            html.Div([
                # Battery chart
                html.Div([
                    dcc.Graph(id='battery-chart')
                ], style={'width': '50%', 'display': 'inline-block'}),
                
                # Location map
                html.Div([
                    dcc.Graph(id='location-map')
                ], style={'width': '50%', 'display': 'inline-block'})
            ], style={'marginBottom': 30}),
            
            # Alerts and logs
            html.Div([
                html.Div([
                    html.H3("🚨 Recent Alerts", style={'color': '#e74c3c'}),
                    html.Div(id='alerts-table')
                ], style={'width': '50%', 'display': 'inline-block', 'paddingRight': 20}),
                
                html.Div([
                    html.H3("📋 System Logs", style={'color': '#2c3e50'}),
                    html.Div(id='system-logs', 
                            style={'height': '300px', 'overflowY': 'scroll', 
                                  'border': '1px solid #ddd', 'padding': '10px',
                                  'backgroundColor': '#f8f9fa', 'fontFamily': 'monospace'})
                ], style={'width': '50%', 'display': 'inline-block'})
            ]),
            
            # Auto-refresh interval
            dcc.Interval(
                id='interval-component',
                interval=5000,  # Update every 5 seconds
                n_intervals=0
            ),
            
            # Store for device data
            dcc.Store(id='device-data-store'),
            dcc.Store(id='selected-device-store')
            
        ], style={'margin': '20px', 'fontFamily': 'Arial, sans-serif'})
        
        # Enhanced CSS
        self.app.index_string = '''
        <!DOCTYPE html>
        <html>
            <head>
                {%metas%}
                <title>{%title%}</title>
                {%favicon%}
                {%css%}
                <style>
                    .command-button {
                        background: linear-gradient(45deg, #2ecc71, #27ae60);
                        color: white;
                        border: none;
                        padding: 10px 15px;
                        margin: 5px;
                        border-radius: 5px;
                        cursor: pointer;
                        font-size: 14px;
                        transition: all 0.3s ease;
                    }
                    .command-button:hover {
                        background: linear-gradient(45deg, #27ae60, #229954);
                        transform: translateY(-2px);
                        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
                    }
                    .command-button:disabled {
                        background: #95a5a6;
                        cursor: not-allowed;
                        transform: none;
                        box-shadow: none;
                    }
                    .audio-button {
                        background: linear-gradient(45deg, #f39c12, #e67e22);
                    }
                    .audio-button:hover {
                        background: linear-gradient(45deg, #e67e22, #d35400);
                    }
                    .firmware-button {
                        background: linear-gradient(45deg, #9b59b6, #8e44ad);
                    }
                    .firmware-button:hover {
                        background: linear-gradient(45deg, #8e44ad, #7d3c98);
                    }
                    .status-card {
                        background: white;
                        border-radius: 8px;
                        padding: 20px;
                        margin: 10px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                        border-left: 4px solid #3498db;
                        transition: all 0.3s ease;
                    }
                    .status-card:hover {
                        transform: translateY(-2px);
                        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
                    }
                    .led-indicator {
                        display: inline-block;
                        width: 12px;
                        height: 12px;
                        border-radius: 50%;
                        margin-right: 8px;
                        box-shadow: 0 0 3px rgba(0,0,0,0.3);
                    }
                    .led-green { 
                        background-color: #2ecc71;
                        box-shadow: 0 0 8px rgba(46,204,113,0.6);
                    }
                    .led-red { 
                        background-color: #e74c3c;
                        box-shadow: 0 0 8px rgba(231,76,60,0.6);
                    }
                    .led-yellow {
                        background-color: #f39c12;
                        box-shadow: 0 0 8px rgba(243,156,18,0.6);
                    }
                    .led-unknown { 
                        background-color: #95a5a6;
                    }
                    .system-status {
                        padding: 10px;
                        border-radius: 5px;
                        margin: 5px 0;
                    }
                    .status-connected {
                        background-color: #d5f4e6;
                        color: #27ae60;
                        border: 1px solid #27ae60;
                    }
                    .status-disconnected {
                        background-color: #fadbd8;
                        color: #e74c3c;
                        border: 1px solid #e74c3c;
                    }
                    .status-mock {
                        background-color: #fef5e7;
                        color: #f39c12;
                        border: 1px solid #f39c12;
                    }
                </style>
            </head>
            <body>
                {%app_entry%}
                <footer>
                    {%config%}
                    {%scripts%}
                    {%renderer%}
                </footer>
            </body>
        </html>
        '''
        
    def setup_callbacks(self):
        """Setup all Dash callbacks with proper error handling"""
        
        @self.app.callback(
            [Output('device-dropdown', 'options'),
             Output('connection-status', 'children')],
            [Input('refresh-devices-btn', 'n_clicks'),
             Input('interval-component', 'n_intervals')]
        )
        def update_devices_and_status(refresh_clicks, n_intervals):
            try:
                # Process queue messages
                self.process_message_queue()
                
                # Get devices
                devices = self.get_devices_from_db()
                device_options = [{'label': device, 'value': device} for device in devices]
                
                # Connection status with enhanced info
                if not MQTT_AVAILABLE:
                    status_class = "status-mock"
                    status_text = "🔧 Demo Mode - MQTT Service Not Available"
                elif hasattr(self.mqtt_service, 'connection_status'):
                    if "Connected" in self.mqtt_service.connection_status:
                        status_class = "status-connected"
                        status_text = f"✅ {self.mqtt_service.connection_status}"
                    else:
                        status_class = "status-disconnected" 
                        status_text = f"❌ {self.mqtt_service.connection_status}"
                else:
                    status_class = "status-disconnected"
                    status_text = "❓ Status Unknown"
                
                status_div = html.Div([
                    html.Div(status_text, className=f"system-status {status_class}"),
                    html.Div(f"Devices Available: {len(devices)} | Queue Size: {self.message_queue.qsize()}", 
                            style={'textAlign': 'center', 'color': '#7f8c8d', 'fontSize': '12px', 'marginTop': '5px'})
                ])
                
                return device_options, status_div
                
            except Exception as e:
                logger.error(f"Error in update_devices_and_status: {e}")
                return [], html.Div(f"Error: {str(e)}", style={'color': '#e74c3c'})
            
        @self.app.callback(
            Output('device-status-cards', 'children'),
            [Input('device-dropdown', 'value'),
             Input('interval-component', 'n_intervals')]
        )
        def update_device_status(selected_device, n_intervals):
            try:
                if not selected_device:
                    return html.Div("Select a device to view status", 
                                  style={'textAlign': 'center', 'color': '#7f8c8d', 'padding': '20px'})
                                  
                status = self.get_device_status(selected_device)
                if not status:
                    return html.Div(f"No data available for {selected_device}", 
                                  style={'textAlign': 'center', 'color': '#e74c3c', 'padding': '20px'})
                                  
                # Create LED indicators
                def get_led_class(led_status):
                    if led_status ==
