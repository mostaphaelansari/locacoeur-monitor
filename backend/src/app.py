import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
import json
import queue
import time
from datetime import datetime, timezone
from typing import Dict, Any
import logging
from mqtt import MQTTService, VALID_AUDIO_MESSAGES, ALERT_CODES, RESULT_CODES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gui.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LocacoeurGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Locacoeur Cloud API Control Panel")
        self.root.geometry("1200x800")
        
        # Initialize MQTT service
        self.mqtt_service = MQTTService()
        self.message_queue = queue.Queue()
        self.devices = {}  # Store device information
        self.selected_device = tk.StringVar()
        
        # Start MQTT service in a separate thread
        self.mqtt_thread = threading.Thread(target=self.run_mqtt_service, daemon=True)
        self.mqtt_thread.start()
        
        # Start queue processing
        self.root.after(100, self.process_queue)
        
        self.setup_gui()
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
    def setup_gui(self):
        """Setup the main GUI layout"""
        # Main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        
        # Device selection frame
        device_frame = ttk.LabelFrame(main_frame, text="Device Selection", padding="5")
        device_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=5)
        device_frame.columnconfigure(1, weight=1)
        
        ttk.Label(device_frame, text="Select Device:").grid(row=0, column=0, padx=5)
        self.device_combo = ttk.Combobox(device_frame, textvariable=self.selected_device, state="readonly")
        self.device_combo.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=5)
        self.device_combo.bind("<<ComboboxSelected>>", self.on_device_select)
        
        ttk.Button(device_frame, text="Refresh Devices", command=self.refresh_devices).grid(row=0, column=2, padx=5)
        
        # Command frame
        command_frame = ttk.LabelFrame(main_frame, text="Commands", padding="5")
        command_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=5)
        
        commands = [
            ("Request Config", self.request_config),
            ("Request Location", self.request_location),
            ("Request Firmware Version", self.request_firmware_version),
            ("Request Logs", self.request_log),
            ("Play Audio Message 1", lambda: self.play_audio("message_1")),
            ("Play Audio Message 2", lambda: self.play_audio("message_2")),
            ("Update Firmware", self.update_firmware),
        ]
        
        for idx, (text, command) in enumerate(commands):
            ttk.Button(command_frame, text=text, command=command).grid(row=idx//3, column=idx%3, padx=5, pady=2)
        
        # Status frame
        status_frame = ttk.LabelFrame(main_frame, text="Device Status", padding="5")
        status_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        status_frame.columnconfigure(0, weight=1)
        status_frame.rowconfigure(1, weight=1)
        
        # LED status indicators
        led_frame = ttk.Frame(status_frame)
        led_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=5)
        
        self.leds = {
            "Power": tk.StringVar(value="Unknown"),
            "Defibrillator": tk.StringVar(value="Unknown"),
            "Monitoring": tk.StringVar(value="Unknown"),
            "Assistance": tk.StringVar(value="Unknown"),
            "MQTT": tk.StringVar(value="Unknown"),
            "Environmental": tk.StringVar(value="Unknown"),
        }
        
        for idx, (led_name, var) in enumerate(self.leds.items()):
            ttk.Label(led_frame, text=f"{led_name}:").grid(row=0, column=idx*2, padx=5)
            ttk.Label(led_frame, textvariable=var, width=10).grid(row=0, column=idx*2+1, padx=5)
        
        # Status details
        self.status_text = scrolledtext.ScrolledText(status_frame, height=10, state='disabled')
        self.status_text.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        
        # Alerts frame
        alerts_frame = ttk.LabelFrame(main_frame, text="Alerts & Logs", padding="5")
        alerts_frame.grid(row=3, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        alerts_frame.columnconfigure(0, weight=1)
        alerts_frame.rowconfigure(0, weight=1)
        
        self.alerts_text = scrolledtext.ScrolledText(alerts_frame, height=10, state='disabled')
        self.alerts_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # MQTT connection status
        self.connection_status = tk.StringVar(value="Disconnected")
        ttk.Label(main_frame, text="MQTT Status:").grid(row=4, column=0, sticky=tk.W, padx=5)
        ttk.Label(main_frame, textvariable=self.connection_status).grid(row=4, column=0, sticky=tk.E, padx=5)
        
    def run_mqtt_service(self):
        """Run the MQTT service and handle connection status updates"""
        try:
            self.mqtt_service.run()
        except Exception as e:
            logger.error(f"MQTT service error: {e}")
            self.message_queue.put(("error", f"MQTT service crashed: {e}"))
        
    def process_queue(self):
        """Process messages from the queue to update GUI"""
        try:
            while not self.message_queue.empty():
                msg_type, data = self.message_queue.get_nowait()
                if msg_type == "device_data":
                    self.update_device_data(data)
                elif msg_type == "result":
                    self.update_result(data)
                elif msg_type == "connection_status":
                    self.connection_status.set(data)
                elif msg_type == "error":
                    self.log_alert(f"ERROR: {data}")
        except queue.Empty:
            pass
        self.root.after(100, self.process_queue)
        
    def refresh_devices(self):
        """Refresh the list of known devices from database"""
        conn = self.mqtt_service.connect_db()
        if not conn:
            messagebox.showerror("Error", "Failed to connect to database")
            return
        try:
            cur = conn.cursor()
            cur.execute("SELECT serial FROM Devices")
            devices = [row[0] for row in cur.fetchall()]
            self.device_combo['values'] = devices
            if devices and not self.selected_device.get():
                self.selected_device.set(devices[0])
                self.on_device_select(None)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to refresh devices: {e}")
        finally:
            cur.close()
            self.mqtt_service.release_db(conn)
        
    def on_device_select(self, event):
        """Handle device selection change"""
        device_serial = self.selected_device.get()
        if not device_serial:
            return
        self.update_status_display(device_serial)
        
    def update_status_display(self, device_serial: str):
        """Update status display for selected device"""
        conn = self.mqtt_service.connect_db()
        if not conn:
            return
        try:
            cur = conn.cursor()
            # Get latest status
            cur.execute(
                """
                SELECT payload, led_power, led_defibrillator, led_monitoring, 
                       led_assistance, led_mqtt, led_environmental
                FROM device_data
                WHERE device_serial = %s AND topic LIKE %s
                ORDER BY timestamp DESC LIMIT 1
                """,
                (device_serial, f"LC1/{device_serial}/event/status")
            )
            result = cur.fetchone()
            if result:
                payload, power, defib, mon, assist, mqtt, env = result
                payload = json.loads(payload)
                
                # Update LED indicators
                self.leds["Power"].set(power or "Unknown")
                self.leds["Defibrillator"].set(defib or "Unknown")
                self.leds["Monitoring"].set(mon or "Unknown")
                self.leds["Assistance"].set(assist or "Unknown")
                self.leds["MQTT"].set(mqtt or "Unknown")
                self.leds["Environmental"].set(env or "Unknown")
                
                # Update status text
                self.status_text.configure(state='normal')
                self.status_text.delete(1.0, tk.END)
                status_info = (
                    f"Battery: {payload.get('battery', 'Unknown')}%\n"
                    f"Power Source: {payload.get('power_source', 'Unknown')}\n"
                    f"Defibrillator Status: {payload.get('defibrillator', 'Unknown')}\n"
                    f"Connection: {payload.get('connection', 'Unknown')}\n"
                    f"Location: Lat {payload.get('location', {}).get('latitude', 'Unknown')}, "
                    f"Lon {payload.get('location', {}).get('longitude', 'Unknown')}\n"
                    f"Timestamp: {datetime.fromtimestamp(payload.get('timestamp', 0)/1000, tz=timezone.utc)}"
                )
                self.status_text.insert(tk.END, status_info)
                self.status_text.configure(state='disabled')
                
        except Exception as e:
            logger.error(f"Failed to update status for {device_serial}: {e}")
        finally:
            cur.close()
            self.mqtt_service.release_db(conn)
        
    def update_device_data(self, data: Dict[str, Any]):
        """Update GUI with new device data"""
        device_serial = data.get("device_serial")
        topic = data.get("topic")
        payload = data.get("payload")
        
        if device_serial == self.selected_device.get():
            if topic.endswith("/event/status"):
                self.update_status_display(device_serial)
            elif topic.endswith("/event/alert"):
                alert_id = payload.get("id")
                alert_desc = ALERT_CODES.get(alert_id, f"Unknown alert ID: {alert_id}")
                self.log_alert(f"Alert for {device_serial}: {alert_desc} (ID: {alert_id})")
            elif topic.endswith("/event/config"):
                self.log_alert(f"Config updated for {device_serial}: {json.dumps(payload, indent=2)}")
        
        # Update device list if new device detected
        if device_serial not in self.device_combo['values']:
            self.refresh_devices()
        
    def update_result(self, data: Dict[str, Any]):
        """Update GUI with command result"""
        device_serial = data.get("device_serial")
        result_code = int(data.get("result_status", -1))
        result_desc = RESULT_CODES.get(result_code, f"Unknown result code: {result_code}")
        message = data.get("result_message", "")
        
        if device_serial == self.selected_device.get():
            self.log_alert(f"Command result for {device_serial}: {result_desc} ({message})")
            
    def log_alert(self, message: str):
        """Log alert or message to alerts text area"""
        self.alerts_text.configure(state='normal')
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        self.alerts_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.alerts_text.see(tk.END)
        self.alerts_text.configure(state='disabled')
        
    def request_config(self):
        """Request device configuration"""
        device_serial = self.selected_device.get()
        if not device_serial:
            messagebox.showwarning("Warning", "Please select a device")
            return
        try:
            self.mqtt_service.request_config(device_serial)
            self.log_alert(f"Requested configuration for {device_serial}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to request config: {e}")
        
    def request_location(self):
        """Request device location"""
        device_serial = self.selected_device.get()
        if not device_serial:
            messagebox.showwarning("Warning", "Please select a device")
            return
        try:
            self.mqtt_service.request_location(device_serial)
            self.log_alert(f"Requested location for {device_serial}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to request location: {e}")
        
    def request_firmware_version(self):
        """Request firmware version"""
        device_serial = self.selected_device.get()
        if not device_serial:
            messagebox.showwarning("Warning", "Please select a device")
            return
        try:
            self.mqtt_service.request_firmware_version(device_serial)
            self.log_alert(f"Requested firmware version for {device_serial}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to request firmware version: {e}")
        
    def request_log(self):
        """Request device logs"""
        device_serial = self.selected_device.get()
        if not device_serial:
            messagebox.showwarning("Warning", "Please select a device")
            return
        try:
            self.mqtt_service.request_log(device_serial)
            self.log_alert(f"Requested logs for {device_serial}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to request logs: {e}")
        
    def play_audio(self, audio_message: str):
        """Play audio message on device"""
        device_serial = self.selected_device.get()
        if not device_serial:
            messagebox.showwarning("Warning", "Please select a device")
            return
        try:
            self.mqtt_service.play_audio(device_serial, audio_message)
            self.log_alert(f"Sent play audio command ({audio_message}) for {device_serial}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to play audio: {e}")
        
    def update_firmware(self):
        """Open dialog to update firmware"""
        device_serial = self.selected_device.get()
        if not device_serial:
            messagebox.showwarning("Warning", "Please select a device")
            return
            
        dialog = tk.Toplevel(self.root)
        dialog.title("Update Firmware")
        dialog.geometry("400x200")
        dialog.transient(self.root)
        
        ttk.Label(dialog, text="Firmware Version:").grid(row=0, column=0, padx=5, pady=5)
        version_var = tk.StringVar(value="2.1.1")
        ttk.Entry(dialog, textvariable=version_var).grid(row=0, column=1, padx=5, pady=5)
        
        ttk.Label(dialog, text="Firmware URL (optional):").grid(row=1, column=0, padx=5, pady=5)
        url_var = tk.StringVar()
        ttk.Entry(dialog, textvariable=url_var).grid(row=1, column=1, padx=5, pady=5)
        
        def submit():
            try:
                version = version_var.get().strip()
                url = url_var.get().strip() or None
                if not version:
                    messagebox.showwarning("Warning", "Version is required")
                    return
                self.mqtt_service.update_firmware(device_serial, version, url)
                self.log_alert(f"Sent firmware update command for {device_serial} (version: {version})")
                dialog.destroy()
            except Exception as e:
                messagebox.showerror("Error", f"Failed to send firmware update: {e}")
        
        ttk.Button(dialog, text="Update", command=submit).grid(row=2, column=0, padx=5, pady=10)
        ttk.Button(dialog, text="Cancel", command=dialog.destroy).grid(row=2, column=1, padx=5, pady=10)
        
    def on_closing(self):
        """Handle window closing"""
        if messagebox.askokcancel("Quit", "Do you want to quit?"):
            self.mqtt_service.running = False
            self.root.destroy()

# Override MQTTService methods to integrate with GUI
class GUIMQTTService(MQTTService):
    def __init__(self, gui_queue: queue.Queue):
        super().__init__()
        self.gui_queue = gui_queue
        
    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        super().on_connect(client, userdata, flags, reason_code, properties)
        status = "Connected" if reason_code == 0 else f"Disconnected (Code: {reason_code})"
        self.gui_queue.put(("connection_status", status))
        
    def insert_device_data(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        super().insert_device_data(device_serial, topic, data)
        self.gui_queue.put(("device_data", {
            "device_serial": device_serial,
            "topic": topic,
            "payload": data
        }))
        
    def insert_result(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        super().insert_result(device_serial, topic, data)
        self.gui_queue.put(("result", {
            "device_serial": device_serial,
            "result_status": data.get("result"),
            "result_message": data.get("message", "")
        }))

if __name__ == "__main__":
    root = tk.Tk()
    # Replace MQTTService with GUIMQTTService
    gui = LocacoeurGUI(root)
    gui.mqtt_service = GUIMQTTService(gui.message_queue)
    root.mainloop()
