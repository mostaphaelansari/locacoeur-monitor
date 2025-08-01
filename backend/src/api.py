ubuntu@vps-d4be9002:/opt/locacoeur-monitor/backend/src$ cat api.py
from fastapi import FastAPI
import psycopg2
from decouple import config

app = FastAPI()

DB_CONFIG = {
    "dbname": config("DB_NAME", default="mqtt_db"),
    "user": config("DB_USER", default="mqtt_user"),
    "password": config("DB_PASSWORD"),
    "host": config("DB_HOST", default="91.134.90.10"),
    "port": config("DB_PORT", default="5432")
}

@app.get("/devices")
async def get_devices():
    with psycopg2.connect(**DB_CONFIG) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT d.serial, dd.battery, dd.led_power, dd.led_defibrillator, dd.latitude, dd.longitude
            FROM Devices d
            LEFT JOIN device_data dd ON d.serial = dd.device_serial
            WHERE dd.received_at = (
                SELECT MAX(received_at) FROM device_data WHERE device_serial = d.serial
            )
        """)
        return [{"serial": row[0], "battery": row[1], "led_power": row[2],
                 "led_defibrillator": row[3], "latitude": row[4], "longitude": row[5]}
                for row in cur.fetchall()]

@app.get("/alerts")
async def get_alerts():
    with psycopg2.connect(**DB_CONFIG) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT device_serial, alert_id, alert_message, received_at
            FROM device_data
            WHERE alert_id IS NOT NULL
            ORDER BY received_at DESC
            LIMIT 50
        """)
        return [{"device_serial": row[0], "alert_id": row[1], "alert_message": row[2],
                 "received_at": row[3].isoformat()} for row in cur.fetchall()]
