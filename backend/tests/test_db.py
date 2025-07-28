import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.utils import get_db_connection
import pytest
import psycopg2

@pytest.fixture
def db_conn():
    conn = get_db_connection()
    yield conn
    conn.rollback()
    conn.close()

def test_insert_device(db_conn):
    cur = db_conn.cursor()
    cur.execute("INSERT INTO Devices (serial, mqtt_broker_url) VALUES ('TEST-001', 'mqtts://test.com')")
    db_conn.commit()
    cur.execute("SELECT serial, mqtt_broker_url FROM Devices WHERE serial = 'TEST-001'")
    assert cur.fetchone() == ("TEST-001", "mqtts://test.com")

def test_insert_device_data(db_conn):
    cur = db_conn.cursor()
    cur.execute("INSERT INTO Devices (serial) VALUES ('TEST-001')")
    cur.execute("""
        INSERT INTO device_data (device_serial, topic, battery, led_power, led_defibrillator, received_at)
        VALUES ('TEST-001', 'LC1/TEST-001/event/status', 90, 'Green', 'Green', CURRENT_TIMESTAMP)
    """)
    db_conn.commit()
    cur.execute("SELECT battery, led_power, led_defibrillator FROM device_data WHERE device_serial = 'TEST-001'")
    result = cur.fetchone()
    assert result == (90, "Green", "Green")

def test_invalid_battery(db_conn):
    cur = db_conn.cursor()
    cur.execute("INSERT INTO Devices (serial) VALUES ('TEST-001')")
    with pytest.raises(psycopg2.errors.CheckViolation):
        cur.execute("""
            INSERT INTO device_data (device_serial, topic, battery, received_at)
            VALUES ('TEST-001', 'LC1/TEST-001/event/status', 150, CURRENT_TIMESTAMP)
        """)
