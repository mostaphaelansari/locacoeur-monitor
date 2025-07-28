import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest
from unittest.mock import MagicMock, patch
from src.mqtt_to_postgres import MQTTService

@pytest.fixture
def mqtt_service():
    with patch('paho.mqtt.client.Client') as mock_client:
        service = MQTTService()
        service.client = mock_client.return_value
        return service

def test_mqtt_connection(mqtt_service):
    mqtt_service.setup_mqtt_client()
    mqtt_service.client.connect.assert_called_with(
        host="mqtt.locacoeur.com",
        port=8883,
        keepalive=60
    )

def test_mqtt_subscriptions(mqtt_service):
    mqtt_service.on_connect(mqtt_service.client, None, None, 0)
    mqtt_service.client.subscribe.assert_called_with([
        ("LC1/+/event/#", 0),
        ("LC1/+/command/#", 0),
        ("LC1/+/result", 0)
    ])

def test_message_handling_status(mqtt_service):
    with patch.object(mqtt_service, 'insert_device_data') as mock_insert:
        mqtt_service.on_message(
            mqtt_service.client,
            None,
            MagicMock(topic="LC1/DEV-001/event/status", payload=b'{"battery": 85, "led_power": "Green"}')
        )
        mock_insert.assert_called_with(
            "DEV-001",
            "LC1/DEV-001/event/status",
            {"battery": 85, "led_power": "Green"}
        )

def test_message_handling_alert(mqtt_service):
    with patch.object(mqtt_service, 'insert_device_data') as mock_insert:
        mqtt_service.on_message(
            mqtt_service.client,
            None,
            MagicMock(topic="LC1/DEV-001/event/alert", payload=b'{"id": 1, "message": "Low battery"}')
        )
        mock_insert.assert_called_with(
            "DEV-001",
            "LC1/DEV-001/event/alert",
            {"id": 1, "message": "Low battery"}
        )

def test_message_handling_command(mqtt_service):
    with patch.object(mqtt_service, 'insert_command') as mock_insert:
        mqtt_service.on_message(
            mqtt_service.client,
            None,
            MagicMock(topic="LC1/DEV-001/command/config", payload=b'{"operation_id": "get_config"}')
        )
        mock_insert.assert_called_with(
            "DEV-001",
            "LC1/DEV-001/command/config",
            {"operation_id": "get_config"}
        )

def test_message_handling_result(mqtt_service):
    with patch.object(mqtt_service, 'insert_result') as mock_insert:
        mqtt_service.on_message(
            mqtt_service.client,
            None,
            MagicMock(topic="LC1/DEV-001/result", payload=b'{"status": "success", "message": "OK"}')
        )
        mock_insert.assert_called_with(
            "DEV-001",
            "LC1/DEV-001/result",
            {"status": "success", "message": "OK"}
        )
