MQTT_CONFIG = {
    "BROKER_HOST": "localhost",
    "BROKER_PORT": 1883,
    "USE_TLS": False
}

DATABASE_CONFIG = {
    "DB_HOST": "localhost",
    "DB_PORT": 5432,
    "DB_NAME": "locacoeur"
}

APP_CONFIG = {
    "DEBUG": True,
    "HOST": "0.0.0.0",
    "PORT": 8050,
    "LOG_LEVEL": "INFO"
}

def print_config_status():
    print("✅ Configuration loaded successfully")

def validate_config():
    return []
