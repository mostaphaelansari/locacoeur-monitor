CREATE TABLE Servers (
    server_id SERIAL PRIMARY KEY,
    environment TEXT NOT NULL,
    mqtt_url TEXT NOT NULL,
    UNIQUE (environment)
);

CREATE TABLE Devices (
    serial VARCHAR(50) PRIMARY KEY,
    mqtt_broker_url VARCHAR(255),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Commands (
    command_id SERIAL PRIMARY KEY,
    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
    server_id INTEGER REFERENCES Servers(server_id) ON DELETE RESTRICT,
    operation_id VARCHAR(50),
    topic VARCHAR(255),
    message_id VARCHAR(50),
    payload JSONB,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_get BOOLEAN
);

CREATE TABLE Results (
    result_id SERIAL PRIMARY KEY,
    command_id INTEGER REFERENCES Commands(command_id) ON DELETE SET NULL,
    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
    topic VARCHAR(255),
    result_status VARCHAR(50),
    result_message TEXT,
    payload JSONB,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Events (
    event_id SERIAL PRIMARY KEY,
    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
    server_id INTEGER REFERENCES Servers(server_id) ON DELETE RESTRICT,
    operation_id VARCHAR(50),
    topic VARCHAR(255),
    message_id VARCHAR(50),
    payload JSONB,
    event_timestamp TIMESTAMP WITHOUT TIME ZONE,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    original_timestamp TEXT,
    UNIQUE (device_serial, topic, event_timestamp)
);

CREATE TABLE LEDs (
    led_id SERIAL PRIMARY KEY,
    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
    led_type VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    description TEXT,
    last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT leds_status_check CHECK (status IN ('Green', 'Red', 'Off')),
    UNIQUE (device_serial, led_type)
);

CREATE TABLE device_data (
    id SERIAL PRIMARY KEY,
    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
    topic VARCHAR(255),
    battery INTEGER CHECK (battery >= 0 AND battery <= 100),
    connection VARCHAR(50),
    defibrillator INTEGER CHECK (defibrillator >= 0),
    latitude DOUBLE PRECISION CHECK (latitude BETWEEN -90 AND 90),
    longitude DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),
    power_source VARCHAR(50),
    timestamp BIGINT,
    led_power VARCHAR(50) CHECK (led_power IN ('Green', 'Red')),
    led_defibrillator VARCHAR(50) CHECK (led_defibrillator IN ('Green', 'Red')),
    led_monitoring VARCHAR(50) CHECK (led_monitoring IN ('Green', 'Red')),
    led_assistance VARCHAR(50) CHECK (led_assistance IN ('Green', 'Red')),
    led_mqtt VARCHAR(50) CHECK (led_mqtt IN ('Green', 'Red')),
    led_environmental VARCHAR(50) CHECK (led_environmental IN ('Green', 'Red', 'Off')),
    payload JSONB,
    received_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    alert_id INTEGER,
    alert_message VARCHAR(255),
    original_timestamp BIGINT
);
