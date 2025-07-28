# Locacoeur Monitor Backend

Backend service for Locacoeur Cloud API 1.0.2, handling MQTT communication, PostgreSQL storage, and alerts.

## Setup
1. Install dependencies: `pip install -r requirements.txt`
2. Set up environment variables in `.env`
3. Initialize database: `psql -h 91.134.90.10 -U mqtt_user -d mqtt_db -f config/init_db.sql`
4. Run MQTT service: `python src/mqtt_to_postgres.py`
5. Run API: `uvicorn src.api:app --host 0.0.0.0 --port 8000`

## Requirements
- Python 3.8+
- PostgreSQL
- MQTT broker access
