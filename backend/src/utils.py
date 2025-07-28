import psycopg2
from decouple import config
import smtplib
from email.message import EmailMessage


def get_db_connection():
    return psycopg2.connect(
        dbname=config("DB_NAME", default="mqtt_db"),
        user=config("DB_USER", default="mqtt_user"),
        password=config("DB_PASSWORD"),
        host=config("DB_HOST", default="91.134.90.10"),
        port=config("DB_PORT", default="5432")
    )

def send_email_alert(subject, body, recipient):
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = config("SMTP_SENDER")
    msg['To'] = recipient
    with smtplib.SMTP(config("SMTP_HOST", default="ssl0.ovh.net"), 
                      config("SMTP_PORT", default=587)) as server:
        server.starttls()
        server.login(config("SMTP_USER"), config("SMTP_PASSWORD"))
        server.send_message(msg)
