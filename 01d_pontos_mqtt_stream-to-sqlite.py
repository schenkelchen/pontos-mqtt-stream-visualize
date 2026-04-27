import os
import ssl
import json
import sqlite3
from datetime import datetime, timezone
from dotenv import load_dotenv

import paho.mqtt.client as mqtt

BROKER_HOST = "pontos.ri.se"
BROKER_PORT = 443
WS_PATH = "/mqtt"

load_dotenv()
USERNAME = "__token__"
PASSWORD = os.getenv("PONTOS_PASSWORD") # issued to public on https://pontos.ri.se/get_started

MQTT_TOPICS = [
    ("PONTOS_EGRESS/imo_7932018/positioningsystem_longitude_deg/1", 0),
    ("PONTOS_EGRESS/imo_7932018/positioningsystem_latitude_deg/1", 0),
]

DB_FILE = "pontos_mqtt.db"


def init_db():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=10)
    cur = conn.cursor()

    mode = cur.execute("PRAGMA journal_mode=WAL;").fetchone()[0]
    print(f"journal_mode = {mode}")

    cur.execute("PRAGMA synchronous=NORMAL;")

    cur.execute("""
        CREATE TABLE mqtt_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at_utc TEXT NOT NULL,
            topic TEXT NOT NULL,
            payload_text TEXT,
            value REAL,
            timestamp INTEGER,
            payload_utc TEXT,
            qos INTEGER,
            retain INTEGER
        )
    """)

    conn.commit()
    return conn


def unix_to_utc_text(unix_ts):
    if unix_ts is None:
        return None
    try:
        return datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()
    except (ValueError, OSError, OverflowError, TypeError):
        return None


def save_message(conn, topic, payload_text, value, payload_timestamp, payload_utc, qos, retain):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO mqtt_messages (
            received_at_utc,
            topic,
            payload_text,
            value,
            timestamp,
            payload_utc,
            qos,
            retain
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.now(timezone.utc).isoformat(),
        topic,
        payload_text,
        value,
        payload_timestamp,
        payload_utc,
        qos,
        int(retain)
    ))
    conn.commit()


def on_connect(client, userdata, flags, reason_code, properties=None):
    print(f"Connected with result code: {reason_code}")
    result, mid = client.subscribe(MQTT_TOPICS)
    print(f"Subscribe result={result}, mid={mid}")


def on_message(client, userdata, msg):
    payload_text = msg.payload.decode("utf-8", errors="replace")

    value = None
    payload_timestamp = None
    payload_utc = None

    try:
        payload_json = json.loads(payload_text)
        value = payload_json.get("value")
        payload_timestamp = payload_json.get("timestamp")
        payload_utc = unix_to_utc_text(payload_timestamp)
    except json.JSONDecodeError:
        print(f"Warning: payload is not valid JSON: {payload_text}")

    save_message(
        userdata["db_conn"],
        msg.topic,
        payload_text,
        value,
        payload_timestamp,
        payload_utc,
        msg.qos,
        msg.retain
    )

    print(
        f"[{datetime.now(timezone.utc).isoformat()}] "
        f"{msg.topic} -> value={value}, timestamp={payload_timestamp}, payload_utc={payload_utc}"
    )


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    print(f"Disconnected: {reason_code}")


def main():
    db_conn = init_db()

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="pontos_python_logger",
        transport="websockets"
    )

    client.user_data_set({"db_conn": db_conn})
    client.username_pw_set(USERNAME, PASSWORD)
    client.ws_set_options(path=WS_PATH)

    client.tls_set(
        cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLS_CLIENT
    )
    client.tls_insecure_set(False)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    print("Connecting...")
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client.loop_forever()


if __name__ == "__main__":
    main()