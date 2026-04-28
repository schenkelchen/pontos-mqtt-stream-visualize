"""Ingest live PONTOS MQTT vessel position messages into a local SQLite database.

This module connects to the PONTOS broker over secure WebSockets, subscribes
to the configured latitude and longitude topics, and stores each received
message in a SQLite table for later visualization.
"""

import json
import os
import sqlite3
import ssl
from datetime import datetime, timezone

from dotenv import load_dotenv
import paho.mqtt.client as mqtt

BROKER_HOST = "pontos.ri.se"
BROKER_PORT = 443
WS_PATH = "/mqtt"
USERNAME = "__token__"
DB_FILE = "pontos_mqtt.db"
MQTT_TOPICS = [
    ("PONTOS_EGRESS/imo_7932018/positioningsystem_longitude_deg/1", 0),
    ("PONTOS_EGRESS/imo_7932018/positioningsystem_latitude_deg/1", 0),
]

load_dotenv()
PASSWORD = os.getenv("PONTOS_PASSWORD")


def init_db() -> sqlite3.Connection:
    """Create a fresh SQLite database and initialize the message table.

    Returns:
        sqlite3.Connection: Open connection to the initialized database.
    """
    if os.path.exists(DB_FILE):
        # Start with a clean local database on each run.
        os.remove(DB_FILE)

    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=10)
    cur = conn.cursor()

    mode = cur.execute("PRAGMA journal_mode=WAL;").fetchone()[0]
    print(f"journal_mode = {mode}")

    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute(
        """
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
        """
    )

    conn.commit()
    return conn


def unix_to_utc_text(unix_ts: int | None) -> str | None:
    """Convert a Unix timestamp to an ISO 8601 UTC string.

    Args:
        unix_ts: Unix timestamp in seconds.

    Returns:
        str | None: UTC timestamp as ISO 8601 text, or None if conversion fails.
    """
    if unix_ts is None:
        return None

    try:
        return datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()
    except (ValueError, OSError, OverflowError, TypeError):
        return None


def save_message(
    conn: sqlite3.Connection,
    topic: str,
    payload_text: str,
    value: float | None,
    payload_timestamp: int | None,
    payload_utc: str | None,
    qos: int,
    retain: bool,
) -> None:
    """Insert a received MQTT message into the SQLite database.

    Args:
        conn: Open SQLite connection.
        topic: MQTT topic name.
        payload_text: Raw decoded MQTT payload.
        value: Parsed numeric value from the JSON payload, if available.
        payload_timestamp: Parsed Unix timestamp from the payload, if available.
        payload_utc: UTC timestamp converted from the payload timestamp.
        qos: MQTT quality-of-service level.
        retain: MQTT retain flag.
    """
    cur = conn.cursor()
    cur.execute(
        """
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
        """,
        (
            datetime.now(timezone.utc).isoformat(),
            topic,
            payload_text,
            value,
            payload_timestamp,
            payload_utc,
            qos,
            int(retain),
        ),
    )
    conn.commit()


def on_connect(client, userdata, flags, reason_code, properties=None) -> None:
    """Subscribe to the configured topics after the MQTT client connects.

    Args:
        client: MQTT client instance.
        userdata: User-defined data passed to callbacks.
        flags: Connection response flags.
        reason_code: Broker connection result.
        properties: MQTT v5 properties.
    """
    del flags, properties

    print(f"Connected with result code: {reason_code}")
    result, mid = client.subscribe(MQTT_TOPICS)
    print(f"Subscribe result={result}, mid={mid}")


def on_message(client, userdata, msg) -> None:
    """Parse and persist an incoming MQTT message.

    Args:
        client: MQTT client instance.
        userdata: User-defined data passed to callbacks.
        msg: MQTT message object.
    """
    del client

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
        msg.retain,
    )

    print(
        f"[{datetime.now(timezone.utc).isoformat()}] "
        f"{msg.topic} -> value={value}, timestamp={payload_timestamp}, "
        f"payload_utc={payload_utc}"
    )


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None) -> None:
    """Log MQTT disconnection events.

    Args:
        client: MQTT client instance.
        userdata: User-defined data passed to callbacks.
        disconnect_flags: MQTT disconnect flags.
        reason_code: Broker disconnect result.
        properties: MQTT v5 properties.
    """
    del client, userdata, disconnect_flags, properties
    print(f"Disconnected: {reason_code}")


def main() -> None:
    """Start the MQTT client and stream messages into SQLite."""
    db_conn = init_db()

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="pontos_python_logger",
        transport="websockets",
    )

    client.user_data_set({"db_conn": db_conn})
    client.username_pw_set(USERNAME, PASSWORD)
    client.ws_set_options(path=WS_PATH)
    client.tls_set(
        cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
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
