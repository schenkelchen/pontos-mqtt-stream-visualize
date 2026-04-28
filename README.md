# Ferry live position from MQTT to Streamlit map

This repository contains a small two-step Python solution that listens to live MQTT position messages, stores them in a local SQLite database, and displays the latest ferry position in a Streamlit map view.[1][2]

## What the solution does

The first script, `01d_pontos_mqtt_stream-to-sqlite.py`, connects to the PONTOS MQTT broker over secure WebSockets, subscribes to latitude and longitude topics for IMO `7932018`, parses incoming JSON payloads, and writes each message into a local SQLite database called `pontos_mqtt.db`.[1]

The second script, `02d_streamlit_sqlite-to-live-map.py`, reads the most recent latitude and longitude pair from the `mqtt_messages` table, combines them by payload timestamp, and renders the latest vessel position in a Streamlit app using PyDeck.[2]

Together, the scripts form a simple pipeline:

1. MQTT stream -> SQLite database.[1]
2. SQLite database -> Streamlit live map.[2]

## How it works

### 1. MQTT ingestion

The ingestion script uses `paho-mqtt` with WebSocket transport, TLS, and the broker endpoint `pontos.ri.se:443/mqtt`.[1]

It authenticates with username `__token__` and reads the password from the environment variable `PONTOS_PASSWORD` via `python-dotenv`.[1]

For each incoming MQTT message, the script:

- Decodes the payload as UTF-8 text.[1]
- Tries to parse the payload as JSON.[1]
- Extracts `value` and `timestamp`.[1]
- Converts the Unix timestamp to UTC ISO format.[1]
- Stores topic, payload, value, timestamps, QoS, and retain flag in SQLite.[1]

### 2. Local storage

On startup, the ingestion script recreates `pontos_mqtt.db`, enables WAL mode, and creates a table named `mqtt_messages` for the incoming records.[1]

This means the database is built fresh when the collector starts, which is useful for a clean local run but also means previous data is deleted on each restart.[1]

### 3. Live visualization

The Streamlit app expects the local database file `pontos_mqtt.db` and reads from the `mqtt_messages` table.[2]

It selects the newest latitude row and longitude row that share the same `payload_utc`, validates coordinate ranges, and shows the latest position on a map with an automatic refresh interval of 15 seconds.[2]

## File overview

| File | Purpose |
|------|---------|
| `01d_pontos_mqtt_stream-to-sqlite.py` | Subscribes to MQTT topics and stores incoming vessel position messages in SQLite.[1] |
| `02d_streamlit_sqlite-to-live-map.py` | Reads the latest position from SQLite and shows it in a Streamlit live map.[2] |

## Run locally

### Prerequisites

- Python 3.10+ is recommended.
- A PONTOS access token/password.
- Internet access to reach the MQTT broker.

### 1. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

On Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

```bash
pip install streamlit pandas pydeck paho-mqtt python-dotenv
```

### 3. Create a `.env` file

Create a file named `.env` in the project root:

```env
PONTOS_PASSWORD=your_token_here
```

The ingestion script loads this variable automatically with `load_dotenv()` and uses it as the MQTT password.[1]

### 4. Start the MQTT collector

Run this in one terminal:

```bash
python 01d_pontos_mqtt_stream-to-sqlite.py
```

This creates `pontos_mqtt.db` and keeps listening for new messages.[1]

### 5. Start the Streamlit app

Run this in a second terminal:

```bash
streamlit run 02d_streamlit_sqlite-to-live-map.py
```

Then open the local URL shown by Streamlit in the terminal, usually `http://localhost:8501`.

## How to retrieve the password

The MQTT password is not hardcoded in the scripts. Instead, the collector reads it from the environment variable `PONTOS_PASSWORD`, and the code comment points to `https://pontos.ri.se/get_started` as the place where the password/token is issued publicly.[1]

To retrieve it:

1. Open `https://pontos.ri.se/get_started`.[1]
2. Follow the instructions on that page to obtain the public token/password.
3. Put the retrieved value into your `.env` file as `PONTOS_PASSWORD=...`.[1]

## Notes

- The Streamlit app uses `DB_PATH = "pontos_mqtt.db"`, so both scripts should be run from the same project folder unless the database path is adjusted in code.[2]
- The app is currently configured for table name `mqtt_messages`.[2]
- The map refresh interval is set to 15 seconds.[2]
- The vessel label shown in the UI is `Road traffic ferry 'ADA' (IMO 7932018)`.[2]
