"""Display the latest vessel position from SQLite in a Streamlit map view.

This module reads latitude and longitude messages stored in a local SQLite
file, combines the latest matching coordinate pair, and renders the result in a
Streamlit dashboard with automatic refresh.
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pydeck as pdk
import streamlit as st

DB_PATH = "pontos_mqtt.db"
TABLE_NAME = "mqtt_messages"
REFRESH_SECONDS = 15
VESSEL_LABEL = "Road traffic ferry 'ADA' (IMO 7932018)"

st.set_page_config(
    page_title="Ferry Live Position",
    page_icon="🛳️",
    layout="wide",
)

st.title("🛳️ Road Traffic Ferry - Latest Position")


@st.cache_data(ttl=2)
def get_latest_position(db_path: str) -> pd.DataFrame:
    """Fetch the newest valid latitude and longitude pair from SQLite.

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        pd.DataFrame: DataFrame containing at most one latest coordinate pair.

    Raises:
        FileNotFoundError: If the SQLite database file does not exist.
    """
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    query = f"""
    WITH lat AS (
        SELECT
            payload_utc,
            value AS latitude,
            received_at_utc AS lat_received_at_utc
        FROM {TABLE_NAME}
        WHERE topic LIKE '%latitude%'
          AND value IS NOT NULL
          AND payload_utc IS NOT NULL
    ),
    lon AS (
        SELECT
            payload_utc,
            value AS longitude,
            received_at_utc AS lon_received_at_utc
        FROM {TABLE_NAME}
        WHERE topic LIKE '%longitude%'
          AND value IS NOT NULL
          AND payload_utc IS NOT NULL
    )
    SELECT
        lat.payload_utc,
        lat.latitude,
        lon.longitude,
        lat.lat_received_at_utc,
        lon.lon_received_at_utc
    FROM lat
    JOIN lon
        ON lat.payload_utc = lon.payload_utc
    WHERE lat.latitude BETWEEN -90 AND 90
      AND lon.longitude BETWEEN -180 AND 180
    ORDER BY lat.payload_utc DESC
    LIMIT 1
    """

    with sqlite3.connect(db_path, timeout=1) as conn:
        df = pd.read_sql_query(query, conn)

    if not df.empty:
        df["payload_utc"] = pd.to_datetime(
            df["payload_utc"],
            utc=True,
            errors="coerce",
        )
        df["lat_received_at_utc"] = pd.to_datetime(
            df["lat_received_at_utc"],
            utc=True,
            errors="coerce",
        )
        df["lon_received_at_utc"] = pd.to_datetime(
            df["lon_received_at_utc"],
            utc=True,
            errors="coerce",
        )
        # Use the newest MQTT arrival time from the matching coordinate pair.
        df["mqtt_updated"] = df[
            ["lat_received_at_utc", "lon_received_at_utc"]
        ].max(axis=1)

    return df


def fmt_ts(timestamp_value: Any) -> str:
    """Format timestamps for display in the Streamlit UI.

    Args:
        timestamp_value: Timestamp-like value to format.

    Returns:
        str: Human-readable UTC timestamp or ``N/A``.
    """
    if pd.isna(timestamp_value):
        return "N/A"

    if isinstance(timestamp_value, pd.Timestamp):
        return timestamp_value.strftime("%Y-%m-%d %H:%M:%S UTC")

    return str(timestamp_value)


def norm(value: Any) -> str | float | None:
    """Normalize values before storing them in Streamlit session state.

    Args:
        value: Value to normalize.

    Returns:
        str | float | None: Comparable normalized representation.
    """
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if isinstance(value, float):
        return round(value, 6)

    return str(value)


def render_row(
    container: Any,
    label: str,
    value: Any,
    changed: bool = False,
    flash_cycle: int = 0,
) -> None:
    """Render a single key-value row in the side information panel.

    Args:
        container: Streamlit container used for rendering.
        label: Row label text.
        value: Row value text.
        changed: Whether the row should animate as updated.
        flash_cycle: Unique animation cycle identifier.
    """
    css_class = f"flash-{flash_cycle}" if changed else ""
    container.markdown(
        f"""
        <div class="value-row {css_class}">
            <span class="value-label">{label}</span>
            <span>{value}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


if "previous_values" not in st.session_state:
    st.session_state.previous_values = {}

if "flash_cycle" not in st.session_state:
    st.session_state.flash_cycle = 0


@st.fragment(run_every=REFRESH_SECONDS)
def live_view() -> None:
    """Render the live map and latest position details."""
    streamlit_updated = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    latest_df = get_latest_position(DB_PATH)

    if latest_df.empty:
        st.warning("No matching latitude/longitude pair found.")
        return

    row = latest_df.iloc[0]
    lat = float(row["latitude"])
    lon = float(row["longitude"])

    current_values = {
        "latitude": norm(lat),
        "longitude": norm(lon),
        "payload_utc": norm(row["payload_utc"]),
        "mqtt_updated": norm(row["mqtt_updated"]),
        "streamlit_updated": streamlit_updated,
    }

    previous_values = st.session_state.previous_values
    changed = {
        key: (
            key in previous_values
            and previous_values[key] != current_values[key]
        )
        for key in current_values
    }

    if any(changed.values()):
        st.session_state.flash_cycle += 1

    flash_cycle = st.session_state.flash_cycle

    st.markdown(
        f"""
        <style>
        .value-row {{
            padding: 0.42rem 0.55rem;
            margin: 0.18rem 0;
            border-radius: 0.5rem;
        }}

        .value-label {{
            font-weight: 600;
            display: inline-block;
            min-width: 145px;
        }}

        @keyframes flashFade{flash_cycle} {{
            0% {{
                background-color: rgba(255, 214, 10, 0.00);
                box-shadow: 0 0 0 rgba(255, 214, 10, 0.00);
            }}
            25% {{
                background-color: rgba(255, 214, 10, 0.65);
                box-shadow: 0 0 0.55rem rgba(255, 214, 10, 0.45);
            }}
            100% {{
                background-color: rgba(255, 214, 10, 0.00);
                box-shadow: 0 0 0 rgba(255, 214, 10, 0.00);
            }}
        }}

        .flash-{flash_cycle} {{
            animation: flashFade{flash_cycle} 1.0s ease-out;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    col_map, col_info = st.columns([3, 1])

    with col_map:
        map_df = pd.DataFrame(
            [
                {
                    "latitude": lat,
                    "longitude": lon,
                    "name": VESSEL_LABEL,
                }
            ]
        )

        deck = pdk.Deck(
            initial_view_state=pdk.ViewState(
                latitude=lat,
                longitude=lon,
                zoom=12,
                pitch=0,
            ),
            layers=[
                pdk.Layer(
                    "ScatterplotLayer",
                    data=map_df,
                    get_position="[longitude, latitude]",
                    get_radius=90,
                    get_fill_color=[0, 122, 255, 220],
                    pickable=True,
                )
            ],
            tooltip={"text": "{name}\nLat: {latitude}\nLon: {longitude}"},
            map_style="light",
        )

        st.pydeck_chart(deck, width="stretch", height=600)

    with col_info:
        st.subheader("Latest position")
        info_box = st.container()
        render_row(info_box, "Latitude", f"{lat:.6f}", changed["latitude"], flash_cycle)
        render_row(info_box, "Longitude", f"{lon:.6f}", changed["longitude"], flash_cycle)
        render_row(
            info_box,
            "Observed UTC",
            fmt_ts(row["payload_utc"]),
            changed["payload_utc"],
            flash_cycle,
        )
        render_row(
            info_box,
            "MQTT Updated",
            fmt_ts(row["mqtt_updated"]),
            changed["mqtt_updated"],
            flash_cycle,
        )
        render_row(
            info_box,
            "Streamlit Updated",
            streamlit_updated,
            changed["streamlit_updated"],
            flash_cycle,
        )
        st.write(f"Refresh interval: {REFRESH_SECONDS} seconds")

    st.session_state.previous_values = current_values


try:
    live_view()
except Exception as exc:
    st.error(str(exc))
