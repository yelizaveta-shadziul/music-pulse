import streamlit as st
import pandas as pd
import psycopg2
import time

st.set_page_config(page_title="Music Pulse", layout="wide")
st.title("🎵 Music Pulse — Live Analytics")

def get_data():
    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname="musicpulse", user="music", password="music"
    )
    df = pd.read_sql(
        "SELECT * FROM music_metrics ORDER BY window_start DESC LIMIT 50",
        conn
    )
    conn.close()
    return df

placeholder = st.empty()

while True:
    df = get_data()
    with placeholder.container():
        col1, col2, col3 = st.columns(3)
        if not df.empty:
            col1.metric("Average BPM", f"{df['avg_tempo'].iloc[0]:.0f}")
            col2.metric("Energy",     f"{df['avg_energy'].iloc[0]:.2f}")
            col3.metric("Mood",       f"{df['avg_valence'].iloc[0]:.2f}")

            st.subheader("BPM over time")
            st.line_chart(df.set_index("window_start")["avg_tempo"])

            st.subheader("Mood indicator")
            st.progress(float(df['avg_valence'].iloc[0]))
        else:
            st.info("Waiting for data from Spark...")
    time.sleep(10)
    st.rerun()