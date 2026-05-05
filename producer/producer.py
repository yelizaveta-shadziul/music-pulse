import json, time, os, random
from kafka import KafkaProducer
from dotenv import load_dotenv
import pylast

load_dotenv()

network = pylast.LastFMNetwork(
    api_key=os.getenv("LASTFM_API_KEY")
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_tracks():
    top_tracks = network.get_top_tracks(limit=20)
    messages = []
    for track in top_tracks:
        try:
            t = track.item
            messages.append({
                "track_id":   str(hash(t.title + t.artist.name)),
                "track_name": t.title,
                "artist":     t.artist.name,
                "energy":     round(random.uniform(0.5, 1.0), 2),
                "tempo":      round(random.uniform(90, 160), 1),
                "valence":    round(random.uniform(0.3, 0.9), 2),
                "timestamp":  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            })
        except:
            continue
    return messages

while True:
    tracks = get_tracks()
    for track in tracks:
        producer.send('spotify_stream', track)
        print(f"Sent: {track['artist']} — {track['track_name']}")
    print(f"--- Pause 30 seconds ---")
    time.sleep(30)