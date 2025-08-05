from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
import time
import datetime
import pandas as pd
import re
import sys

load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
EXCLUDE = ['Mi playlist #40']


def get_all_playlist_tracks(playlist_id: str, playlist_name: str) -> pd.DataFrame:
    """
    Fetches all tracks from a given Spotify playlist and returns them as a 
    DataFrame, sorted chronologically by album release date and then by album name.
    """
    try:
        results = sp.playlist_items(playlist_id)
        tracks = results['items']
        
        while results['next']:
            results = sp.next(results)
            tracks.extend(results['items'])

        # Filter out any potential null tracks (e.g., deleted songs)
        valid_tracks = [item for item in tracks if item and item.get('track')]

        songs = [
            {
                'song_id': item['track']['id'],
                'song_name': item['track']['name'],
                'album_name': item['track']['album']['name'],
                'album_release_date': item['track']['album']['release_date']
            } for item in valid_tracks
        ]
    
        if not songs:
            return pd.DataFrame()

        df = pd.DataFrame(songs)
        df['album_release_date'] = pd.to_datetime(df['album_release_date'], errors='coerce')
        df = df.dropna(subset=['song_id', 'album_release_date'])
        df = df.sort_values(by=['album_release_date', 'album_name'], ascending=[True, True])
        
        # Guardar archivo y formatear el nombre de la playlist
        sanitized_name = re.sub(r'[<>:"/\\|?*]', '', playlist_name)
        backup_dir = r".\Backups"
        os.makedirs(backup_dir, exist_ok=True)
        timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M-%S')
        filename = os.path.join(backup_dir, f"{timestamp}_{sanitized_name}.csv")
        df.to_csv(filename, index=False)
        return df
    
    except Exception as e:
        print(f"Error fetching playlist tracks: {e}")
        return pd.DataFrame()


def remove_all_tracks_from_playlist(playlist_id: str, track_ids: list) -> bool:
    """
    Removes all occurrences of the given track IDs from a playlist.
    Handles batching for lists larger than 100.
    """
    if not track_ids:
        print("No tracks to remove.")
        return False
        
    print(f"Removing {len(track_ids)} tracks in batches...")
    # The API limit for removing items is 100 per call
    for i in range(0, len(track_ids), 100):
        chunk = track_ids[i:i + 100]
        try:
            sp.playlist_remove_all_occurrences_of_items(playlist_id, chunk)
            print(f"Removed batch of {len(chunk)} tracks.")
        except Exception as e:
            print(f"Error removing batch: {e}")
    print("All tracks removed from playlist.")
    return True


def add_songs(playlist_id: str, track_ids: list):
    """
    Adds songs one-by-one with a delay to ensure the 'Date Added' column
    in the Spotify client is correctly ordered. This is intentionally slow.
    """
    if not track_ids:
        print("No tracks to add.")
        return
        
    total_tracks = len(track_ids)
    print(f"Añadiendo {total_tracks} canciones una por una (esto será lento)...")
    for i, song_id in enumerate(track_ids):
        try:
            sp.playlist_add_items(playlist_id, [song_id])
            # Formato mejorado para el progreso
            progress_text = f"Progreso: [ {i + 1} / {total_tracks} ]"
            print(progress_text, end='\r')
            sys.stdout.flush()
            time.sleep(1)
        except Exception as e:
            print(f"\nError añadiendo la canción {song_id}: {e}")
            continue
            
    print(f"\nProceso completado. Se han añadido todas las canciones a la playlist.")


if __name__ == '__main__':
    
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=['playlist-modify-private', 'playlist-read-private', 'playlist-modify-public'])
        )

    # Esto extrae las primeras 50 playlists del usuario, si hay mas hay que hacer otra logica
    user_id = sp.me()['id']
    playlists = sp.user_playlists(user=user_id) 
    playlist_items = playlists['items']

    PLAYLISTS = {p['name']: p['id'] for p in playlist_items if p['name'] not in EXCLUDE}

    for name, id in PLAYLISTS.items():
        print(name)
        track_ids_df = get_all_playlist_tracks(playlist_id=id, playlist_name=name)
        track_ids = track_ids_df['song_id'].to_list()
        remove_all_tracks_from_playlist(playlist_id=id, track_ids=track_ids)
        add_songs(playlist_id=id, track_ids=track_ids)