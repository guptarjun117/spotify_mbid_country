import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

import config

_spotify_client = None


def get_spotify_client():
    global _spotify_client
    if _spotify_client is not None:
        return _spotify_client

    try:
        auth_manager = SpotifyClientCredentials(
            client_id=config.SPOTIFY_CLIENT_ID,
            client_secret=config.SPOTIFY_CLIENT_SECRET,
        )
        _spotify_client = spotipy.Spotify(auth_manager=auth_manager)
    except Exception:
        _spotify_client = None

    return _spotify_client
