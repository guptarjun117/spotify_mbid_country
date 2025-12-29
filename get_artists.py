from spotify_client import get_spotify_client


def get_unique_artists_from_playlist(playlist_url, max_tracks=None):
    """
    Fetches unique artists from a Spotify playlist.
    For tracks with multiple artists, only takes the FIRST artist.
    Returns list of dicts with 'artist_name' and 'spotify_link'.
    """
    sp = get_spotify_client()
    if not sp:
        raise RuntimeError("Spotify client is not initialized")

    if "playlist/" in playlist_url:
        playlist_uri = playlist_url.split("playlist/")[-1].split("?")[0]
    else:
        playlist_uri = playlist_url

    print("Fetching data for playlist...")

    results = sp.playlist_tracks(playlist_uri)
    tracks = results["items"]

    while results["next"] and (max_tracks is None or len(tracks) < max_tracks):
        results = sp.next(results)
        tracks.extend(results["items"])

    if max_tracks:
        tracks = tracks[:max_tracks]

    unique_artists = {}

    for item in tracks:
        track = item.get("track")
        if track and track["artists"]:
            artist = track["artists"][0]
            artist_id = artist["id"]
            artist_name = artist["name"]

            if artist_id not in unique_artists:
                artist_info = sp.artist(artist_id)
                unique_artists[artist_id] = {
                    "artist_name": artist_name,
                    "spotify_link": artist_info["external_urls"]["spotify"],
                }

    print(f"Found {len(unique_artists)} unique artists (first artist per track only)")

    artists_list = list(unique_artists.values())
    artists_list.sort(key=lambda x: x["artist_name"].lower())

    return artists_list
