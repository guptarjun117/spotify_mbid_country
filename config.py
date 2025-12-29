import os

PLAYLIST_URL = "https://open.spotify.com/playlist/5HsCtlOfnz73LDwOOO8Npn?si=MldmiuchSvqonnPiRDRPFg&pi=9X8rqJCgRZWDd"

# https://developer.spotify.com/documentation/web-api

SPOTIFY_CLIENT_ID = "fb61282fafb94811976b38138df76cb4"      # Replace with your Spotify Client ID
SPOTIFY_CLIENT_SECRET = "feeba104de8e44ea8f2fdd8246027e79"  # Replace with your Spotify Client Secret
LISTENBRAINZ_TOKEN = "67b66a3a-42d5-4888-a9a1-5ba39952c4c2"

OUTPUT_CSV = "countries.csv"

USER_AGENT = "CountryExtractorBot/1.0 (MusicBrainzCountryApp@gmail.com)"

DB_FILE = "musicbrainz_sqlite_cache.db"

LEGACY_PICKLE_CACHE_FILE = "musicbrainz_cache.pkl"
MIGRATE_PICKLE_TO_SQLITE = True

MB_MIN_INTERVAL_SECONDS = 1.05
LB_MIN_INTERVAL_SECONDS = 0.20

MAX_WORKERS = 8
SAVE_EVERY_N_ARTISTS = 25

STEP1B_TOP_N = 2
STEP1B_MIN_SCORE = 45
STEP1B_DO_RECORDING_CHECK_IF_AMBIGUOUS_ONLY = True
STEP1B_URLRELS_SCORE_GATE = 60
STEP1B_CLOSE_SCORE_DELTA = 8

BASE_DIR = None

def set_base_dir(path):
    global BASE_DIR
    BASE_DIR = path


def resolve_path(name):
    if not BASE_DIR:
        return name
    return os.path.join(BASE_DIR, name)
