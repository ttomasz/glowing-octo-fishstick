# glowing-octo-fishstick
Scripts I wrote for myself to help me find songs to play on guitar

# Usage

## Prepare environment
Using [uv](https://docs.astral.sh/uv/getting-started/installation/):
```
uv sync --frozen
```

## .env file needed for spotify integration
Example contents:
```
SPOTIPY_CLIENT_ID=...
SPOTIPY_CLIENT_SECRET=...
SPOTIPY_REDIRECT_URI=http://localhost:8080
```
Requires registering app at: https://developer.spotify.com/dashboard/applications

## Run
```
uv run scrape.py
```
