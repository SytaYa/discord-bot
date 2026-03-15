# ══════════════════════════════════════════════════════════════════
#  Dockerfile — discord-bot v5.17
#  Image : python:3.11-slim (Debian Bookworm)
#  Musique : Spotify (métadonnées) + Deezer (audio via FFmpeg)
#  FFmpeg lit les URLs MP3/FLAC Deezer directement — pas de yt-dlp
# ══════════════════════════════════════════════════════════════════

FROM python:3.11-slim

LABEL maintainer="discord-bot" \
      description="Discord bot — Spotify + Deezer audio, no YouTube"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# ── Dépendances système ────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        libopus0 \
        libopus-dev \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Dépendances Python ─────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install -r requirements.txt

# ── Code source ────────────────────────────────────────────────────
COPY . .

EXPOSE 8080
CMD ["python", "bot_agent.py"]
