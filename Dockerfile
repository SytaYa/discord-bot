# ══════════════════════════════════════════════════════════════════
#  Dockerfile — discord-bot v5.2
#  Image : python:3.11-slim (Debian Bookworm)
#  FFmpeg installé via apt-get (on est root dans le build Docker)
#  Compatible Render Web Service (Docker runtime)
# ══════════════════════════════════════════════════════════════════

FROM python:3.11-slim

# ── Métadonnées ────────────────────────────────────────────────────
LABEL maintainer="discord-bot" \
      description="Discord bot with music support (yt-dlp + FFmpeg)"

# ── Variables d'environnement système ─────────────────────────────
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# ── Dépendances système (dont FFmpeg) ─────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        libopus0 \
        libopus-dev \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Répertoire de travail ──────────────────────────────────────────
WORKDIR /app

# ── Dépendances Python ─────────────────────────────────────────────
# Copier requirements en premier pour profiter du cache Docker layer
COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install -r requirements.txt

# ── Code source ────────────────────────────────────────────────────
COPY bot_agent.py .

# Copier bot_data.json s'il existe (migration initiale), sinon ignorer
# Le .dockerignore exclut les fichiers sensibles (.env, secrets, etc.)
COPY . .

# ── Port exposé (health check Render) ─────────────────────────────
EXPOSE 8080

# ── Lancement ──────────────────────────────────────────────────────
CMD ["python", "bot_agent.py"]
