# ============================================================================
# Dockerfile — Instagram Status Monitor Bot
# ============================================================================
# Much lighter than before: no Playwright/Chromium needed anymore, since
# detection now uses a direct API call instead of browser automation.
#
# IMPORTANT: rename the bot file you want to deploy to "bot.py" before
# building, or update the CMD line below to point at the correct filename
# (bot_full.py or bot_reactivation.py) — these are two SEPARATE bots that
# must be deployed as two separate services with two separate BOT_TOKENs.
#
# BUILD:
#   docker build -t igmonitor .
#
# RUN:
#   docker run -d --name igmonitor --env-file .env \
#       -v $(pwd)/data:/app/data --restart unless-stopped igmonitor
# ============================================================================

FROM python:3.11-slim

WORKDIR /app

# Install DejaVu fonts (needed for Pillow-based stat card image generation)
RUN apt-get update && \
    apt-get install -y --no-install-recommends fonts-dejavu-core && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the bot code. If deploying bot_reactivation.py instead of bot_full.py,
# change this line accordingly (or rename your chosen file to bot.py first).
COPY bot_full.py bot.py

ENV DATA_FILE=/app/data/monitored_accounts.json
RUN mkdir -p /app/data /app/cards

CMD ["python", "bot.py"]
