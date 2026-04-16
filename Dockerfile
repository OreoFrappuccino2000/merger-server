FROM python:3.11-slim

# Install system dependencies (ffmpeg for audio/video processing)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all files for Flask app or specific files for FastAPI
COPY . .

EXPOSE 5001 8080

# Support both Flask (gunicorn) and FastAPI (uvicorn) startup
CMD ["sh", "-c", "if [ -f audio_video_merge_service.py ]; then gunicorn --bind 0.0.0.0:5001 --timeout 300 --workers 2 audio_video_merge_service:app; else uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}; fi"]
