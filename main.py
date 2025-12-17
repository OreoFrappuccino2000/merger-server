import os
import uuid
import subprocess
import requests
from fastapi import FastAPI, UploadFile, File, Form

app = FastAPI()

TMP = "/tmp"

def download(url: str, path: str):
    r = requests.get(url, stream=True)
    r.raise_for_status()
    with open(path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)

@app.post("/merge")
async def merge(
    video_url: str = Form(...),
    audio_url: str = Form(None),
    audio_file: UploadFile = File(None)
):
    job_id = str(uuid.uuid4())

    video_path = f"{TMP}/{job_id}_video.mp4"
    audio_path = f"{TMP}/{job_id}_audio.wav"
    output_path = f"{TMP}/{job_id}_final.mp4"

    # 1. Download video
    download(video_url, video_path)

    # 2. Get audio
    if audio_url:
        download(audio_url, audio_path)
    elif audio_file:
        with open(audio_path, "wb") as f:
            f.write(await audio_file.read())
    else:
        return {"error": "No audio provided"}

    # 3. Merge (replace audio track)
    subprocess.run([
        "ffmpeg", "-y",
        "-i", video_path,
        "-i", audio_path,
        "-map", "0:v:0",
        "-map", "1:a:0",
        "-c:v", "copy",
        "-c:a", "aac",
        "-shortest",
        output_path
    ], check=True)

    return {
        "job_id": job_id,
        "output_file": output_path
    }
