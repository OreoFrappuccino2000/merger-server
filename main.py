import os
import uuid
import subprocess
import requests
from fastapi import FastAPI, Form, HTTPException

app = FastAPI()
TMP = "/tmp"

def download_file(url: str, out_path: str):
    r = requests.get(url, stream=True, allow_redirects=True, timeout=60)
    r.raise_for_status()

    content_type = r.headers.get("content-type", "")
    if "text/html" in content_type:
        raise HTTPException(
            status_code=400,
            detail=f"URL does not return a raw file: {url}"
        )

    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

@app.post("/merge")
def merge(
    video_url: str = Form(...),
    audio_url: str = Form(...)
):
    job_id = str(uuid.uuid4())

    video_path = f"{TMP}/{job_id}_video.mp4"
    audio_path = f"{TMP}/{job_id}_audio.wav"
    output_path = f"{TMP}/{job_id}_final.mp4"

    # 1. Download assets
    download_file(video_url, video_path)
    download_file(audio_url, audio_path)

    # 2. Merge (replace audio track)
    try:
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", video_path,
                "-i", audio_path,
                "-map", "0:v:0",
                "-map", "1:a:0",
                "-c:v", "copy",
                "-c:a", "aac",
                "-shortest",
                output_path
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE
        )
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=e.stderr.decode())

    # 3. TODO: upload to public storage
    # public_url = upload_to_cos(output_path)

    return {
        "job_id": job_id,
        "output_file": output_path
        # replace with:
        # "video_url": public_url
    }
