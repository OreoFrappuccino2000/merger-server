import os
import uuid
import subprocess
import requests
from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import FileResponse

app = FastAPI()

TMP_DIR = "/tmp"
BASE_URL = "https://merger-server-production.up.railway.app"


# ---------- helpers ----------

def download_file(url: str, out_path: str):
    try:
        r = requests.get(url, stream=True, allow_redirects=True, timeout=60)
        r.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to download: {url}")

    content_type = r.headers.get("content-type", "")
    if "text/html" in content_type:
        raise HTTPException(
            status_code=400,
            detail=f"URL does not return raw binary: {url}"
        )

    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)


# ---------- POST: merge ----------

@app.post("/merge")
def merge(
    video_url: str = Form(...),
    audio_url: str = Form(...)
):
    job_id = str(uuid.uuid4())

    video_path = f"{TMP_DIR}/{job_id}_video.mp4"
    audio_path = f"{TMP_DIR}/{job_id}_audio.wav"
    output_path = f"{TMP_DIR}/{job_id}_final.mp4"

    # 1. download assets
    download_file(video_url, video_path)
    download_file(audio_url, audio_path)

    # 2. merge with ffmpeg (replace audio track)
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
            "-af", "aresample=async=1:first_pts=0",
            "-movflags", "+faststart",
            "-shortest",
            output_path
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE
    )
    except subprocess.CalledProcessError as e:
        raise HTTPException(
            status_code=500,
            detail=e.stderr.decode()
        )

    # 3. return direct download URL
    return {
        "job_id": job_id,
        "video_url": f"{BASE_URL}/download/{job_id}"
    }


# ---------- GET: direct mp4 download ----------

@app.get("/download/{job_id}")
def download_video(job_id: str):
    video_path = f"{TMP_DIR}/{job_id}_final.mp4"

    if not os.path.exists(video_path):
        raise HTTPException(status_code=404, detail="Video not found")

    return FileResponse(
        video_path,
        media_type="video/mp4",
        filename=f"{job_id}.mp4"
    )
