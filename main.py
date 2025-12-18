import os
import uuid
import subprocess
import requests
from fastapi import FastAPI, Form, HTTPException, Request, Response
from fastapi.responses import StreamingResponse

app = FastAPI()

TMP_DIR = "/tmp"
BASE_URL = "https://merger-server-production.up.railway.app"


# ---------- helpers ----------

def download_file(url: str, out_path: str):
    """
    Downloads a URL to a local path. Fails fast if the URL returns HTML.
    """
    try:
        r = requests.get(url, stream=True, allow_redirects=True, timeout=60)
        r.raise_for_status()
    except Exception:
        raise HTTPException(status_code=400, detail=f"Failed to download: {url}")

    content_type = (r.headers.get("content-type") or "").lower()
    if "text/html" in content_type:
        raise HTTPException(status_code=400, detail=f"URL does not return raw binary: {url}")

    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)


def ensure_ready(path: str, min_bytes: int = 10_000):
    """
    Ensure the file exists and is non-trivially sized (prevents returning URL too early).
    """
    if not os.path.exists(path):
        raise HTTPException(status_code=500, detail="Output video not created")
    if os.path.getsize(path) < min_bytes:
        raise HTTPException(status_code=500, detail="Output video too small / not ready")


def file_iterator(path: str, start: int = 0, end: int = None, chunk_size: int = 1024 * 1024):
    """
    Generator yielding file bytes from [start, end] inclusive.
    """
    with open(path, "rb") as f:
        f.seek(start)
        remaining = (end - start + 1) if end is not None else None

        while True:
            if remaining is not None and remaining <= 0:
                break

            read_size = chunk_size if remaining is None else min(chunk_size, remaining)
            data = f.read(read_size)
            if not data:
                break

            if remaining is not None:
                remaining -= len(data)

            yield data


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

    # 1) download assets
    download_file(video_url, video_path)
    download_file(audio_url, audio_path)

    # 2) merge with ffmpeg (replace audio track)
    #    - aresample async + first_pts fixes "first second got cut"
    #    - faststart helps MP4 streaming / playback
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
        raise HTTPException(status_code=500, detail=e.stderr.decode(errors="ignore"))

    ensure_ready(output_path)

    # 3) return direct download URL
    return {
        "job_id": job_id,
        "video_url": f"{BASE_URL}/download/{job_id}"
    }


# ---------- HEAD: check download exists (optional but useful) ----------

@app.head("/download/{job_id}")
def head_download(job_id: str, response: Response):
    video_path = f"{TMP_DIR}/{job_id}_final.mp4"
    if not os.path.exists(video_path):
        raise HTTPException(status_code=404, detail="Video not found")

    size = os.path.getsize(video_path)
    response.headers["Content-Length"] = str(size)
    response.headers["Accept-Ranges"] = "bytes"
    response.headers["Content-Type"] = "video/mp4"
    response.headers["Cache-Control"] = "no-store"
    return


# ---------- GET: streaming mp4 download with Range support ----------

@app.get("/download/{job_id}")
def download_video(job_id: str, request: Request):
    video_path = f"{TMP_DIR}/{job_id}_final.mp4"
    if not os.path.exists(video_path):
        raise HTTPException(status_code=404, detail="Video not found")

    file_size = os.path.getsize(video_path)
    range_header = request.headers.get("range")

    headers = {
        "Content-Type": "video/mp4",
        "Content-Disposition": f'attachment; filename="{job_id}.mp4"',
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-store",
    }

    # Range request (resumable / chunked downloads)
    if range_header:
        # Expected: "bytes=start-end"
        try:
            bytes_range = range_header.replace("bytes=", "").split("-")
            start = int(bytes_range[0]) if bytes_range[0] else 0
            end = int(bytes_range[1]) if len(bytes_range) > 1 and bytes_range[1] else file_size - 1
        except Exception:
            raise HTTPException(status_code=416, detail="Invalid Range header")

        if start >= file_size:
            raise HTTPException(status_code=416, detail="Range start out of bounds")

        end = min(end, file_size - 1)
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        headers["Content-Length"] = str(end - start + 1)

        return StreamingResponse(
            file_iterator(video_path, start=start, end=end),
            status_code=206,
            media_type="video/mp4",
            headers=headers
        )

    # Full download
    headers["Content-Length"] = str(file_size)
    return StreamingResponse(
        file_iterator(video_path, start=0, end=file_size - 1),
        media_type="video/mp4",
        headers=headers
    )
