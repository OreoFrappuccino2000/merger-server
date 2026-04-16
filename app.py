import os
import uuid
import time
import shutil
import base64
import tempfile
import logging
import threading
import subprocess
import requests
import json
from flask import Flask, request, jsonify, send_file, abort

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.url_map.strict_slashes = False

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
FRAME_MAX_WIDTH = int(os.environ.get("FRAME_MAX_WIDTH", "512"))
FRAME_JPEG_QUALITY = int(os.environ.get("FRAME_JPEG_QUALITY", "8"))  # ffmpeg q:v 2(best)-31(worst)
MAX_FRAMES_DEFAULT = int(os.environ.get("MAX_FRAMES", "10"))
FRAMES_STORE = os.path.join(tempfile.gettempdir(), "frame_store")
FRAME_TTL_SECONDS = 600  # auto-delete frames after 10 minutes
os.makedirs(FRAMES_STORE, exist_ok=True)

# ---------------------------------------------------------------------------
# Resolve ffmpeg / ffprobe binary paths
# Priority: env var > bundled bin/ > imageio-ffmpeg > system PATH
# ---------------------------------------------------------------------------
def _resolve_ffmpeg():
    """Find a working ffmpeg binary."""
    # 1. Env var
    env_ff = os.environ.get("FFMPEG_BIN", "")
    if env_ff and shutil.which(env_ff):
        return env_ff
    # 2. Bundled
    bundled = os.path.join(os.path.dirname(__file__), "bin", "ffmpeg")
    if os.path.exists(bundled):
        os.chmod(bundled, 0o755)
        return bundled
    # 3. imageio-ffmpeg (pip-installed, ships its own static binary)
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        pass
    # 4. System PATH fallback
    return "ffmpeg"

def _resolve_ffprobe():
    """Find a working ffprobe binary."""
    env_fp = os.environ.get("FFPROBE_BIN", "")
    if env_fp and shutil.which(env_fp):
        return env_fp
    # ffprobe usually lives next to ffmpeg
    ffmpeg_dir = os.path.dirname(FFMPEG_BIN)
    candidate = os.path.join(ffmpeg_dir, "ffprobe")
    if os.path.isfile(candidate):
        os.chmod(candidate, 0o755)
        return candidate
    if shutil.which("ffprobe"):
        return "ffprobe"
    # No ffprobe available — we'll fall back to ffmpeg-based duration detection
    return None

FFMPEG_BIN = _resolve_ffmpeg()
FFPROBE_BIN = _resolve_ffprobe()
logger.info(f"Using ffmpeg: {FFMPEG_BIN}")
logger.info(f"Using ffprobe: {FFPROBE_BIN}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _cleanup_old_jobs():
    """Remove frame directories older than FRAME_TTL_SECONDS."""
    now = time.time()
    try:
        for name in os.listdir(FRAMES_STORE):
            job_dir = os.path.join(FRAMES_STORE, name)
            if os.path.isdir(job_dir):
                age = now - os.path.getmtime(job_dir)
                if age > FRAME_TTL_SECONDS:
                    shutil.rmtree(job_dir, ignore_errors=True)
                    logger.info(f"Cleaned up expired job: {name}")
    except Exception as e:
        logger.warning(f"Cleanup error: {e}")


def _probe_duration(video_path: str) -> float:
    """Get video duration in seconds via ffprobe, with ffmpeg fallback."""
    if FFPROBE_BIN:
        cmd = [
            FFPROBE_BIN,
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            video_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())

    # Fallback: use ffmpeg to get duration from stderr
    cmd = [FFMPEG_BIN, "-i", video_path, "-f", "null", "-"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    # Parse "Duration: HH:MM:SS.xx" from stderr
    import re
    match = re.search(r"Duration:\s*(\d+):(\d+):(\d+\.\d+)", result.stderr)
    if match:
        h, m, s = float(match.group(1)), float(match.group(2)), float(match.group(3))
        return h * 3600 + m * 60 + s
    raise RuntimeError("Could not determine video duration")


def _extract_single_frame(video_path: str, timestamp: float, output_path: str):
    """Extract one frame as a downscaled JPEG."""
    scale_filter = f"scale='min({FRAME_MAX_WIDTH},iw)':-2"
    cmd = [
        FFMPEG_BIN,
        "-ss", str(timestamp),
        "-i", video_path,
        "-frames:v", "1",
        "-vf", scale_filter,
        "-q:v", str(FRAME_JPEG_QUALITY),
        "-y",
        output_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg error at ts={timestamp}: {result.stderr.strip()}")


def _extract_frames(video_path: str, max_frames: int, job_dir: str) -> dict:
    """Extract frames to job_dir and return metadata."""
    duration = _probe_duration(video_path)
    if duration <= 0:
        raise ValueError("Video duration is 0 or could not be determined.")

    n_frames = max(1, min(max_frames, int(duration)))
    interval = duration / n_frames
    timestamps = [round(interval * (i + 0.5), 3) for i in range(n_frames)]

    extracted = []
    for idx, ts in enumerate(timestamps):
        fname = f"frame_{idx + 1:03d}.jpg"
        out_path = os.path.join(job_dir, fname)
        try:
            _extract_single_frame(video_path, ts, out_path)
        except Exception as e:
            logger.warning(f"Failed to extract frame at ts={ts}: {e}")
            continue

        if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            h = int(ts // 3600)
            m = int((ts % 3600) // 60)
            s = ts % 60
            extracted.append({
                "index": idx + 1,
                "filename": fname,
                "timestamp_seconds": ts,
                "timestamp_formatted": f"{h:02d}:{m:02d}:{s:05.2f}",
            })

    if not extracted:
        raise RuntimeError("ffmpeg produced no output frames.")

    return {"frames": extracted, "duration": round(duration, 3)}


# ---------------------------------------------------------------------------
# Audio-Video Merge Helpers
# ---------------------------------------------------------------------------
MERGE_STORE = os.path.join(tempfile.gettempdir(), "merge_store")
os.makedirs(MERGE_STORE, exist_ok=True)

def _get_audio_duration(audio_path: str) -> float:
    """Get audio duration in seconds."""
    if FFPROBE_BIN:
        cmd = [
            FFPROBE_BIN,
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            audio_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    
    # Fallback: use ffmpeg
    cmd = [FFMPEG_BIN, "-i", audio_path, "-f", "null", "-"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    import re
    match = re.search(r"Duration:\s*(\d+):(\d+):(\d+\.\d+)", result.stderr)
    if match:
        h, m, s = float(match.group(1)), float(match.group(2)), float(match.group(3))
        return h * 3600 + m * 60 + s
    raise RuntimeError("Could not determine audio duration")

def _create_audio_filter_complex(audio_inputs: list, video_duration: float) -> str:
    """Create ffmpeg filter complex for audio merging."""
    filters = []
    inputs = []
    
    for i, audio_info in enumerate(audio_inputs):
        # Calculate actual insert time with delay for optimal effect
        insert_time = audio_info['start_time']
        audio_duration = audio_info['duration']
        
        # Ensure audio doesn't extend beyond video
        if insert_time + audio_duration > video_duration:
            audio_duration = video_duration - insert_time
        
        filters.append(f"[a{i}]adelay={int(insert_time * 1000)}|{int(insert_time * 1000)}[d{i}]")
        inputs.append(f"[d{i}]")
    
    # Mix all delayed audio streams
    if len(inputs) > 1:
        mix_inputs = ''.join(inputs)
        filters.append(f"{mix_inputs}amix=inputs={len(inputs)}:duration=longest[aout]")
    else:
        filters.append(f"{inputs[0]}acopy[aout]")
    
    return ";".join(filters)

def _merge_audio_video(video_path: str, audio_inputs: list, output_path: str) -> dict:
    """Merge video with multiple audio tracks."""
    # Get video duration
    video_duration = _probe_duration(video_path)
    
    # Download and process each audio file
    audio_files = []
    tmp_files = []
    
    try:
        for i, audio_info in enumerate(audio_inputs):
            # Download audio file
            tmp_audio = tempfile.NamedTemporaryFile(delete=False, suffix=".wav", dir="/tmp")
            tmp_audio.close()
            
            resp = requests.get(audio_info['url'], timeout=120, stream=True)
            resp.raise_for_status()
            with open(tmp_audio.name, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
            
            # Get audio duration
            audio_duration = _get_audio_duration(tmp_audio.name)
            audio_info['duration'] = audio_duration
            audio_files.append(tmp_audio.name)
            tmp_files.append(tmp_audio.name)
        
        # Create filter complex
        filter_complex = _create_audio_filter_complex(audio_inputs, video_duration)
        
        # Build ffmpeg command
        cmd = [FFMPEG_BIN, "-i", video_path]
        
        # Add audio inputs
        for audio_file in audio_files:
            cmd.extend(["-i", audio_file])
        
        cmd.extend([
            "-filter_complex", filter_complex,
            "-map", "0:v",
            "-map", "[aout]",
            "-c:v", "copy",  # Copy video stream
            "-c:a", "aac",   # Encode audio to AAC
            "-b:a", "128k",  # Audio bitrate
            "-y",
            output_path
        ])
        
        # Run ffmpeg
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            raise RuntimeError(f"ffmpeg merge failed: {result.stderr.strip()}")
        
        return {
            "success": True,
            "video_duration": video_duration,
            "audio_tracks": len(audio_inputs)
        }
        
    finally:
        # Cleanup temporary audio files
        for tmp_file in tmp_files:
            if os.path.exists(tmp_file):
                os.remove(tmp_file)

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify(status="ok", service="frame-extraction")


@app.route("/extract_frames", methods=["POST"])
def extract_frames():
    """
    Extract frames from a video and return image URLs.

    Accepts JSON:
        video_url:   str  — publicly accessible video URL
        max_frames:  int  — max frames to extract (default 10)

    Returns JSON:
        {
            "success": true,
            "frames": [
                {
                    "index": 1,
                    "url": "https://.../frames/<job_id>/frame_001.jpg",
                    "timestamp_seconds": 1.5,
                    "timestamp_formatted": "00:00:01.50"
                },
                ...
            ],
            "count": 5,
            "duration_seconds": 10.0,
            "job_id": "<uuid>"
        }
    """
    # Cleanup old jobs in background
    threading.Thread(target=_cleanup_old_jobs, daemon=True).start()

    data = request.get_json(silent=True) or {}
    video_url = data.get("video_url", "").strip()
    max_frames = int(data.get("max_frames", MAX_FRAMES_DEFAULT))

    if not video_url:
        return jsonify(success=False, error="Provide video_url"), 400

    if max_frames < 1 or max_frames > 100:
        return jsonify(success=False, error="max_frames must be 1-100"), 400

    # Create job directory
    job_id = uuid.uuid4().hex[:12]
    job_dir = os.path.join(FRAMES_STORE, job_id)
    os.makedirs(job_dir, exist_ok=True)

    tmp_video = None
    try:
        # Download video
        logger.info(f"[{job_id}] Downloading video from: {video_url[:80]}...")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4", dir="/tmp") as f:
            tmp_video = f.name
            resp = requests.get(video_url, timeout=120, stream=True)
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)

        logger.info(f"[{job_id}] Extracting up to {max_frames} frames...")
        result = _extract_frames(tmp_video, max_frames, job_dir)

        # Build public URLs for each frame
        # Use the Host header so URLs work regardless of domain
        base_url = request.url_root.rstrip("/")
        frames_out = []
        for fr in result["frames"]:
            frames_out.append({
                "index": fr["index"],
                "url": f"{base_url}/frames/{job_id}/{fr['filename']}",
                "timestamp_seconds": fr["timestamp_seconds"],
                "timestamp_formatted": fr["timestamp_formatted"],
            })

        logger.info(f"[{job_id}] Done — {len(frames_out)} frames extracted")
        return jsonify(
            success=True,
            frames=frames_out,
            count=len(frames_out),
            duration_seconds=result["duration"],
            job_id=job_id,
        )

    except requests.exceptions.RequestException as e:
        logger.error(f"[{job_id}] Download failed: {e}")
        shutil.rmtree(job_dir, ignore_errors=True)
        return jsonify(success=False, error=f"Failed to download video: {str(e)}"), 400
    except Exception as e:
        logger.exception(f"[{job_id}] Extraction failed")
        shutil.rmtree(job_dir, ignore_errors=True)
        return jsonify(success=False, error=str(e)), 500
    finally:
        if tmp_video and os.path.exists(tmp_video):
            os.remove(tmp_video)


@app.route("/frames/<job_id>/<filename>", methods=["GET"])
def serve_frame(job_id, filename):
    """Serve an extracted frame as a JPEG image."""
    # Sanitize inputs to prevent path traversal
    if ".." in job_id or ".." in filename:
        abort(400)

    frame_path = os.path.join(FRAMES_STORE, job_id, filename)
    logger.info(f"Serving frame: {frame_path}, exists={os.path.isfile(frame_path)}")
    if not os.path.isfile(frame_path):
        # List what's actually in the store for debugging
        try:
            jobs = os.listdir(FRAMES_STORE)
            logger.info(f"Available jobs in store: {jobs}")
            if job_id in jobs:
                files = os.listdir(os.path.join(FRAMES_STORE, job_id))
                logger.info(f"Files in job {job_id}: {files}")
        except Exception:
            pass
        abort(404)

    return send_file(frame_path, mimetype="image/jpeg")


@app.route("/merge_audio_video", methods=["POST"])
def merge_audio_video():
    """
    Merge audio tracks with video at specified timestamps.
    
    Accepts JSON:
        video_url: str - original video URL
        audio_inputs: list - list of audio inputs with url and start_time
        
    Example:
        {
            "video_url": "https://example.com/video.mp4",
            "audio_inputs": [
                {"url": "https://example.com/audio1.wav", "start_time": 0.0},
                {"url": "https://example.com/audio2.wav", "start_time": 4.0}
            ]
        }
    """
    data = request.get_json(silent=True) or {}
    video_url = data.get("video_url", "").strip()
    audio_inputs = data.get("audio_inputs", [])
    
    if not video_url:
        return jsonify(success=False, error="Provide video_url"), 400
    
    if not audio_inputs or not isinstance(audio_inputs, list):
        return jsonify(success=False, error="Provide audio_inputs array"), 400
    
    # Validate audio inputs
    for i, audio in enumerate(audio_inputs):
        if not audio.get('url') or not isinstance(audio.get('start_time'), (int, float)):
            return jsonify(success=False, error=f"Invalid audio input at index {i}"), 400
    
    # Create job directory
    job_id = uuid.uuid4().hex[:12]
    job_dir = os.path.join(MERGE_STORE, job_id)
    os.makedirs(job_dir, exist_ok=True)
    
    output_file = os.path.join(job_dir, "merged_video.mp4")
    tmp_video = None
    
    try:
        # Download video
        logger.info(f"[{job_id}] Downloading video from: {video_url[:80]}...")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4", dir="/tmp") as f:
            tmp_video = f.name
            resp = requests.get(video_url, timeout=120, stream=True)
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
        
        # Merge audio and video
        logger.info(f"[{job_id}] Merging {len(audio_inputs)} audio tracks with video...")
        result = _merge_audio_video(tmp_video, audio_inputs, output_file)
        
        if not os.path.exists(output_file) or os.path.getsize(output_file) == 0:
            raise RuntimeError("Merge operation produced no output")
        
        # Generate download URL
        base_url = request.url_root.rstrip("/")
        download_url = f"{base_url}/download_merged/{job_id}/merged_video.mp4"
        
        logger.info(f"[{job_id}] Merge completed successfully")
        return jsonify({
            "success": True,
            "download_url": download_url,
            "job_id": job_id,
            "video_duration": result["video_duration"],
            "audio_tracks": result["audio_tracks"]
        })
        
    except requests.exceptions.RequestException as e:
        logger.error(f"[{job_id}] Download failed: {e}")
        shutil.rmtree(job_dir, ignore_errors=True)
        return jsonify(success=False, error=f"Failed to download video: {str(e)}"), 400
    except Exception as e:
        logger.exception(f"[{job_id}] Merge failed")
        shutil.rmtree(job_dir, ignore_errors=True)
        return jsonify(success=False, error=str(e)), 500
    finally:
        if tmp_video and os.path.exists(tmp_video):
            os.remove(tmp_video)

@app.route("/download_merged/<job_id>/<filename>", methods=["GET"])
def download_merged(job_id, filename):
    """Download merged video file."""
    if ".." in job_id or ".." in filename:
        abort(400)
    
    file_path = os.path.join(MERGE_STORE, job_id, filename)
    if not os.path.isfile(file_path):
        abort(404)
    
    return send_file(file_path, mimetype="video/mp4", as_attachment=True)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
