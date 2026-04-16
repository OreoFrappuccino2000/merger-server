import os
import uuid
import time
import shutil
import tempfile
import logging
import subprocess
import requests
from flask import Flask, request, jsonify, send_file, abort

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.url_map.strict_slashes = False

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MERGE_STORE = os.path.join(tempfile.gettempdir(), "audio_video_merge_store")
os.makedirs(MERGE_STORE, exist_ok=True)
MERGE_TTL_SECONDS = 3600  # auto-delete merged files after 1 hour

# ---------------------------------------------------------------------------
# Resolve ffmpeg / ffprobe binary paths
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
    # 3. imageio-ffmpeg
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
    return None

FFMPEG_BIN = _resolve_ffmpeg()
FFPROBE_BIN = _resolve_ffprobe()
logger.info(f"Using ffmpeg: {FFMPEG_BIN}")
logger.info(f"Using ffprobe: {FFPROBE_BIN}")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _cleanup_old_merges():
    """Remove merged files older than MERGE_TTL_SECONDS."""
    now = time.time()
    try:
        for name in os.listdir(MERGE_STORE):
            job_dir = os.path.join(MERGE_STORE, name)
            if os.path.isdir(job_dir):
                age = now - os.path.getmtime(job_dir)
                if age > MERGE_TTL_SECONDS:
                    shutil.rmtree(job_dir, ignore_errors=True)
                    logger.info(f"Cleaned up expired merge job: {name}")
    except Exception as e:
        logger.warning(f"Cleanup error: {e}")

def _probe_duration(media_path: str) -> float:
    """Get media duration in seconds via ffprobe, with ffmpeg fallback."""
    if FFPROBE_BIN:
        cmd = [
            FFPROBE_BIN,
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            media_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    
    # Fallback: use ffmpeg
    cmd = [FFMPEG_BIN, "-i", media_path, "-f", "null", "-"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    import re
    match = re.search(r"Duration:\s*(\d+):(\d+):(\d+\.\d+)", result.stderr)
    if match:
        h, m, s = float(match.group(1)), float(match.group(2)), float(match.group(3))
        return h * 3600 + m * 60 + s
    raise RuntimeError("Could not determine media duration")

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
        
        filters.append(f"[{i+1}:a]adelay={int(insert_time * 1000)}|{int(insert_time * 1000)}[d{i}]")
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
            audio_duration = _probe_duration(tmp_audio.name)
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
    return jsonify(status="ok", service="audio-video-merge")

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
    # Cleanup old jobs in background
    import threading
    threading.Thread(target=_cleanup_old_merges, daemon=True).start()
    
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
    port = int(os.environ.get("PORT", 5001))  # Different port than app.py
    app.run(host="0.0.0.0", port=port, debug=False)
