import os
import base64
import logging
import subprocess
import tempfile
import cv2

logger = logging.getLogger(__name__)


def get_video_duration(input_path: str) -> float:
    """Get video duration in seconds using OpenCV."""
    cap = cv2.VideoCapture(input_path)
    if not cap.isOpened():
        raise ValueError(f'Cannot open video file: {input_path}')
    total_frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
    native_fps = cap.get(cv2.CAP_PROP_FPS)
    cap.release()
    if native_fps <= 0:
        return 0.0
    return total_frames / native_fps


def extract_frames_from_file(
    input_path: str,
    fps: float = 1.0,
    max_frames: int = 10,
    fmt: str = 'jpeg',
    quality: int = 85,
) -> dict:
    """
    Extract frames from a video file.

    Args:
        input_path:  Path to the input video file.
        fps:         How many frames to extract per second of video.
        max_frames:  Maximum number of frames to return.
        fmt:         Output image format: 'jpeg' or 'png'.
        quality:     JPEG quality (1-100). Ignored for PNG.

    Returns:
        {
            "frames": [<base64_str>, ...],
            "count": int,
            "fps_extracted": float,
            "duration_seconds": float,
        }
    """
    cap = cv2.VideoCapture(input_path)
    if not cap.isOpened():
        raise ValueError(f'Cannot open video file: {input_path}')

    native_fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    if native_fps <= 0:
        native_fps = 25.0  # fallback

    duration = total_frames / native_fps
    logger.info(f'Video info: duration={duration:.2f}s, native_fps={native_fps}, total_frames={total_frames}')

    # Calculate frame interval: every N native frames we capture one
    frame_interval = max(1, int(round(native_fps / fps)))

    # Encode params
    if fmt == 'jpeg':
        encode_ext = '.jpg'
        encode_params = [cv2.IMWRITE_JPEG_QUALITY, quality]
    else:
        encode_ext = '.png'
        encode_params = [cv2.IMWRITE_PNG_COMPRESSION, 3]

    frames_b64 = []
    frame_idx = 0
    captured = 0

    while captured < max_frames:
        ret, frame = cap.read()
        if not ret:
            break

        if frame_idx % frame_interval == 0:
            success, buf = cv2.imencode(encode_ext, frame, encode_params)
            if success:
                frames_b64.append(base64.b64encode(buf.tobytes()).decode('utf-8'))
                captured += 1
                logger.debug(f'Captured frame {frame_idx} ({captured}/{max_frames})')

        frame_idx += 1

    cap.release()

    actual_fps = captured / duration if duration > 0 else 0.0

    logger.info(f'Extracted {captured} frames from {duration:.2f}s video')

    return {
        'frames': frames_b64,
        'count': captured,
        'fps_extracted': round(actual_fps, 4),
        'duration_seconds': round(duration, 4),
    }
