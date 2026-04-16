"""
frame_extraction_service.py
────────────────────────────
Lightweight Flask HTTP service that performs ffmpeg-based video frame
extraction. Designed to be called by the Dify Code Node (frame_extraction_node.py)
which cannot run subprocess or write files directly.

Usage:
    pip install flask
    python frame_extraction_service.py

Endpoints:
    POST /extract
        Request JSON:
            {
                "video_url":  "<url>",
                "max_frames": 20,          # optional, default 20
                "filename":   "video.mp4", # optional, for logging
                "mime_type":  "video/mp4"  # optional, for logging
            }
        Response JSON:
            {
                "frames": [...],
                "video_duration": 15.3,
                "total_frames_extracted": 20,
                "error": null
            }

    GET /health
        Returns {"status": "ok"}

    GET /metrics
        Provides detailed performance metrics in Prometheus format

    GET /stats
        Provides detailed statistics in JSON format
"""

import os
import json
import base64
import subprocess
import tempfile
import urllib.request
import time
import psutil
import logging
import threading
import uuid
from datetime import datetime
from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor, TimeoutError


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('frame_extraction_service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 性能监控数据
_performance_stats = {
    'total_requests': 0,
    'successful_requests': 0,
    'failed_requests': 0,
    'total_processing_time': 0.0,
    'last_request_time': None
}

# 性能优化配置
MAX_PROCESSING_TIME = 180  # 最大处理时间180秒（Render免费版CPU较慢，需要更多时间）
MAX_CONCURRENT_REQUESTS = 5  # 最大并发请求数
MEMORY_THRESHOLD_MB = 500  # 内存使用阈值500MB

# 并发控制
_request_semaphore = threading.Semaphore(MAX_CONCURRENT_REQUESTS)
_active_requests = {}

# 可靠性配置
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY_BASE = 1.0  # 基础重试延迟（秒）
RETRY_DELAY_MULTIPLIER = 2.0  # 重试延迟倍数

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
MAX_FRAMES           = 10  # 改为默认10帧，符合需求文档要求
SUPPORTED_EXTENSIONS = {".mp4", ".mov", ".avi", ".webm", ".mkv", ".flv", ".wmv", ".m4v", ".3gp", ".mpeg", ".mpg"}
SUPPORTED_MIME_TYPES = {
    "video/mp4", "video/quicktime", "video/x-msvideo", "video/webm", 
    "video/x-matroska", "video/x-flv", "video/x-ms-wmv", "video/mp4",
    "video/3gpp", "video/mpeg"
}
FFMPEG_BIN           = os.environ.get("FFMPEG_BIN",  "ffmpeg")
FFPROBE_BIN          = os.environ.get("FFPROBE_BIN", "ffprobe")
HOST                 = os.environ.get("HOST", "0.0.0.0")
PORT                 = int(os.environ.get("PORT", 10000))  # Render uses PORT env var
MAX_FILE_SIZE        = 100 * 1024 * 1024  # 100MB文件大小限制
UPLOAD_FOLDER        = "/tmp/uploads"  # 文件上传目录

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE  # 限制上传文件大小


# ─────────────────────────────────────────────────────────────────────────────
# Error Handling and Response Formats
# ─────────────────────────────────────────────────────────────────────────────

class FrameExtractionError(Exception):
    """自定义异常类，用于帧提取过程中的错误"""
    def __init__(self, stage: str, code: str, message: str, status_code: int = 500):
        self.stage = stage
        self.code = code
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


def _error(stage: str, code: str, message: str, status_code: int = 500) -> dict:
    """生成标准化的错误响应，兼容Dify格式"""
    return {
        "success": False,
        "frames": [],
        "video_duration": 0.0,
        "total_frames_extracted": 0,
        "error": {
            "error_stage": stage,
            "error_code": code,
            "error_message": message,
            "status_code": status_code
        },
        "timestamp": _get_current_timestamp(),
        "metadata": {
            "service_version": "1.0.0",
            "error_recoverable": _is_recoverable_error(code)
        }
    }


def _success_response(frames: list, duration: float) -> dict:
    """生成标准化的成功响应，兼容Dify格式"""
    return {
        "success": True,
        "frames": frames,
        "video_duration": round(duration, 3),
        "total_frames_extracted": len(frames),
        "error": None,
        "timestamp": _get_current_timestamp(),
        # Dify兼容的额外字段
        "metadata": {
            "service_version": "1.0.0",
            "extraction_method": "uniform_sampling",
            "frame_format": "base64_jpeg",
            "max_frames_supported": 100
        }
    }


def _get_current_timestamp() -> str:
    """获取当前时间戳"""
    return datetime.now().isoformat()


def _log_request(method: str, endpoint: str, status_code: int, processing_time: float = None):
    """记录请求日志"""
    logger.info(f"{method} {endpoint} - Status: {status_code} - Time: {processing_time:.3f}s")
    
    # 更新性能统计
    _performance_stats['total_requests'] += 1
    if 200 <= status_code < 300:
        _performance_stats['successful_requests'] += 1
    else:
        _performance_stats['failed_requests'] += 1
    
    if processing_time:
        _performance_stats['total_processing_time'] += processing_time
    
    _performance_stats['last_request_time'] = _get_current_timestamp()


def _get_system_stats():
    """获取系统统计信息"""
    try:
        return {
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_usage': psutil.virtual_memory().percent,
            'disk_usage': psutil.disk_usage('/').percent,
            'process_memory_mb': psutil.Process().memory_info().rss / 1024 / 1024,
            'uptime_seconds': time.time() - psutil.boot_time()
        }
    except Exception as e:
        logger.warning(f"Failed to get system stats: {e}")
        return {'error': str(e)}


def _validate_input_parameters(video_input: str, max_frames: int, filename: str, mime_type: str) -> None:
    """验证输入参数"""
    # 检查输入源
    if not video_input:
        raise FrameExtractionError("input_validation", "NO_INPUT_SOURCE", 
                                  "Either video_url (for JSON) or file (for form-data) is required.", 400)
    
    # 如果是文件上传，video_input参数是"file_upload"字符串
    if video_input != "file_upload" and not video_input.startswith(("http://", "https://")):
        raise FrameExtractionError("input_validation", "INVALID_VIDEO_URL",
                                  "video_url must be a valid HTTP/HTTPS URL", 400)
    
    if max_frames < 1 or max_frames > 100:
        raise FrameExtractionError("input_validation", "INVALID_FRAME_COUNT",
                                  "max_frames must be between 1 and 100", 400)
    
    extension = _splitext(filename) or ".mp4"
    if not _is_supported_format(extension, mime_type):
        raise FrameExtractionError("input_validation", "UNSUPPORTED_FORMAT",
                                  f"Format '{extension}' with mime type '{mime_type}' is not supported.", 400)


def _is_recoverable_error(error_code: str) -> bool:
    """判断错误是否可恢复"""
    recoverable_errors = {
        "DOWNLOAD_FAILED", "REQUEST_FAILED", "HTTP_ERROR", "TIMEOUT_ERROR"
    }
    return error_code in recoverable_errors


# ─────────────────────────────────────────────────────────────────────────────
# Request Handlers
# ─────────────────────────────────────────────────────────────────────────────

def _retry_with_backoff(func, *args, max_retries=MAX_RETRY_ATTEMPTS, **kwargs):
    """带指数退避的重试机制"""
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            
            # 判断是否应该重试
            if not _should_retry(e) or attempt == max_retries:
                break
                
            # 计算延迟时间
            delay = RETRY_DELAY_BASE * (RETRY_DELAY_MULTIPLIER ** attempt)
            logger.info(f"Retry attempt {attempt + 1}/{max_retries} after {delay:.1f}s: {e}")
            time.sleep(delay)
    
    # 所有重试都失败
    raise last_exception


def _should_retry(exception):
    """判断异常是否应该重试"""
    # 可重试的异常类型
    retryable_errors = [
        "DOWNLOAD_FAILED", "REQUEST_FAILED", "HTTP_ERROR", "TIMEOUT_ERROR",
        "ConnectionError", "TimeoutError", "socket.timeout"
    ]
    
    error_str = str(exception)
    for retryable in retryable_errors:
        if retryable in error_str:
            return True
    
    # 网络相关的异常
    if isinstance(exception, (ConnectionError, TimeoutError)):
        return True
    
    return False


def _graceful_degradation(func, fallback_func, *args, **kwargs):
    """优雅降级机制"""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.warning(f"Primary function failed, using fallback: {e}")
        try:
            return fallback_func(*args, **kwargs)
        except Exception as fallback_error:
            logger.error(f"Fallback function also failed: {fallback_error}")
            raise e  # 返回原始错误


def _extract_frames_with_fallback(video_path: str, max_frames: int):
    """带优雅降级的帧提取"""
    def primary_extraction():
        """主提取方法"""
        return _extract_frames_with_timeout(video_path, max_frames)
    
    def fallback_extraction():
        """降级提取方法（减少帧数）"""
        reduced_frames = max(1, max_frames // 2)  # 减少一半帧数
        logger.info(f"Using fallback extraction with {reduced_frames} frames")
        return _extract_frames_with_timeout(video_path, reduced_frames)
    
    return _graceful_degradation(primary_extraction, fallback_extraction)


def _cleanup_resources():
    """资源清理函数"""
    try:
        # 清理临时文件
        temp_dir = "/tmp/uploads"
        if os.path.exists(temp_dir):
            for filename in os.listdir(temp_dir):
                filepath = os.path.join(temp_dir, filename)
                try:
                    # 删除超过1小时的临时文件
                    if os.path.isfile(filepath):
                        file_age = time.time() - os.path.getmtime(filepath)
                        if file_age > 3600:  # 1小时
                            os.remove(filepath)
                            logger.info(f"Cleaned up old temp file: {filename}")
                except Exception as e:
                    logger.warning(f"Failed to cleanup {filename}: {e}")
    except Exception as e:
        logger.warning(f"Resource cleanup failed: {e}")


def _handle_file_upload():
    """处理文件上传（带可靠性保障）"""
    try:
        # 资源清理
        _cleanup_resources()
        
        # 解析请求
        uploaded_file = request.files['file']
        max_frames = int(request.form.get('max_frames', MAX_FRAMES))
        
        # 验证输入
        _validate_input_parameters(video_input="file_upload", max_frames=max_frames, filename=uploaded_file.filename or "upload", mime_type=uploaded_file.content_type or "video/mp4")
        
        # 保存上传的文件
        with tempfile.NamedTemporaryFile(delete=False, suffix='.tmp') as tmp_file:
            uploaded_file.save(tmp_file.name)
            temp_path = tmp_file.name
        
        try:
            # 使用重试机制提取帧
            result = _retry_with_backoff(
                _extract_frames_with_fallback,
                temp_path, max_frames
            )
            
            return jsonify(_success_response(result['frames'], result['duration']))
            
        finally:
            # 清理临时文件
            if os.path.exists(temp_path):
                os.remove(temp_path)
                
    except Exception as e:
        logger.error(f"File upload processing failed: {e}")
        raise


def _handle_json_request():
    """处理JSON请求（带可靠性保障）"""
    try:
        # 资源清理
        _cleanup_resources()
        
        # 解析JSON请求
        data = request.get_json()
        video_url = data.get('video_url', '')
        max_frames = int(data.get('max_frames', MAX_FRAMES))
        filename = data.get('filename', 'video.mp4')
        mime_type = data.get('mime_type', 'video/mp4')
        
        # 验证输入
        _validate_input_parameters(video_input=video_url, max_frames=max_frames, filename=filename, mime_type=mime_type)
        
        # 下载视频文件
        with tempfile.NamedTemporaryFile(delete=False, suffix='.tmp') as tmp_file:
            temp_path = tmp_file.name
        
        try:
            # 使用重试机制下载
            _retry_with_backoff(_download, video_url, temp_path)
            
            # 使用重试机制提取帧
            result = _retry_with_backoff(
                _extract_frames_with_fallback,
                temp_path, max_frames
            )
            
            return jsonify(_success_response(result['frames'], result['duration']))
            
        finally:
            # 清理临时文件
            if os.path.exists(temp_path):
                os.remove(temp_path)
                
    except Exception as e:
        logger.error(f"JSON request processing failed: {e}")
        raise


def _process_video_file(video_path: str, max_frames: int, filename: str, mime_type: str):
    """处理视频文件提取帧"""
    # 2. Probe duration
    try:
        duration = _probe_duration(video_path)
    except Exception as exc:
        raise FrameExtractionError("ffprobe", "PROBE_FAILED", str(exc), 500)

    if duration <= 0:
        raise FrameExtractionError("ffprobe", "INVALID_DURATION",
                                  "Video duration is 0 or could not be determined.", 422)

    # 3. Calculate uniform timestamps
    n_frames   = max(1, min(max_frames, int(duration)))
    interval   = duration / n_frames
    timestamps = [round(interval * (i + 0.5), 3) for i in range(n_frames)]

    # 4. Extract frames
    frames_dir = os.path.join(os.path.dirname(video_path), "frames")
    os.makedirs(frames_dir, exist_ok=True)

    extracted_frames = []
    for idx, ts in enumerate(timestamps):
        frame_path = os.path.join(frames_dir, f"frame_{idx+1:03d}.jpg")
        try:
            _extract_frame(video_path, ts, frame_path)
        except Exception:
            continue  # skip unextractable frames

        if not os.path.exists(frame_path):
            continue

        with open(frame_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")

        extracted_frames.append({
            "frame_index":         idx + 1,
            "timestamp_seconds":   ts,
            "timestamp_formatted": _fmt_ts(ts),
            "image_base64":        b64,
            "image_mime":          "image/jpeg",
        })

    if not extracted_frames:
        raise FrameExtractionError("ffmpeg", "NO_FRAMES_EXTRACTED",
                                  "ffmpeg ran but produced no output frames.", 500)

    return jsonify(_success_response(extracted_frames, duration))


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def index():
    """根路径，提供友好的欢迎页面"""
    return jsonify({
        "service": "Frame Extraction Service",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "health": "/health",
            "extract_frames": "/extract",
            "metrics": "/metrics",
            "stats": "/stats"
        },
        "documentation": "Use POST /extract with video_url or file upload to extract frames",
        "timestamp": _get_current_timestamp()
    })

@app.route("/health", methods=["GET"])
def health():
    """增强的健康检查端点"""
    start_time = time.time()
    
    try:
        # 基础健康检查
        health_status = {
            "status": "ok",
            "timestamp": _get_current_timestamp(),
            "service": "frame_extraction_service",
            "version": "1.0.0"
        }
        
        # 系统状态检查
        system_stats = _get_system_stats()
        health_status["system"] = system_stats
        
        # 性能统计
        health_status["performance"] = {
            "total_requests": _performance_stats['total_requests'],
            "successful_requests": _performance_stats['successful_requests'],
            "failed_requests": _performance_stats['failed_requests'],
            "average_processing_time": (
                _performance_stats['total_processing_time'] / _performance_stats['total_requests'] 
                if _performance_stats['total_requests'] > 0 else 0
            ),
            "last_request_time": _performance_stats['last_request_time']
        }
        
        # 服务可用性检查
        health_status["services"] = {
            "ffmpeg": _check_ffmpeg_availability(),
            "ffprobe": _check_ffprobe_availability(),
            "temp_directory": _check_temp_directory()
        }
        
        processing_time = time.time() - start_time
        _log_request("GET", "/health", 200, processing_time)
        
        return jsonify(health_status)
        
    except Exception as e:
        processing_time = time.time() - start_time
        _log_request("GET", "/health", 500, processing_time)
        logger.error(f"Health check failed: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


def _check_ffmpeg_availability():
    """检查ffmpeg可用性"""
    try:
        result = subprocess.run([FFMPEG_BIN, "-version"], 
                              capture_output=True, text=True, timeout=5)
        return result.returncode == 0
    except Exception as e:
        logger.warning(f"FFmpeg check failed: {e}")
        return False


def _check_ffprobe_availability():
    """检查ffprobe可用性"""
    try:
        result = subprocess.run([FFPROBE_BIN, "-version"], 
                              capture_output=True, text=True, timeout=5)
        return result.returncode == 0
    except Exception as e:
        logger.warning(f"FFprobe check failed: {e}")
        return False


def _check_temp_directory():
    """检查临时目录可用性"""
    try:
        with tempfile.NamedTemporaryFile(dir="/tmp", delete=True) as f:
            f.write(b"test")
        return True
    except Exception as e:
        logger.warning(f"Temp directory check failed: {e}")
        return False


def _check_memory_usage():
    """检查内存使用情况"""
    try:
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        
        if memory_mb > MEMORY_THRESHOLD_MB:
            logger.warning(f"Memory usage high: {memory_mb:.2f}MB (threshold: {MEMORY_THRESHOLD_MB}MB)")
            return False
        return True
    except Exception as e:
        logger.warning(f"Memory check failed: {e}")
        return True


def _extract_frames(video_path: str, max_frames: int) -> dict:
    """Core frame extraction logic.

    Returns dict with keys: frames (list of dicts), duration (float).
    """
    # Probe duration
    try:
        duration = _probe_duration(video_path)
    except Exception as exc:
        raise FrameExtractionError("ffprobe", "PROBE_FAILED", str(exc), 500)

    if duration <= 0:
        raise FrameExtractionError("ffprobe", "INVALID_DURATION",
                                  "Video duration is 0 or could not be determined.", 422)

    # Calculate uniform timestamps
    n_frames = max(1, min(max_frames, int(duration)))
    interval = duration / n_frames
    timestamps = [round(interval * (i + 0.5), 3) for i in range(n_frames)]

    # Extract frames to temp directory
    frames_dir = tempfile.mkdtemp(prefix="frames_")
    extracted_frames = []

    try:
        for idx, ts in enumerate(timestamps):
            frame_path = os.path.join(frames_dir, f"frame_{idx+1:03d}.jpg")
            try:
                _extract_frame(video_path, ts, frame_path)
            except Exception:
                continue  # skip unextractable frames

            if not os.path.exists(frame_path):
                continue

            with open(frame_path, "rb") as f:
                b64 = base64.b64encode(f.read()).decode("utf-8")

            extracted_frames.append({
                "frame_index":         idx + 1,
                "timestamp_seconds":   ts,
                "timestamp_formatted": _fmt_ts(ts),
                "image_base64":        b64,
                "image_mime":          "image/jpeg",
            })
    finally:
        # Cleanup temp frames directory
        import shutil
        shutil.rmtree(frames_dir, ignore_errors=True)

    if not extracted_frames:
        raise FrameExtractionError("ffmpeg", "NO_FRAMES_EXTRACTED",
                                  "ffmpeg ran but produced no output frames.", 500)

    return {"frames": extracted_frames, "duration": duration}


def _extract_frames_with_timeout(video_path: str, max_frames: int, timeout: int = MAX_PROCESSING_TIME):
    """带超时控制的帧提取函数（线程安全版本）"""
    # 检查内存使用
    if not _check_memory_usage():
        raise FrameExtractionError("performance", "MEMORY_LIMIT_EXCEEDED",
                                  f"Memory usage exceeds threshold ({MEMORY_THRESHOLD_MB}MB)", 503)

    # Use ThreadPoolExecutor for thread-safe timeout (signal.alarm only works in main thread)
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        future = executor.submit(_extract_frames, video_path, max_frames)
        return future.result(timeout=timeout)
    except TimeoutError:
        raise FrameExtractionError("performance", "PROCESSING_TIMEOUT",
                                  f"Frame extraction timed out after {timeout} seconds", 504)
    finally:
        executor.shutdown(wait=False)


def _process_request_with_concurrency_control(request_id: str, func, *args, **kwargs):
    """带并发控制的请求处理"""
    start_time = time.time()
    
    # 获取信号量（控制并发数）
    _request_semaphore.acquire()
    _active_requests[request_id] = {
        'start_time': start_time,
        'status': 'processing'
    }
    
    try:
        # 执行处理函数
        result = func(*args, **kwargs)
        
        # 更新请求状态
        _active_requests[request_id]['status'] = 'completed'
        _active_requests[request_id]['end_time'] = time.time()
        _active_requests[request_id]['success'] = True
        
        return result
        
    except Exception as e:
        # 更新请求状态
        _active_requests[request_id]['status'] = 'failed'
        _active_requests[request_id]['end_time'] = time.time()
        _active_requests[request_id]['error'] = str(e)
        _active_requests[request_id]['success'] = False
        
        raise e
    finally:
        # 释放信号量
        _request_semaphore.release()


@app.route("/extract", methods=["POST"])
@app.route("/extract_frames", methods=["POST"])  # 兼容Dify的默认端点
@app.route("/api/extract", methods=["POST"])     # 标准API端点
def extract():
    request_id = str(uuid.uuid4())
    start_time = time.time()
    
    try:
        # 检查请求内容类型
        content_type = request.content_type or ''
        
        # 检查是否同时提供了文件上传和JSON数据
        has_file_upload = content_type.startswith('multipart/form-data') and 'file' in request.files and request.files['file'].filename != ''
        has_json_data = content_type.startswith('application/json')
        
        # 输入源优先级：文件上传优先于URL
        if has_file_upload:
            logger.info(f"Processing file upload (request_id: {request_id})")
            result = _process_request_with_concurrency_control(
                request_id, _handle_file_upload
            )
        elif has_json_data:
            logger.info(f"Processing JSON request (request_id: {request_id})")
            result = _process_request_with_concurrency_control(
                request_id, _handle_json_request
            )
        else:
            raise FrameExtractionError("input_validation", "NO_VALID_INPUT",
                                      "No valid input source provided. Use multipart/form-data with 'file' field or application/json with 'video_url'.", 400)
        
        processing_time = time.time() - start_time
        _log_request("POST", "/extract", 200, processing_time)
        
        return result

    except FrameExtractionError as e:
        processing_time = time.time() - start_time
        _log_request("POST", "/extract", e.status_code, processing_time)
        return jsonify(_error(e.stage, e.code, e.message, e.status_code)), e.status_code
    except Exception as e:
        # 处理未预期的异常
        processing_time = time.time() - start_time
        _log_request("POST", "/extract", 500, processing_time)
        return jsonify(_error("unexpected", "UNEXPECTED_ERROR", str(e))), 500


@app.route("/metrics", methods=["GET"])
def metrics():
    """提供详细的性能指标（Prometheus格式兼容）"""
    start_time = time.time()
    
    try:
        system_stats = _get_system_stats()
        
        # Prometheus格式的指标
        metrics_data = [
            f"# HELP frame_extraction_requests_total Total number of requests",
            f"# TYPE frame_extraction_requests_total counter",
            f"frame_extraction_requests_total {_performance_stats['total_requests']}",
            
            f"# HELP frame_extraction_requests_successful_total Total successful requests",
            f"# TYPE frame_extraction_requests_successful_total counter",
            f"frame_extraction_requests_successful_total {_performance_stats['successful_requests']}",
            
            f"# HELP frame_extraction_requests_failed_total Total failed requests",
            f"# TYPE frame_extraction_requests_failed_total counter",
            f"frame_extraction_requests_failed_total {_performance_stats['failed_requests']}",
            
            f"# HELP frame_extraction_processing_time_seconds_total Total processing time",
            f"# TYPE frame_extraction_processing_time_seconds_total counter",
            f"frame_extraction_processing_time_seconds_total {_performance_stats['total_processing_time']:.3f}",
            
            f"# HELP frame_extraction_cpu_percent CPU usage percentage",
            f"# TYPE frame_extraction_cpu_percent gauge",
            f"frame_extraction_cpu_percent {system_stats.get('cpu_percent', 0)}",
            
            f"# HELP frame_extraction_memory_percent Memory usage percentage",
            f"# TYPE frame_extraction_memory_percent gauge",
            f"frame_extraction_memory_percent {system_stats.get('memory_usage', 0)}",
            
            f"# HELP frame_extraction_disk_percent Disk usage percentage",
            f"# TYPE frame_extraction_disk_percent gauge",
            f"frame_extraction_disk_percent {system_stats.get('disk_usage', 0)}",
            
            f"# HELP frame_extraction_process_memory_mb Process memory usage in MB",
            f"# TYPE frame_extraction_process_memory_mb gauge",
            f"frame_extraction_process_memory_mb {system_stats.get('process_memory_mb', 0):.2f}",
        ]
        
        processing_time = time.time() - start_time
        _log_request("GET", "/metrics", 200, processing_time)
        
        return '\n'.join(metrics_data), 200, {'Content-Type': 'text/plain'}
        
    except Exception as e:
        processing_time = time.time() - start_time
        _log_request("GET", "/metrics", 500, processing_time)
        logger.error(f"Metrics endpoint failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/stats", methods=["GET"])
def stats():
    """提供详细的统计信息（JSON格式）"""
    start_time = time.time()
    
    try:
        stats_data = {
            "service": "frame_extraction_service",
            "version": "1.0.0",
            "timestamp": _get_current_timestamp(),
            "performance": {
                "total_requests": _performance_stats['total_requests'],
                "successful_requests": _performance_stats['successful_requests'],
                "failed_requests": _performance_stats['failed_requests'],
                "success_rate": (
                    _performance_stats['successful_requests'] / _performance_stats['total_requests'] * 100 
                    if _performance_stats['total_requests'] > 0 else 0
                ),
                "average_processing_time": (
                    _performance_stats['total_processing_time'] / _performance_stats['total_requests'] 
                    if _performance_stats['total_requests'] > 0 else 0
                ),
                "last_request_time": _performance_stats['last_request_time']
            },
            "system": _get_system_stats(),
            "services": {
                "ffmpeg": _check_ffmpeg_availability(),
                "ffprobe": _check_ffprobe_availability(),
                "temp_directory": _check_temp_directory()
            }
        }
        
        processing_time = time.time() - start_time
        _log_request("GET", "/stats", 200, processing_time)
        
        return jsonify(stats_data)
        
    except Exception as e:
        processing_time = time.time() - start_time
        _log_request("GET", "/stats", 500, processing_time)
        logger.error(f"Stats endpoint failed: {e}")
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _download(url: str, dest_path: str, max_retries: int = 3, timeout: int = 120):
    """下载视频文件，支持重试和超时控制"""
    import time
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    last_error = None
    
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            
            # 设置超时
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                # 检查响应状态码
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}: {resp.reason}")
                
                # 检查内容类型
                content_type = resp.headers.get('Content-Type', '')
                if not content_type.startswith('video/'):
                    # 记录警告但不阻止下载
                    print(f"Warning: Content-Type is '{content_type}', expected video/*")
                
                # 获取文件大小（如果可用）
                content_length = resp.headers.get('Content-Length')
                total_size = int(content_length) if content_length else None
                
                # 下载文件
                downloaded = 0
                with open(dest_path, "wb") as out:
                    while True:
                        chunk = resp.read(8192)  # 8KB chunks
                        if not chunk:
                            break
                        out.write(chunk)
                        downloaded += len(chunk)
                        
                        # 显示下载进度（如果知道总大小）
                        if total_size:
                            progress = (downloaded / total_size) * 100
                            print(f"Download progress: {progress:.1f}% ({downloaded}/{total_size} bytes)")
                
                # 验证下载的文件大小
                if total_size and downloaded != total_size:
                    raise RuntimeError(f"Download incomplete: {downloaded}/{total_size} bytes")
                
                print(f"Download completed successfully: {downloaded} bytes")
                return  # 下载成功
                
        except Exception as exc:
            last_error = exc
            print(f"Download attempt {attempt + 1} failed: {exc}")
            
            # 如果不是最后一次尝试，等待后重试
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # 指数退避
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"All {max_retries} download attempts failed")
    
    # 所有尝试都失败
    raise RuntimeError(f"Failed to download after {max_retries} attempts: {last_error}")


def _probe_duration(video_path: str) -> float:
    cmd = [FFPROBE_BIN, "-v", "quiet", "-print_format", "json",
           "-show_format", video_path]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"ffprobe error: {result.stderr.strip()}")
    info = json.loads(result.stdout)
    return float(info.get("format", {}).get("duration", 0))


# Frame output config – keep total response under Dify's 1 MB text limit.
# With max_width=512 and q:v=8, each JPEG frame is roughly 20-40 KB.
# 10 frames × ~40 KB × 1.37 (base64 overhead) ≈ 550 KB – well within 1 MB.
FRAME_MAX_WIDTH = int(os.environ.get("FRAME_MAX_WIDTH", "512"))
FRAME_JPEG_QUALITY = int(os.environ.get("FRAME_JPEG_QUALITY", "8"))  # ffmpeg -q:v scale 2(best)–31(worst)


def _extract_frame(video_path: str, timestamp: float, output_path: str):
    # Scale down: keep aspect ratio, limit width to FRAME_MAX_WIDTH pixels.
    # -2 ensures height is divisible by 2 (required by many codecs).
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


def _fmt_ts(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:05.2f}"


def _splitext(filename: str) -> str:
    if "." not in filename:
        return ""
    return "." + filename.rsplit(".", 1)[-1].lower()


def _is_supported_format(extension: str, mime_type: str) -> bool:
    """检查文件格式是否受支持"""
    # 如果提供了MIME类型，优先检查MIME类型
    if mime_type and mime_type in SUPPORTED_MIME_TYPES:
        return True
    
    # 检查文件扩展名
    if extension in SUPPORTED_EXTENSIONS:
        return True
    
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host=HOST, port=PORT, debug=False)
