import hashlib
import mimetypes
from pathlib import Path
from typing import Tuple

def compute_hashes(file_path: str | Path, chunk_size: int = 65536) -> Tuple[str, str]:
    """
    Computes both SHA-1 (hex) and MD5 (hex) in a single streaming pass.
    """
    sha1 = hashlib.sha1()
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            sha1.update(chunk)
            md5.update(chunk)
    return sha1.hexdigest(), md5.hexdigest()

def compute_sha1(file_path: str | Path) -> str:
    sha1 = hashlib.sha1()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            sha1.update(chunk)
    return sha1.hexdigest()

def get_mime_type(file_path: str | Path) -> str:
    mime, _ = mimetypes.guess_type(str(file_path))
    if not mime:
        suffix = Path(file_path).suffix.lower()
        if suffix in [".heic", ".heif"]:
            return "image/heic"
        elif suffix in [".dng", ".raw", ".cr2", ".nef"]:
            return "image/x-adobe-dng"
        elif suffix in [".mov"]:
            return "video/quicktime"
        return "application/octet-stream"
    return mime

def format_bytes(size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:3.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PB"

VIDEO_EXTENSIONS = {
    ".mp4", ".mov", ".m4v", ".avi", ".mkv", ".wmv", ".flv",
    ".webm", ".3gp", ".3g2", ".ts", ".mts", ".m2ts", ".vob",
    ".ogv", ".mpg", ".mpeg", ".m4p", ".asf", ".f4v"
}

def is_video_file(file_path: str | Path) -> bool:
    """Check if a file path or name is a video by file extension or MIME type."""
    suffix = Path(file_path).suffix.lower()
    if suffix in VIDEO_EXTENSIONS:
        return True
    mime = get_mime_type(file_path)
    return mime.startswith("video/")

def is_video_asset(asset: dict) -> bool:
    """Check if an Immich or database asset is a video."""
    if not asset:
        return False
    if str(asset.get("type", "")).upper() == "VIDEO":
        return True
    mime = (asset.get("originalMimeType") or "").lower()
    if mime.startswith("video/"):
        return True
    fname = asset.get("originalFileName") or asset.get("filename") or ""
    fpath = asset.get("originalPath") or ""
    return is_video_file(fname) or is_video_file(fpath)

def is_hidden_or_locked_asset(asset: dict) -> bool:
    """
    Check if an Immich asset is hidden, locked, archived, trashed, or private.
    """
    if not asset:
        return False

    # 1. Immich visibility field ('hidden', 'locked', 'archive', 'private')
    visibility = str(asset.get("visibility") or "").lower().strip()
    if visibility in ("hidden", "locked", "archive", "private"):
        return True

    # 2. Boolean flags on the asset
    if asset.get("isArchived") or asset.get("isTrashed") or asset.get("isLocked"):
        return True

    # 3. Path or filename patterns (e.g. /locked/, /hidden/, /private/)
    path = str(asset.get("originalPath") or "").lower().replace("\\", "/")
    fname = str(asset.get("originalFileName") or asset.get("filename") or "").lower()
    for marker in ("/locked/", "/hidden/", "/private/", ".locked", ".hidden"):
        if marker in path or marker in fname:
            return True

    return False

