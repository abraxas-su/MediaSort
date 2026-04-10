"""Core helpers for a limited file organizer (Photos, Videos, Audio, Documents)."""

from pathlib import Path

PHOTO_EXTS: set[str] = {
    "jpg",
    "jpeg",
    "png",
    "webp",
    "gif",
    "bmp",
    "tiff",
    "tif",
    "heic",
    "heif",
    "raw",
    "cr2",
    "nef",
    "arw",
}

VIDEO_EXTS: set[str] = {
    "mp4",
    "mkv",
    "mov",
    "avi",
    "webm",
    "flv",
    "wmv",
    "m4v",
    "3gp",
    "ts",
    "vob",
}

DOCUMENT_EXTS: set[str] = {
    "pdf",
    "txt",
    "doc",
    "docx",
    "xls",
    "xlsx",
    "ppt",
    "pptx",
    "odt",
    "ods",
    "odp",
    "rtf",
    "md",
    "csv",
    "epub",
}

AUDIO_EXTS: set[str] = {
    "mp3",
    "wav",
    "flac",
    "aac",
    "m4a",
    "ogg",
    "wma",
    "aiff",
    "alac",
    "opus",
}

OUTPUT_FOLDERS: tuple[str, ...] = ("Photos", "Videos", "Audio", "Documents")

_WIN_PROTECTED: tuple[str, ...] = (
    r"C:\Windows",
    r"C:\Program Files",
    r"C:\Program Files (x86)",
    r"C:\ProgramData",
    r"C:\Users\Default",
)
_UNIX_PROTECTED: tuple[str, ...] = (
    "/etc",
    "/usr",
    "/bin",
    "/sbin",
    "/lib",
    "/lib64",
    "/boot",
    "/sys",
    "/proc",
    "/dev",
)
PROTECTED_DIRS: frozenset[Path] = frozenset(
    Path(p).resolve() for p in (*_WIN_PROTECTED, *_UNIX_PROTECTED) if Path(p).exists()
)

PHOTO_SUBFOLDERS: dict[str, set[str]] = {
    "JPEG": {"jpg", "jpeg"},
    "PNG": {"png"},
    "GIF": {"gif"},
    "WebP": {"webp"},
    "RAW": {"raw", "cr2", "nef", "arw"},
    "Other": {"bmp", "tiff", "tif", "heic", "heif"},
}
_PHOTO_EXT_TO_SUB: dict[str, str] = {
    ext: sub for sub, exts in PHOTO_SUBFOLDERS.items() for ext in exts
}

DOCUMENT_SUBFOLDERS: dict[str, set[str]] = {
    "PDF": {"pdf"},
    "Text": {"txt", "md", "rtf"},
    "Code": {"ts"},
    "Word": {"doc", "docx", "odt"},
    "Spreadsheets": {"xls", "xlsx", "ods", "csv"},
    "Presentations": {"ppt", "pptx", "odp"},
    "eBooks": {"epub"},
}
_DOC_EXT_TO_SUB: dict[str, str] = {
    ext: sub for sub, exts in DOCUMENT_SUBFOLDERS.items() for ext in exts
}

AUDIO_SUBFOLDERS: dict[str, set[str]] = {
    "Lossy": {"mp3", "aac", "m4a", "ogg", "wma", "opus"},
    "Lossless": {"flac", "alac"},
    "Uncompressed": {"wav", "aiff"},
}
_AUDIO_EXT_TO_SUB: dict[str, str] = {
    ext: sub for sub, exts in AUDIO_SUBFOLDERS.items() for ext in exts
}


def _looks_like_mpeg_ts(file_path: Path) -> bool:
    """
    Best-effort MPEG-TS probe.

    MPEG transport streams typically have a sync byte (0x47) every 188 bytes.
    We check a few packet boundaries near the start of the file.
    """
    packet_size = 188
    packets_to_check = 5
    min_required = packet_size * packets_to_check
    try:
        if file_path.stat().st_size < min_required:
            return False
        with open(file_path, "rb") as fh:
            chunk = fh.read(min_required)
    except OSError:
        return False
    if len(chunk) < min_required:
        return False
    return all(chunk[i * packet_size] == 0x47 for i in range(packets_to_check))


def get_category(file_path: Path) -> str | None:
    """Return target category for file or None when it should be ignored."""
    ext = file_path.suffix.lstrip(".").lower()
    if ext == "ts":
        return "Videos" if _looks_like_mpeg_ts(file_path) else None
    if ext in PHOTO_EXTS:
        return "Photos"
    if ext in VIDEO_EXTS:
        return "Videos"
    if ext in AUDIO_EXTS:
        return "Audio"
    if ext in DOCUMENT_EXTS:
        return "Documents"
    return None


def is_protected_path(path: Path) -> bool:
    """Return True if *path* is a drive root or inside a protected system dir."""
    try:
        resolved = path.resolve()
    except (OSError, ValueError):
        return True
    anchor_clean = resolved.anchor.rstrip("\\/")
    resolved_clean = str(resolved).rstrip("\\/")
    if resolved_clean == anchor_clean:
        return True
    for protected in PROTECTED_DIRS:
        try:
            resolved.relative_to(protected)
            return True
        except ValueError:
            pass
    return False


def is_outside_home(path: Path) -> bool:
    """Return True if *path* is not inside the current user's home directory."""
    try:
        path.resolve().relative_to(Path.home().resolve())
        return False
    except (ValueError, RuntimeError, OSError):
        return True


def get_dest_folder(root_dir: Path, file_path: Path, use_subfolders: bool) -> Path | None:
    """Return destination folder or None if the file type is unsupported."""
    category = get_category(file_path)
    if category is None:
        return None
    base = root_dir / category
    if not use_subfolders:
        return base

    ext = file_path.suffix.lstrip(".").lower()
    if category == "Photos":
        sub = _PHOTO_EXT_TO_SUB.get(ext, "Other")
    elif category == "Videos":
        sub = ext.upper() if ext else "Other"
    elif category == "Audio":
        sub = _AUDIO_EXT_TO_SUB.get(ext, "Other")
    else:
        sub = _DOC_EXT_TO_SUB.get(ext, "Other")
    return base / sub


def get_safe_destination(dest_folder: Path, original_name: str) -> Path:
    """Return collision-safe destination path inside dest_folder."""
    candidate = dest_folder / original_name
    if not candidate.exists():
        return candidate

    stem = Path(original_name).stem
    suffix = Path(original_name).suffix
    n = 1
    while True:
        if n > 9_999:
            raise OSError(f"Too many duplicate filenames for: {original_name}")
        alt = dest_folder / f"{stem}({n}){suffix}"
        if not alt.exists():
            return alt
        n += 1
