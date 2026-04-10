"""File organizer (Photos, Videos, Audio, Documents) with undo/history."""

import json
import os
import shutil
import threading
import tkinter as tk
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import Callable

import organizer_core_lite as core

OUTPUT_FOLDERS = core.OUTPUT_FOLDERS

_HISTORY_FILENAME: str = "_organizer_history.json"
_HISTORY_MAX_SESSIONS: int = 20
_HISTORY_MAX_FILE_BYTES: int = 32 * 1024 * 1024
_SCAN_PROGRESS_INTERVAL: int = 10_000
_SCAN_MAX_DEPTH_DEFAULT: int = 2
_DETAILED_FILE_LOG_MAX: int = 2_000
_BATCH_PROGRESS_LOG_EVERY: int = 500

LOG_INFO = "info"
LOG_WARN = "warn"
LOG_ERROR = "error"
LOG_DONE = "done"
LOG_PREVIEW = "preview"
LOG_SEP = "sep"


def _sanitize_for_log(text: str) -> str:
    return "".join(ch if ch.isprintable() else "?" for ch in text)


def _error_reason(exc: Exception) -> str:
    if isinstance(exc, PermissionError):
        return "reason: permission denied (locked/read-only/protected file)"
    if isinstance(exc, FileNotFoundError):
        return "reason: file path no longer exists"
    if isinstance(exc, OSError):
        err_no = getattr(exc, "errno", None)
        return f"reason: OS error (errno={err_no})" if err_no is not None else "reason: OS-level error"
    return "reason: unexpected runtime error"


def _valid_rel_path(rel: str) -> bool:
    if not rel or "\x00" in rel:
        return False
    p = Path(rel)
    if p.is_absolute():
        return False
    return all(part != ".." for part in p.parts)


def _is_relative_to(path: Path, other: Path) -> bool:
    try:
        path.relative_to(other)
        return True
    except ValueError:
        return False


def _validate_path_within_root(path: Path, root_res: Path) -> bool:
    try:
        return _is_relative_to(path.resolve(), root_res)
    except OSError:
        return False


def _validate_destination_safety(dest_dir: Path, dest_path: Path, root_res: Path) -> str | None:
    """Return None when destination looks safe, else a short reason."""
    if not _validate_path_within_root(dest_dir, root_res):
        return "destination directory resolves outside root"
    if _path_has_symlink_component(dest_dir):
        return "destination directory path contains symlink component"
    if not _validate_path_within_root(dest_path.parent, root_res):
        return "destination parent resolves outside root"
    if _path_has_symlink_component(dest_path.parent):
        return "destination parent path contains symlink component"
    return None


def _path_has_symlink_component(path: Path, stop_before: Path | None = None) -> bool:
    """
    Return True if any existing component in path is a symlink.
    stop_before can be used to skip checking final file component.
    """
    cur = Path(path.anchor) if path.is_absolute() else Path(".")
    parts = path.parts[1:] if path.is_absolute() else path.parts
    for part in parts:
        nxt = cur / part
        if stop_before is not None and nxt == stop_before:
            return False
        try:
            if nxt.exists() and nxt.is_symlink():
                return True
        except OSError:
            return True
        cur = nxt
    return False


def _resolve_conflict_path(dest_dir: Path, original_name: str, policy: str) -> Path | None:
    if policy not in ("rename", "overwrite", "skip"):
        policy = "rename"
    direct = dest_dir / original_name
    if policy == "overwrite":
        return direct
    if policy == "skip" and direct.exists():
        return None
    if policy == "rename":
        return core.get_safe_destination(dest_dir, original_name)
    return direct


def _load_history(history_file: Path) -> list[dict]:
    if not history_file.is_file():
        return []
    try:
        if history_file.stat().st_size > _HISTORY_MAX_FILE_BYTES:
            return []
        with open(history_file, "r", encoding="utf-8", errors="replace") as fh:
            data = json.load(fh)
    except (OSError, ValueError, json.JSONDecodeError):
        return []

    sessions = data.get("sessions", []) if isinstance(data, dict) else []
    if not isinstance(sessions, list):
        return []

    valid: list[dict] = []
    for s in sessions[-_HISTORY_MAX_SESSIONS:]:
        if not isinstance(s, dict):
            continue
        if not all(k in s for k in ("id", "ts", "root", "op", "records")):
            continue
        if s.get("op") not in ("move", "copy"):
            continue
        if not isinstance(s.get("records"), list):
            continue
        norm_records: list[dict] = []
        for rec in s["records"]:
            if not isinstance(rec, dict):
                continue
            src = rec.get("src")
            dst = rec.get("dst")
            if not isinstance(src, str) or not isinstance(dst, str):
                continue
            if not _valid_rel_path(src) or not _valid_rel_path(dst):
                continue
            norm_records.append(
                {
                    "src": src,
                    "dst": dst,
                    "cat": str(rec.get("cat", "Documents")),
                    "sub": str(rec.get("sub", "")),
                }
            )
        if not norm_records:
            continue
        valid.append(
            {
                "id": str(s.get("id", "")),
                "ts": str(s.get("ts", "")),
                "root": str(s.get("root", "")),
                "op": s["op"],
                "records": norm_records,
            }
        )
    return valid


def _atomic_write_json(path: Path, payload: dict) -> None:
    tmp_path = path.with_name(f"{path.name}.tmp-{uuid.uuid4().hex}")
    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass


def _save_history(
    history_file: Path, sessions: list[dict], log_cb: Callable[[str, str], None] | None = None
) -> bool:
    try:
        _atomic_write_json(history_file, {"format_version": 1, "sessions": sessions[-_HISTORY_MAX_SESSIONS:]})
        return True
    except OSError as exc:
        if log_cb:
            log_cb(
                f"[ERROR] Failed to save undo history: {exc} ({_error_reason(exc)})",
                LOG_ERROR,
            )
        return False


def _quick_latest_session(history_file: Path) -> dict | None:
    sessions = _load_history(history_file)
    return sessions[-1] if sessions else None


def _cleanup_empty_output_dirs(root_dir: Path) -> int:
    cleaned = 0
    for folder_name in OUTPUT_FOLDERS:
        cat_dir = root_dir / folder_name
        if not cat_dir.is_dir():
            continue
        try:
            with os.scandir(cat_dir) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        try:
                            Path(entry.path).rmdir()
                            cleaned += 1
                        except OSError:
                            pass
        except OSError:
            pass
        try:
            cat_dir.rmdir()
            cleaned += 1
        except OSError:
            pass
    return cleaned


def collect_files(
    root_dir: Path,
    cancel_event: threading.Event,
    recursive_scan: bool,
    max_depth: int,
    scan_progress_cb: Callable[[int], None] | None = None,
) -> tuple[list[Path], bool]:
    category_cache: dict[str, str | None] = {}

    def _is_supported_file(path: Path) -> bool:
        ext = path.suffix.lower()
        if ext not in category_cache:
            category_cache[ext] = core.get_category(path)
        return category_cache[ext] is not None

    files: list[Path] = []
    output_names = frozenset(OUTPUT_FOLDERS)

    if not recursive_scan:
        try:
            with os.scandir(root_dir) as it:
                for entry in it:
                    if cancel_event.is_set():
                        return files, True
                    if entry.is_file(follow_symlinks=False) and entry.name != _HISTORY_FILENAME:
                        p = Path(entry.path)
                        if not _is_supported_file(p):
                            continue
                        files.append(p)
                        if scan_progress_cb and len(files) % _SCAN_PROGRESS_INTERVAL == 0:
                            scan_progress_cb(len(files))
        except OSError:
            pass
        return files, False

    stack: list[tuple[Path, bool, int]] = [(root_dir, True, 0)]
    while stack:
        if cancel_event.is_set():
            return files, True
        directory, is_root, depth = stack.pop()
        try:
            with os.scandir(directory) as it:
                for entry in it:
                    if cancel_event.is_set():
                        return files, True
                    if entry.is_dir(follow_symlinks=False):
                        p = Path(entry.path)
                        if p.name in output_names:
                            continue
                        if depth < max_depth:
                            stack.append((p, False, depth + 1))
                    elif entry.is_file(follow_symlinks=False) and entry.name != _HISTORY_FILENAME:
                        p = Path(entry.path)
                        if not _is_supported_file(p):
                            continue
                        files.append(p)
                        if scan_progress_cb and len(files) % _SCAN_PROGRESS_INTERVAL == 0:
                            scan_progress_cb(len(files))
        except OSError:
            pass
    return files, False


def organize_files(
    root_dir: Path,
    operation: str,
    dry_run: bool,
    use_subfolders: bool,
    recursive_scan: bool,
    max_depth: int,
    conflict_policy: str,
    cancel_event: threading.Event,
    scan_done_cb: Callable[[int], None],
    progress_cb: Callable[[int, int, str], None],
    log_cb: Callable[[str, str], None],
    session_cb: Callable[[dict | None], None],
    done_cb: Callable[[bool, str], None],
) -> None:
    if operation not in ("move", "copy"):
        session_cb(None)
        done_cb(False, f"Invalid operation: {operation!r}")
        return

    try:
        root_res = root_dir.resolve()
        files, scan_cancelled = collect_files(
            root_dir=root_dir,
            cancel_event=cancel_event,
            recursive_scan=recursive_scan,
            max_depth=max_depth,
            scan_progress_cb=lambda n: log_cb(f"[INFO] scan in progress: {n:,} files found", LOG_INFO),
        )
        total = len(files)
        scan_done_cb(total)
        if scan_cancelled:
            session_cb(None)
            done_cb(False, f"Cancelled during scan - found {total:,} file(s).")
            return
        if total == 0:
            session_cb(None)
            done_cb(True, "No files found.")
            return

        session = {
            "id": str(uuid.uuid4()),
            "ts": datetime.now(tz=timezone.utc).isoformat(),
            "root": str(root_dir),
            "op": operation,
            "records": [],
        }
        organized = 0
        skipped = 0
        errors = 0
        detailed_logs = total <= _DETAILED_FILE_LOG_MAX
        ext_category_cache: dict[str, str | None] = {}

        for index, file_path in enumerate(files, start=1):
            if cancel_event.is_set():
                break

            safe_src = _sanitize_for_log(file_path.name)
            ext = file_path.suffix.lower()
            category = ext_category_cache.get(ext)
            if ext not in ext_category_cache:
                category = core.get_category(file_path)
                ext_category_cache[ext] = category
            if category is None:
                skipped += 1
                progress_cb(index, total, file_path.name)
                continue
            dest_dir = core.get_dest_folder(root_dir, file_path, use_subfolders)
            if dest_dir is None:
                skipped += 1
                progress_cb(index, total, file_path.name)
                continue

            try:
                dest_candidate = _resolve_conflict_path(dest_dir, file_path.name, conflict_policy)
                if dest_candidate is None:
                    skipped += 1
                    if detailed_logs:
                        log_cb(f"[SKIP] Destination exists: {safe_src}", LOG_WARN)
                    progress_cb(index, total, file_path.name)
                    continue
                dest_path = dest_candidate
                safety_issue = _validate_destination_safety(dest_dir, dest_path, root_res)
                if safety_issue:
                    raise OSError(safety_issue)
                if dry_run:
                    organized += 1
                    if detailed_logs:
                        rel = dest_path.parent.relative_to(root_dir).as_posix()
                        log_cb(f"[PREVIEW] {safe_src} -> {rel}/{dest_path.name}", LOG_PREVIEW)
                else:
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    # Re-check right before touching files to reduce TOCTOU risk.
                    safety_issue = _validate_destination_safety(dest_dir, dest_path, root_res)
                    if safety_issue:
                        raise OSError(f"Blocked before operation: {safety_issue}")
                    if dest_path.exists() and dest_path.is_symlink():
                        raise OSError("Destination file is a symlink; blocked for safety")
                    if operation == "move":
                        os.replace(file_path, dest_path)
                    else:
                        shutil.copy2(file_path, dest_path)
                    organized += 1
                    if detailed_logs:
                        rel = dest_path.parent.relative_to(root_dir).as_posix()
                        log_cb(f"[{operation.upper()}] {safe_src} -> {rel}/{dest_path.name}", LOG_INFO)
                    src_rel = file_path.relative_to(root_dir).as_posix()
                    dst_rel = dest_path.relative_to(root_dir).as_posix()
                    parts = dest_path.parent.relative_to(root_dir).parts
                    sub = parts[1] if len(parts) > 1 else ""
                    session["records"].append({"src": src_rel, "dst": dst_rel, "cat": category, "sub": sub})
            except PermissionError as exc:
                errors += 1
                log_cb(f"[ERROR] Permission denied: {safe_src} ({_error_reason(exc)})", LOG_ERROR)
            except FileNotFoundError as exc:
                errors += 1
                log_cb(f"[ERROR] File not found: {safe_src} ({_error_reason(exc)})", LOG_ERROR)
            except OSError as exc:
                errors += 1
                log_cb(f"[ERROR] OS error {safe_src}: {exc} ({_error_reason(exc)})", LOG_ERROR)
            except Exception as exc:
                errors += 1
                log_cb(f"[ERROR] {safe_src}: {exc} ({_error_reason(exc)})", LOG_ERROR)

            progress_cb(index, total, file_path.name)
            if (
                not detailed_logs
                and index % _BATCH_PROGRESS_LOG_EVERY == 0
                and not cancel_event.is_set()
            ):
                log_cb(
                    f"[INFO] Progress: {index:,}/{total:,} checked, "
                    f"{organized:,} organized, {skipped:,} skipped, {errors:,} errors.",
                    LOG_INFO,
                )

        if not dry_run and session["records"]:
            session_cb(session)
        else:
            session_cb(None)

        if cancel_event.is_set():
            done_cb(False, f"Cancelled. Organized {organized:,}; skipped {skipped:,}; errors {errors:,}.")
        elif dry_run:
            done_cb(True, f"[DRY RUN] Checked {total:,}. Would organize {organized:,}; skipped {skipped:,}; errors {errors:,}.")
        else:
            cleaned = _cleanup_empty_output_dirs(root_dir)
            done_cb(True, f"Done. Organized {organized:,}; skipped {skipped:,}; errors {errors:,}. Empty folders removed: {cleaned:,}.")
    except Exception as exc:
        session_cb(None)
        done_cb(False, f"Fatal error: {exc}")


def _undo_worker(
    records: list[dict],
    root_dir: Path,
    op_type: str,
    conflict_policy: str,
    cancel_event: threading.Event,
    progress_cb: Callable[[int, int, str], None],
    log_cb: Callable[[str, str], None],
    done_cb: Callable[[bool, str, set[str]], None],
) -> None:
    total = len(records)
    processed = 0
    skipped = 0
    errors = 0
    undone_dsts: set[str] = set()
    root_res = root_dir.resolve()

    for idx, rec in enumerate(records, start=1):
        if cancel_event.is_set():
            break
        src_rel = str(rec.get("src", ""))
        dst_rel = str(rec.get("dst", ""))
        if not _valid_rel_path(src_rel) or not _valid_rel_path(dst_rel):
            errors += 1
            progress_cb(idx, total, "")
            continue

        src_abs = root_dir / src_rel
        dst_abs = root_dir / dst_rel
        try:
            src_abs.resolve().relative_to(root_res)
            dst_abs.resolve().relative_to(root_res)
        except (ValueError, OSError):
            errors += 1
            progress_cb(idx, total, "")
            continue
        if _path_has_symlink_component(dst_abs.parent):
            errors += 1
            log_cb(
                f"[ERROR] Undo blocked (symlink in destination path): "
                f"{_sanitize_for_log(dst_rel)}",
                LOG_ERROR,
            )
            progress_cb(idx, total, "")
            continue

        try:
            if op_type == "copy":
                if not dst_abs.exists():
                    skipped += 1
                else:
                    dst_abs.unlink()
                    processed += 1
                    undone_dsts.add(dst_rel)
            else:
                if not dst_abs.exists():
                    skipped += 1
                else:
                    src_abs.parent.mkdir(parents=True, exist_ok=True)
                    if _path_has_symlink_component(src_abs.parent):
                        raise OSError("Restore target path contains symlink component")
                    restore_target: Path | None = src_abs
                    if src_abs.exists():
                        restore_target = _resolve_conflict_path(
                            src_abs.parent, src_abs.name, conflict_policy
                        )
                        if restore_target is None:
                            skipped += 1
                            log_cb(
                                f"[SKIP] Restore target occupied: "
                                f"{_sanitize_for_log(src_rel)}",
                                LOG_WARN,
                            )
                            progress_cb(idx, total, dst_abs.name)
                            continue
                        if restore_target != src_abs:
                            log_cb(
                                f"[WARN] Restore conflict resolved by rename: "
                                f"{_sanitize_for_log(restore_target.name)}",
                                LOG_WARN,
                            )
                    os.replace(dst_abs, restore_target)
                    processed += 1
                    undone_dsts.add(dst_rel)
        except OSError as exc:
            errors += 1
            log_cb(f"[ERROR] Undo failed for {_sanitize_for_log(dst_abs.name)}: {exc} ({_error_reason(exc)})", LOG_ERROR)
        progress_cb(idx, total, dst_abs.name)

    cleaned = _cleanup_empty_output_dirs(root_dir)
    if cancel_event.is_set():
        done_cb(False, f"Undo cancelled. Restored/deleted {processed:,}/{total:,}. Skipped {skipped:,}, errors {errors:,}, cleaned {cleaned:,}.", undone_dsts)
    else:
        done_cb(True, f"Undo complete. Restored/deleted {processed:,}. Skipped {skipped:,}, errors {errors:,}, cleaned {cleaned:,}.", undone_dsts)


class UndoDialog(tk.Toplevel):
    def __init__(self, parent: "FileOrganizerLiteApp", session: dict, root_dir: Path) -> None:
        super().__init__(parent)
        self._parent = parent
        self._session = session
        self._root_dir = root_dir
        self._vars: dict[str, tk.BooleanVar] = {}

        self.title("Undo - Choose What to Reverse")
        self.resizable(False, False)
        self.grab_set()
        self["padx"] = 14
        self["pady"] = 10

        self._groups = self._build_groups()
        self._build_header()
        self._build_selection()
        self._build_buttons()

    def _build_groups(self) -> dict[str, dict[str, list[dict]]]:
        groups: dict[str, dict[str, list[dict]]] = {}
        for rec in self._session.get("records", []):
            cat = str(rec.get("cat", "Documents"))
            sub = str(rec.get("sub", ""))
            groups.setdefault(cat, {}).setdefault(sub, []).append(rec)
        return groups

    def _build_header(self) -> None:
        op = self._session.get("op", "move").upper()
        count = len(self._session.get("records", []))
        ttk.Label(self, text=f"{op} session - {count:,} file(s)", foreground="#555555").pack(anchor="w", pady=(0, 8))

    def _build_selection(self) -> None:
        lf = ttk.LabelFrame(self, text=" Select what to undo ", padding=(8, 6))
        lf.pack(fill="both", expand=True, pady=(0, 10))
        for cat in ("Photos", "Videos", "Audio", "Documents"):
            if cat not in self._groups:
                continue
            subs = self._groups[cat]
            total_cat = sum(len(v) for v in subs.values())
            cv = tk.BooleanVar(value=True)
            self._vars[cat] = cv
            ttk.Checkbutton(lf, text=f"{cat} ({total_cat:,})", variable=cv, command=lambda c=cat, v=cv: self._cascade_down(c, v)).pack(anchor="w")
            for sub_name in sorted(k for k in subs.keys() if k):
                key = f"{cat}/{sub_name}"
                sv = tk.BooleanVar(value=True)
                self._vars[key] = sv
                ttk.Checkbutton(lf, text=f"   {sub_name} ({len(subs[sub_name]):,})", variable=sv, command=lambda c=cat: self._cascade_up(c)).pack(anchor="w")

    def _build_buttons(self) -> None:
        row = ttk.Frame(self)
        row.pack(fill="x")
        ttk.Button(row, text="Cancel", width=10, command=self.destroy).pack(side="right")
        ttk.Button(row, text="Undo selected", width=13, command=self._undo_selected).pack(side="right", padx=(0, 6))

    def _cascade_down(self, cat: str, cat_var: tk.BooleanVar) -> None:
        val = cat_var.get()
        for key in self._vars:
            if key != cat and key.startswith(f"{cat}/"):
                self._vars[key].set(val)

    def _cascade_up(self, cat: str) -> None:
        children = [k for k in self._vars if k != cat and k.startswith(f"{cat}/")]
        if children and cat in self._vars:
            self._vars[cat].set(any(self._vars[k].get() for k in children))

    def _selected_records(self) -> list[dict]:
        result: list[dict] = []
        for rec in self._session.get("records", []):
            cat = str(rec.get("cat", "Documents"))
            sub = str(rec.get("sub", ""))
            if sub:
                key = f"{cat}/{sub}"
                if key in self._vars:
                    if self._vars[key].get():
                        result.append(rec)
                    # Explicit subgroup choice wins over parent category fallback.
                    continue
            if cat in self._vars and self._vars[cat].get():
                result.append(rec)
        return result

    def _undo_selected(self) -> None:
        records = self._selected_records()
        if not records:
            messagebox.showinfo("Nothing selected", "Select at least one category.", parent=self)
            return
        self.destroy()
        self._parent._start_undo(self._session, records, self._root_dir)


class ToolTip:
    """Simple hover tooltip for Tk/ttk widgets."""

    def __init__(self, widget: tk.Widget, text: str, *, delay_ms: int = 350) -> None:
        self._widget = widget
        self._text = text.strip()
        self._delay_ms = delay_ms
        self._tip_window: tk.Toplevel | None = None
        self._show_job: str | None = None

        self._widget.bind("<Enter>", self._on_enter, add="+")
        self._widget.bind("<Leave>", self._on_leave, add="+")
        self._widget.bind("<ButtonPress>", self._on_leave, add="+")

    def _on_enter(self, _event: tk.Event) -> None:
        if self._tip_window or not self._text:
            return
        self._show_job = self._widget.after(self._delay_ms, self._show)

    def _on_leave(self, _event: tk.Event) -> None:
        if self._show_job is not None:
            self._widget.after_cancel(self._show_job)
            self._show_job = None
        self._hide()

    def _show(self) -> None:
        self._show_job = None
        if self._tip_window or not self._widget.winfo_exists():
            return
        self._tip_window = tk.Toplevel(self._widget)
        self._tip_window.wm_overrideredirect(True)
        self._tip_window.wm_attributes("-topmost", True)
        x = self._widget.winfo_rootx() + 14
        y = self._widget.winfo_rooty() + self._widget.winfo_height() + 8
        self._tip_window.wm_geometry(f"+{x}+{y}")
        label = tk.Label(
            self._tip_window,
            text=self._text,
            justify="left",
            bg="#2b2b2b",
            fg="#f3f3f3",
            relief="solid",
            bd=1,
            padx=8,
            pady=4,
            font=("Segoe UI", 9),
            wraplength=360,
        )
        label.pack()

    def _hide(self) -> None:
        if self._tip_window is not None:
            try:
                self._tip_window.destroy()
            finally:
                self._tip_window = None


class FileOrganizerLiteApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("MediaSort")
        self.resizable(True, True)
        self.minsize(820, 680)
        self["padx"] = 16
        self["pady"] = 14

        self.selected_dir = tk.StringVar(value="")
        self.operation = tk.StringVar(value="move")
        self.dry_run_var = tk.BooleanVar(value=False)
        self.subfolder_var = tk.BooleanVar(value=False)
        self.recursive_var = tk.BooleanVar(value=False)
        self.max_depth_var = tk.IntVar(value=_SCAN_MAX_DEPTH_DEFAULT)
        self.max_depth_display = tk.StringVar(value=str(_SCAN_MAX_DEPTH_DEFAULT))
        self.conflict_policy = tk.StringVar(value="rename")

        self._cancel_event = threading.Event()
        self._q_lock = threading.Lock()
        self._log_queue: list[tuple[str, str]] = []
        self._progress_queue: deque[tuple[int, int, str]] = deque()
        self._scan_done_val: int | None = None
        self._done_signal: tuple[bool, str] | None = None
        self._session_data: dict | None = None
        self._last_session_root: Path | None = None
        self._last_session: dict | None = None
        self._total_files = 0
        self._tooltips: list[ToolTip] = []
        self._max_depth_scale: ttk.Scale | None = None
        self._max_depth_value_label: ttk.Label | None = None
        self._max_depth_entry: ttk.Entry | None = None
        self._max_depth_help_label: ttk.Label | None = None
        self._recursive_state_label: ttk.Label | None = None
        self._recursive_trace_id: str | None = None

        self._apply_styles()
        self._build_ui()
        self.after(80, self._consume_queues)

    def _apply_styles(self) -> None:
        style = ttk.Style(self)
        for theme in ("clam", "vista", "winnative", "default"):
            try:
                style.theme_use(theme)
                break
            except tk.TclError:
                continue
        self.configure(bg="#eef2ff")
        style.configure(".", font=("Segoe UI", 10))
        style.configure("App.TFrame", background="#eef2ff")
        style.configure("Title.TLabel", font=("Segoe UI Semibold", 18), foreground="#4338ca", background="#eef2ff")
        style.configure("Subtle.TLabel", font=("Segoe UI", 9), foreground="#6366f1", background="#eef2ff")
        style.configure("Section.TLabelframe", padding=(12, 10), background="#eef2ff", borderwidth=1, relief="solid")
        style.configure("Section.TLabelframe.Label", font=("Segoe UI Semibold", 10), foreground="#4338ca", background="#eef2ff")
        style.configure("Options.TFrame", background="#e0e7ff")
        style.configure("Option.TRadiobutton", background="#e0e7ff", foreground="#312e81")
        style.configure("Option.TCheckbutton", background="#e0e7ff", foreground="#312e81")
        style.configure("Option.TLabel", background="#e0e7ff", foreground="#4338ca")
        style.configure("OptionState.TLabel", background="#e0e7ff", foreground="#4f46e5", font=("Segoe UI Semibold", 9))
        style.configure(
            "Option.Horizontal.TScale",
            background="#e0e7ff",
            troughcolor="#c7d2fe",
            bordercolor="#a5b4fc",
            lightcolor="#818cf8",
            darkcolor="#4338ca",
        )
        style.configure(
            "Option.TCombobox",
            fieldbackground="#eef2ff",
            background="#c7d2fe",
            foreground="#312e81",
            bordercolor="#a5b4fc",
            arrowcolor="#4338ca",
            selectbackground="#c7d2fe",
            selectforeground="#1f2937",
        )
        style.map(
            "Option.TCombobox",
            fieldbackground=[("readonly", "#eef2ff"), ("disabled", "#e5e7eb")],
            foreground=[("readonly", "#312e81"), ("disabled", "#9ca3af")],
            selectbackground=[("readonly", "#c7d2fe")],
        )
        style.map("Option.TRadiobutton", background=[("active", "#c7d2fe")], foreground=[("active", "#312e81")])
        style.map("Option.TCheckbutton", background=[("active", "#c7d2fe")], foreground=[("active", "#312e81")])
        style.configure(
            "Primary.TButton",
            padding=(14, 7),
            font=("Segoe UI Semibold", 10),
            foreground="#ffffff",
            background="#4f46e5",
            bordercolor="#3730a3",
            relief="flat",
        )
        style.map(
            "Primary.TButton",
            background=[("active", "#4338ca"), ("pressed", "#312e81"), ("disabled", "#a5b4fc")],
            foreground=[("disabled", "#e5e7eb")],
        )
        style.configure(
            "Secondary.TButton",
            padding=(11, 7),
            foreground="#3730a3",
            background="#e0e7ff",
            bordercolor="#a5b4fc",
        )
        style.map("Secondary.TButton", background=[("active", "#c7d2fe"), ("pressed", "#a5b4fc")])
        style.configure("StatLabel.TLabel", font=("Segoe UI", 9), foreground="#6366f1", background="#eef2ff")
        style.configure("StatValue.TLabel", font=("Segoe UI Semibold", 10), foreground="#111827", background="#eef2ff")
        style.configure("Status.TLabel", font=("Segoe UI Semibold", 10), foreground="#4f46e5", background="#eef2ff")
        style.configure("TProgressbar", troughcolor="#dbeafe", background="#6366f1", bordercolor="#a5b4fc", lightcolor="#818cf8", darkcolor="#4338ca")

    def _build_ui(self) -> None:
        header = ttk.Frame(self, style="App.TFrame")
        header.pack(fill="x", pady=(0, 10))
        tk.Frame(header, bg="#6366f1", height=3).pack(fill="x", pady=(0, 8))
        ttk.Label(header, text="MediaSort", style="Title.TLabel").pack(anchor="w")
        ttk.Label(
            header,
            text="Sorts photos, videos, audio, and documents. Other files are left untouched.",
            style="Subtle.TLabel",
        ).pack(anchor="w", pady=(3, 0))

        folder_frame = ttk.LabelFrame(self, text=" Target Folder ", style="Section.TLabelframe")
        folder_frame.pack(fill="x", pady=(0, 10), ipady=2)
        browse_btn = ttk.Button(folder_frame, text="Browse...", command=self._browse_folder, width=12, style="Secondary.TButton")
        browse_btn.pack(side="left")
        folder_entry = ttk.Entry(folder_frame, textvariable=self.selected_dir, state="readonly", font=("Segoe UI", 10))
        folder_entry.pack(side="left", padx=(10, 0), fill="x", expand=True, ipady=2)

        opt = ttk.LabelFrame(self, text=" Options ", style="Section.TLabelframe")
        opt.pack(fill="x", pady=(0, 10), ipady=2)
        row1 = ttk.Frame(opt, style="Options.TFrame")
        row1.pack(fill="x")
        move_radio = ttk.Radiobutton(row1, text="Move files", variable=self.operation, value="move", style="Option.TRadiobutton")
        move_radio.pack(side="left", padx=(0, 16))
        copy_radio = ttk.Radiobutton(row1, text="Copy files", variable=self.operation, value="copy", style="Option.TRadiobutton")
        copy_radio.pack(side="left", padx=(0, 16))
        dry_run_check = ttk.Checkbutton(row1, text="Preview only (no changes)", variable=self.dry_run_var, style="Option.TCheckbutton")
        dry_run_check.pack(side="left")

        row2 = ttk.Frame(opt, style="Options.TFrame")
        row2.pack(fill="x", pady=(8, 0))
        subfolder_check = ttk.Checkbutton(row2, text="Organize into sub-folders", variable=self.subfolder_var, style="Option.TCheckbutton")
        subfolder_check.pack(side="left")
        recursive_check = ttk.Checkbutton(row2, text="Scan inside sub-folders", variable=self.recursive_var, style="Option.TCheckbutton")
        recursive_check.pack(side="left", padx=(16, 0))
        self._recursive_state_label = ttk.Label(row2, text="OFF", style="OptionState.TLabel")
        self._recursive_state_label.pack(side="left", padx=(6, 0))
        max_depth_label = ttk.Label(row2, text="Max depth:", style="Option.TLabel")
        max_depth_label.pack(side="left", padx=(16, 4))
        self._max_depth_scale = ttk.Scale(
            row2,
            from_=1,
            to=200,
            value=float(self.max_depth_var.get()),
            command=self._on_max_depth_scale,
            style="Option.Horizontal.TScale",
        )
        self._max_depth_scale.pack(side="left")
        self._max_depth_value_label = ttk.Label(row2, textvariable=self.max_depth_display, style="StatValue.TLabel", width=4)
        self._max_depth_value_label.pack(side="left", padx=(6, 0))
        self._max_depth_entry = ttk.Entry(row2, width=5, textvariable=self.max_depth_display, justify="center")
        self._max_depth_entry.pack(side="left", padx=(6, 0))
        self._max_depth_entry.bind("<Return>", self._on_max_depth_entry_commit, add="+")
        self._max_depth_entry.bind("<FocusOut>", self._on_max_depth_entry_commit, add="+")
        self._max_depth_help_label = ttk.Label(
            row2,
            text="(Only used when recursive scan is ON)",
            style="Subtle.TLabel",
        )
        self._max_depth_help_label.pack(side="left", padx=(8, 10))
        conflict_label = ttk.Label(row2, text="Conflict:", style="Option.TLabel")
        conflict_label.pack(side="left", padx=(14, 4))
        conflict_combo = ttk.Combobox(
            row2,
            width=10,
            state="readonly",
            textvariable=self.conflict_policy,
            values=("rename", "overwrite", "skip"),
            style="Option.TCombobox",
        )
        conflict_combo.pack(side="left")

        ctrl = ttk.Frame(self, style="App.TFrame")
        ctrl.pack(fill="x", pady=(2, 8))
        self.start_btn = ttk.Button(ctrl, text="Start Organizing", width=16, command=self._start, style="Primary.TButton")
        self.start_btn.pack(side="left")
        self.cancel_btn = ttk.Button(ctrl, text="Cancel", width=12, command=self._cancel, state="disabled", style="Secondary.TButton")
        self.cancel_btn.pack(side="left", padx=(8, 0))
        self.undo_btn = ttk.Button(ctrl, text="Undo Last Session", width=16, command=self._show_undo_dialog, state="disabled", style="Secondary.TButton")
        self.undo_btn.pack(side="left", padx=(8, 0))
        self.status_label = ttk.Label(ctrl, text="Idle", style="Status.TLabel")
        self.status_label.pack(side="left", padx=(16, 0))

        stats = ttk.Frame(self, style="App.TFrame")
        stats.pack(fill="x", pady=(0, 6))
        ttk.Label(stats, text="Total files:", style="StatLabel.TLabel").pack(side="left")
        self.total_label = ttk.Label(stats, text="-", style="StatValue.TLabel")
        self.total_label.pack(side="left", padx=(4, 24))
        ttk.Label(stats, text="Processing:", style="StatLabel.TLabel").pack(side="left")
        self.current_label = ttk.Label(stats, text="-", style="StatValue.TLabel")
        self.current_label.pack(side="left", padx=(4, 0))

        self.progress = ttk.Progressbar(self, orient="horizontal", mode="determinate")
        self.progress.pack(fill="x", pady=(2, 10), ipady=2)
        log_frame = ttk.LabelFrame(self, text=" Activity Log ", style="Section.TLabelframe")
        log_frame.pack(fill="both", expand=True, ipady=2)
        self.log_area = scrolledtext.ScrolledText(
            log_frame,
            state="disabled",
            font=("Consolas", 9),
            bg="#ffffff",
            fg="#1f2937",
            insertbackground="#111827",
            relief="flat",
            borderwidth=0,
            padx=8,
            pady=8,
        )
        self.log_area.pack(fill="both", expand=True)
        self.log_area.tag_configure(LOG_INFO, foreground="#374151")
        self.log_area.tag_configure(LOG_WARN, foreground="#c2410c")
        self.log_area.tag_configure(LOG_ERROR, foreground="#be123c")
        self.log_area.tag_configure(LOG_DONE, foreground="#047857")
        self.log_area.tag_configure(LOG_PREVIEW, foreground="#4338ca")
        self.log_area.tag_configure(LOG_SEP, foreground="#a1a1aa")

        self._tooltips = [
            ToolTip(browse_btn, "Pick the folder whose files you want to organize."),
            ToolTip(folder_entry, "Shows the currently selected folder path."),
            ToolTip(move_radio, "Move files into organizer folders. Original files are relocated."),
            ToolTip(copy_radio, "Copy files into organizer folders and keep originals in place."),
            ToolTip(dry_run_check, "Preview what would happen without moving or copying any files."),
            ToolTip(subfolder_check, "Group files by type-specific subfolders (JPEG, PDF, MP4, etc.)."),
            ToolTip(recursive_check, "Turn recursive scanning ON/OFF. OFF = only top folder files. ON = includes subfolders."),
            ToolTip(self._recursive_state_label, "Current recursion state."),
            ToolTip(max_depth_label, "Maximum folder depth when 'Scan inside sub-folders' is enabled. Example: depth 1 scans only immediate child folders; depth 3 scans children, grandchildren, and great-grandchildren."),
            ToolTip(self._max_depth_scale, "Drag to set depth (1-200). Smaller is faster; larger scans deeper folders."),
            ToolTip(self._max_depth_value_label, "Current recursion depth value."),
            ToolTip(self._max_depth_entry, "Type depth manually (1-200), then press Enter."),
            ToolTip(self._max_depth_help_label, "Simple guide: 1 = one level deep, 2 = two levels deep, etc. This setting is ignored when recursive scan is off."),
            ToolTip(conflict_label, "Choose what happens when destination file already exists."),
            ToolTip(conflict_combo, "rename: keep both, overwrite: replace existing, skip: ignore duplicate."),
            ToolTip(self.start_btn, "Start organizing using the selected options."),
            ToolTip(self.cancel_btn, "Request cancellation of the current organize/undo operation."),
            ToolTip(self.undo_btn, "Open undo for the most recent recorded session in this folder."),
            ToolTip(self.progress, "Shows scan/processing progress for the current run."),
            ToolTip(self.log_area, "Detailed activity log with previews, progress, warnings, and errors."),
        ]
        if self._recursive_trace_id is None:
            self._recursive_trace_id = self.recursive_var.trace_add("write", self._on_recursive_var_change)
        recursive_check.configure(command=self._on_recursive_toggle)
        self._on_recursive_toggle()

    def _on_max_depth_scale(self, value: str) -> None:
        depth = max(1, min(200, int(round(float(value)))))
        if self.max_depth_var.get() != depth:
            self.max_depth_var.set(depth)
        self.max_depth_display.set(str(depth))

    def _on_max_depth_entry_commit(self, _event: tk.Event | None = None) -> None:
        raw = self.max_depth_display.get().strip()
        try:
            depth = int(raw)
        except ValueError:
            depth = self.max_depth_var.get()
        depth = max(1, min(200, depth))
        self.max_depth_var.set(depth)
        self.max_depth_display.set(str(depth))
        if self._max_depth_scale is not None:
            self._max_depth_scale.set(float(depth))

    def _on_recursive_toggle(self) -> None:
        enabled = bool(self.recursive_var.get())
        state = "normal" if enabled else "disabled"
        if self._max_depth_scale is not None:
            self._max_depth_scale.configure(state=state)
        if self._max_depth_value_label is not None:
            self._max_depth_value_label.configure(foreground="#111827" if enabled else "#9ca3af")
        if self._max_depth_entry is not None:
            self._max_depth_entry.configure(state=state)
        if self._max_depth_help_label is not None:
            self._max_depth_help_label.configure(
                text="(Only used when recursive scan is ON)" if enabled else "(Recursive scan OFF: top folder only)"
            )
        if self._recursive_state_label is not None:
            self._recursive_state_label.configure(text="ON" if enabled else "OFF", foreground="#059669" if enabled else "#b45309")

    def _on_recursive_var_change(self, *_args: object) -> None:
        self._on_recursive_toggle()

    def _consume_queues(self) -> None:
        with self._q_lock:
            logs = self._log_queue[:]
            self._log_queue.clear()
            progress_updates = list(self._progress_queue)
            self._progress_queue.clear()
            scan_done = self._scan_done_val
            self._scan_done_val = None
            done = self._done_signal
            self._done_signal = None
            session_data = self._session_data
            self._session_data = None
        if logs:
            self.log_area["state"] = "normal"
            try:
                for msg, level in logs:
                    self.log_area.insert("end", msg + "\n", level)
                self.log_area.see("end")
            finally:
                self.log_area["state"] = "disabled"
        if scan_done is not None:
            self._total_files = scan_done
            self.progress.stop()
            self.progress["mode"] = "determinate"
            self.progress["maximum"] = max(scan_done, 1)
            self.progress["value"] = 0
            self.total_label["text"] = f"{scan_done:,}"
        if progress_updates and self.progress["mode"] == "determinate":
            cur, total, filename = progress_updates[-1]
            self.progress["value"] = cur
            self.current_label["text"] = filename
            pct = int(cur / total * 100) if total else 0
            self.status_label["text"] = f"Processing... {pct}%"
            self.status_label["foreground"] = "#4f46e5"
        if session_data is not None:
            self._on_new_session(session_data)
        if done is not None:
            self._finish(*done)
        self.after(80, self._consume_queues)

    def _enqueue_log(self, message: str, level: str = LOG_INFO) -> None:
        with self._q_lock:
            self._log_queue.append((message, level))

    def _enqueue_progress(self, current: int, total: int, filename: str) -> None:
        with self._q_lock:
            self._progress_queue.append((current, total, filename))

    def _enqueue_scan_done(self, total: int) -> None:
        with self._q_lock:
            self._scan_done_val = total

    def _enqueue_done(self, success: bool, summary: str) -> None:
        with self._q_lock:
            self._done_signal = (success, summary)

    def _enqueue_session(self, session: dict | None) -> None:
        if session and session.get("records"):
            with self._q_lock:
                self._session_data = session

    def _on_new_session(self, session: dict) -> None:
        self._last_session = session
        root_dir = self._last_session_root
        if root_dir and root_dir.is_dir():
            hf = root_dir / _HISTORY_FILENAME
            sessions = _load_history(hf)
            sessions.append(session)
            if not _save_history(hf, sessions, self._enqueue_log):
                self._enqueue_log(
                    "[WARN] Undo history could not be saved. Undo may be unavailable.",
                    LOG_WARN,
                )
        self.undo_btn["state"] = "normal"

    def _refresh_undo_state(self) -> None:
        root_dir = self._last_session_root
        if root_dir is None or not root_dir.is_dir():
            self.undo_btn["state"] = "disabled"
            self._last_session = None
            return
        latest = _quick_latest_session(root_dir / _HISTORY_FILENAME)
        if latest:
            self._last_session = latest
            self.undo_btn["state"] = "normal"
        else:
            self._last_session = None
            self.undo_btn["state"] = "disabled"

    def _finish(self, success: bool, summary: str) -> None:
        self.progress.stop()
        self.log_area["state"] = "normal"
        try:
            self.log_area.insert("end", "-" * 56 + "\n", LOG_SEP)
            self.log_area.insert("end", ("[OK] " if success else "[!] ") + summary + "\n", LOG_DONE if success else LOG_WARN)
            self.log_area.see("end")
        finally:
            self.log_area["state"] = "disabled"
        self.start_btn["state"] = "normal"
        self.cancel_btn["state"] = "disabled"
        self.current_label["text"] = "-"
        self.status_label["text"] = "Completed" if success else "Stopped"
        self.status_label["foreground"] = "#059669" if success else "#c2410c"
        self.progress["mode"] = "determinate"
        if success and self._total_files > 0:
            self.progress["value"] = self.progress["maximum"]
        elif not success:
            self.progress["value"] = 0
        self._refresh_undo_state()

    def _browse_folder(self) -> None:
        picked = filedialog.askdirectory(title="Select folder to organize")
        if picked:
            self.selected_dir.set(picked)
            self._last_session_root = Path(picked)
            self._refresh_undo_state()

    def _start(self) -> None:
        path = self.selected_dir.get().strip()
        if not path or not Path(path).is_dir():
            messagebox.showerror("Invalid folder", "Please select a valid folder first.")
            return
        root_dir = Path(path)
        max_depth = self.max_depth_var.get()
        if max_depth < 1 or max_depth > 200:
            messagebox.showerror("Invalid depth", "Max depth must be between 1 and 200.")
            return
        if core.is_protected_path(root_dir):
            messagebox.showerror("Protected Directory", "This location is system-critical and blocked.")
            return
        if core.is_outside_home(root_dir):
            if not messagebox.askyesno("Outside Home Directory", "Folder is outside your user home. Continue anyway?"):
                return
        if self.operation.get() == "move" and not self.dry_run_var.get():
            if not messagebox.askyesno("Confirm Move", "Files will be moved into organizer folders. Continue?"):
                return

        self._last_session_root = root_dir
        self._cancel_event.clear()
        self.start_btn["state"] = "disabled"
        self.cancel_btn["state"] = "normal"
        self.undo_btn["state"] = "disabled"
        self.progress["mode"] = "indeterminate"
        self.progress["value"] = 0
        self.progress.start(10)
        self.total_label["text"] = "-"
        self.current_label["text"] = "-"
        self.status_label["text"] = "Scanning..."
        self.status_label["foreground"] = "#4f46e5"

        self._enqueue_log("-" * 56, LOG_SEP)
        self._enqueue_log(f"Folder    : {path}", LOG_INFO)
        self._enqueue_log(f"Operation : {self.operation.get().upper()}" + (" (DRY RUN)" if self.dry_run_var.get() else ""), LOG_INFO)
        self._enqueue_log("Scope     : Photos, Videos, Audio, Documents only", LOG_INFO)
        self._enqueue_log(f"Subfolders: {'enabled' if self.subfolder_var.get() else 'disabled'}", LOG_INFO)
        self._enqueue_log(f"Scan mode : {'recursive' if self.recursive_var.get() else 'loose files only'}", LOG_INFO)
        self._enqueue_log(f"Max depth : {max_depth}", LOG_INFO)
        self._enqueue_log(f"Conflicts : {self.conflict_policy.get()}", LOG_INFO)
        self._enqueue_log("Ignored   : all other extensions (e.g. .dll, .exe, unknown)", LOG_INFO)
        self._enqueue_log("-" * 56, LOG_SEP)

        thread = threading.Thread(
            target=organize_files,
            args=(
                root_dir,
                self.operation.get(),
                self.dry_run_var.get(),
                self.subfolder_var.get(),
                self.recursive_var.get(),
                max_depth,
                self.conflict_policy.get(),
                self._cancel_event,
                self._enqueue_scan_done,
                self._enqueue_progress,
                self._enqueue_log,
                self._enqueue_session,
                self._enqueue_done,
            ),
            daemon=True,
        )
        thread.start()

    def _cancel(self) -> None:
        self._cancel_event.set()
        self.cancel_btn["state"] = "disabled"
        self.status_label["text"] = "Cancelling..."
        self.status_label["foreground"] = "#c2410c"
        self._enqueue_log("Cancellation requested...", LOG_WARN)

    def _show_undo_dialog(self) -> None:
        root_dir = self._last_session_root
        if root_dir is None:
            messagebox.showinfo("No History", "Select a folder first to check undo history.")
            return
        sessions = _load_history(root_dir / _HISTORY_FILENAME)
        if not sessions:
            messagebox.showinfo("No History", "No undo history found for this folder.")
            self.undo_btn["state"] = "disabled"
            return
        UndoDialog(self, sessions[-1], root_dir)

    def _start_undo(self, session: dict, records: list[dict], root_dir: Path) -> None:
        session_id = session.get("id", "")
        op_type = session.get("op", "move")

        def _undo_done(success: bool, summary: str, undone_dsts: set[str]) -> None:
            try:
                hf = root_dir / _HISTORY_FILENAME
                all_sessions = _load_history(hf)
                updated = []
                for s in all_sessions:
                    if s.get("id") == session_id:
                        remaining = [r for r in s.get("records", []) if r.get("dst", "") not in undone_dsts]
                        if remaining:
                            s = dict(s)
                            s["records"] = remaining
                            updated.append(s)
                    else:
                        updated.append(s)
                if not _save_history(hf, updated, self._enqueue_log):
                    self._enqueue_log(
                        "[WARN] Failed to update history after undo.",
                        LOG_WARN,
                    )
            finally:
                self._enqueue_done(success, summary)

        self._cancel_event.clear()
        self.start_btn["state"] = "disabled"
        self.undo_btn["state"] = "disabled"
        self.cancel_btn["state"] = "normal"
        self.progress["mode"] = "indeterminate"
        self.progress["value"] = 0
        self.progress.start(10)
        self.total_label["text"] = "-"
        self.current_label["text"] = "-"
        self.status_label["text"] = "Undoing..."
        self.status_label["foreground"] = "#7c3aed"

        thread = threading.Thread(
            target=_undo_worker,
            args=(
                list(records),
                root_dir,
                op_type,
                self.conflict_policy.get(),
                self._cancel_event,
                self._enqueue_progress,
                self._enqueue_log,
                _undo_done,
            ),
            daemon=True,
        )
        thread.start()


if __name__ == "__main__":
    app = FileOrganizerLiteApp()
    app.mainloop()
