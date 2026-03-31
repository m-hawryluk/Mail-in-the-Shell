#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import ipaddress
import io
import json
import mimetypes
import os
import re
import secrets
import shutil
import sqlite3
import subprocess
import threading
import time
import webbrowser
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from bulk_mailer import (
    AttachmentSpec,
    CampaignLock,
    CampaignLockError,
    acquire_campaign_lock_nowait,
    analyze_contacts,
    append_sent_copy,
    build_message,
    build_templates,
    describe_smtp_exception,
    describe_imap_exception,
    held_campaign_lock,
    ensure_imap_sent_folder,
    imap_config_from_values,
    load_state,
    normalize_email,
    open_smtp_connection,
    open_imap_connection,
    parse_email_list,
    render_template,
    release_campaign_lock,
    send_batch,
    smtp_config_from_values,
    smtp_supports_smtputf8,
    smtp_utf8_requirement_message,
    test_imap_connection,
    test_smtp_connection,
    utc_now,
    wrap_text_as_html,
)


APP_DIR = Path(__file__).resolve().parent
WEB_DIR = APP_DIR / "web"
DATA_DIR = APP_DIR / "storage"
STATE_DIR = APP_DIR / "state" / "web"
SETTINGS_FILE = DATA_DIR / "web_settings.json"
TEMPLATE_DIR = DATA_DIR / "templates"
CAMPAIGN_DIR = DATA_DIR / "campaigns"
INDEX_DIR = DATA_DIR / "library_index"
TEMPLATE_INDEX_DIR = INDEX_DIR / "templates"
CAMPAIGN_INDEX_DIR = INDEX_DIR / "campaigns"
LOG_DIR = DATA_DIR / "logs"
LOG_DB_FILE = LOG_DIR / "activity_logs.db"
TOP_RIGHT_GIF = WEB_DIR / "top-right.gif"
TITLE_IMAGE = APP_DIR / "5B1BA9DC-191C-44A2-9C63-499DF06C69E5.png"
KEYCHAIN_SERVICE = "mail-in-the-shell"
TEST_EMAIL_RECIPIENT = os.getenv("MAILER_TEST_EMAIL_RECIPIENT", "test@example.com").strip() or "test@example.com"
SESSION_COOKIE_NAME = "mail_in_the_shell_session"
SESSION_TTL_SECONDS = 60 * 60 * 12
FINISHED_JOB_TTL_SECONDS = 60 * 30
MAX_REQUEST_BYTES = 32 * 1024 * 1024
MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024
STYLE_BLOCK_PATTERN = re.compile(r"(?is)<style\b[^>]*>(.*?)</style>")
SCRIPT_BLOCK_PATTERN = re.compile(r"(?is)<script\b[^>]*>.*?</script>")
IFRAME_BLOCK_PATTERN = re.compile(r"(?is)<iframe\b[^>]*>.*?</iframe>")
LINK_TAG_PATTERN = re.compile(r"(?is)<link\b[^>]*>")
STYLE_ATTR_PATTERN = re.compile(r"""(?is)\sstyle\s*=\s*("([^"]*)"|'([^']*)')""")
SRC_ATTR_PATTERN = re.compile(r"""(?is)\s(src|poster|background)\s*=\s*("([^"]*)"|'([^']*)'|([^\s>]+))""")
SRCSET_ATTR_PATTERN = re.compile(r"""(?is)\ssrcset\s*=\s*("([^"]*)"|'([^']*)'|([^\s>]+))""")
CSS_URL_PATTERN = re.compile(r"""(?is)url\(\s*(?:'([^']*)'|"([^"]*)"|([^)]*))\s*\)""")
CSS_IMPORT_PATTERN = re.compile(r"(?is)@import\s+[^;]+;?")
ALLOWED_LOG_TONES = {"info", "success", "warning", "error"}
BOOTSTRAP_PATH = "/api/bootstrap"
LOG_EXPORT_PATH = "/api/log-export.csv"


DEFAULT_PROFILE = {
    "host": "smtp.example.com",
    "port": 587,
    "username": "hello@example.com",
    "from_email": "hello@example.com",
    "from_name": "Mail in the Shell",
    "reply_to": "hello@example.com",
    "use_starttls": True,
    "use_ssl": False,
    "verify_tls": False,
    "save_sent_copy": False,
    "imap_host": "imap.example.com",
    "imap_port": 993,
    "imap_username": "hello@example.com",
    "imap_sent_folder": "Sent",
    "imap_use_ssl": True,
    "imap_verify_tls": False,
    "unsubscribe_email": "",
    "unsubscribe_url": "",
    "batch_size": 100,
    "pause_seconds": 1.0,
    "campaign_name": "",
    "max_attempts_per_row": 3,
    "retry_exhausted": False,
}


def migrate_profile_defaults(profile: dict[str, Any]) -> dict[str, Any]:
    migrated = profile.copy()
    reply_to = str(migrated.get("reply_to") or "").strip()
    fallback_reply_to = str(migrated.get("from_email") or DEFAULT_PROFILE["reply_to"]).strip()
    if reply_to and "@" not in reply_to:
        migrated["reply_to"] = fallback_reply_to
    return migrated


def is_loopback_host(host: str) -> bool:
    cleaned_host = host.strip().lower()
    if cleaned_host == "localhost":
        return True
    if cleaned_host.startswith("[") and cleaned_host.endswith("]"):
        cleaned_host = cleaned_host[1:-1]
    try:
        return ipaddress.ip_address(cleaned_host).is_loopback
    except ValueError:
        return False


def preview_url_is_safe(raw_url: str) -> bool:
    cleaned_url = raw_url.strip().strip("\"'").lower()
    if not cleaned_url:
        return True
    return cleaned_url.startswith(("data:", "cid:", "about:", "#"))


def sanitize_preview_css(css_text: str) -> str:
    css_text = CSS_IMPORT_PATTERN.sub("", css_text)

    def replace_url(match: re.Match[str]) -> str:
        raw_url = next((group for group in match.groups() if group is not None), "").strip()
        if preview_url_is_safe(raw_url):
            return f"url('{raw_url}')"
        return "url('')"

    return CSS_URL_PATTERN.sub(replace_url, css_text)


def sanitize_preview_html(html_content: str) -> str:
    sanitized = SCRIPT_BLOCK_PATTERN.sub("", html_content)
    sanitized = IFRAME_BLOCK_PATTERN.sub("", sanitized)
    sanitized = LINK_TAG_PATTERN.sub("", sanitized)
    sanitized = STYLE_BLOCK_PATTERN.sub(lambda match: f"<style>{sanitize_preview_css(match.group(1))}</style>", sanitized)

    def replace_style_attr(match: re.Match[str]) -> str:
        raw_style = match.group(2) if match.group(2) is not None else match.group(3) or ""
        return f' style="{sanitize_preview_css(raw_style)}"'

    sanitized = STYLE_ATTR_PATTERN.sub(replace_style_attr, sanitized)

    def replace_src_attr(match: re.Match[str]) -> str:
        attribute_name = match.group(1).lower()
        raw_value = next((group for group in match.groups()[2:] if group is not None), "").strip()
        if preview_url_is_safe(raw_value):
            return f' {attribute_name}="{raw_value}"'
        return f' {attribute_name}="" data-preview-blocked-{attribute_name}="true"'

    sanitized = SRC_ATTR_PATTERN.sub(replace_src_attr, sanitized)

    def replace_srcset_attr(match: re.Match[str]) -> str:
        raw_value = next((group for group in match.groups()[1:] if group is not None), "").strip()
        candidates = [part.strip().split(" ", 1)[0] for part in raw_value.split(",") if part.strip()]
        if candidates and all(preview_url_is_safe(candidate) for candidate in candidates):
            return f' srcset="{raw_value}"'
        return ' srcset="" data-preview-blocked-srcset="true"'

    return SRCSET_ATTR_PATTERN.sub(replace_srcset_attr, sanitized)


def session_mode_for_request(method: str, path: str) -> str:
    if method == "GET" and path == BOOTSTRAP_PATH:
        return "create"
    if method == "GET" and path == LOG_EXPORT_PATH:
        return "require"
    if method == "POST":
        return "require"
    return "none"


def extract_request_access_token(raw_path: str, header_token: str | None) -> str:
    candidate = str(header_token or "").strip()
    if candidate:
        return candidate
    query_token = parse_qs(urlparse(raw_path).query).get("access_token", [""])[0]
    return str(query_token or "").strip()


def saved_item_index_dir(kind: str) -> Path:
    if kind == "template":
        return TEMPLATE_INDEX_DIR
    if kind == "campaign":
        return CAMPAIGN_INDEX_DIR
    raise SystemExit(f"Unknown saved-item kind: {kind}")


def saved_item_summary(payload: dict[str, Any], path: Path, kind: str) -> dict[str, Any]:
    return {
        "id": str(payload.get("id") or path.stem),
        "name": str(payload.get("name") or path.stem),
        "savedAt": str(payload.get("saved_at") or payload.get("updated_at") or ""),
        "bodyMode": str(payload.get("body_mode") or "html"),
        "attachmentCount": len(payload.get("attachments") or []),
        "recipientCount": int(payload.get("recipient_count") or 0),
        "kind": kind,
    }


def write_saved_item(path: Path, payload: dict[str, Any], kind: str) -> None:
    write_private_json(path, payload)
    index_path = saved_item_index_dir(kind) / f"{path.stem}.json"
    write_private_json(index_path, saved_item_summary(payload, path, kind))


def ensure_private_storage() -> None:
    for directory in (
        DATA_DIR,
        STATE_DIR,
        TEMPLATE_DIR,
        CAMPAIGN_DIR,
        INDEX_DIR,
        TEMPLATE_INDEX_DIR,
        CAMPAIGN_INDEX_DIR,
        LOG_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)
        try:
            directory.chmod(0o700)
        except OSError:
            pass


class ActivityLogStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._init_lock = threading.Lock()
        self._conn_lock = threading.Lock()
        self._initialized = False
        self._connection: sqlite3.Connection | None = None

    def _get_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            self._connection = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        return self._connection

    def ensure_ready(self) -> None:
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                self.db_path.parent.chmod(0o700)
            except OSError:
                pass
            with self._conn_lock:
                connection = self._get_connection()
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=NORMAL")
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS activity_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at TEXT NOT NULL,
                        source TEXT NOT NULL,
                        session_id TEXT NOT NULL,
                        campaign_key TEXT NOT NULL,
                        job_id TEXT NOT NULL,
                        tone TEXT NOT NULL,
                        title TEXT NOT NULL,
                        message TEXT NOT NULL,
                        recipient TEXT NOT NULL,
                        row_number INTEGER
                    )
                    """
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_activity_logs_created_at ON activity_logs(created_at)"
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_activity_logs_campaign_key ON activity_logs(campaign_key)"
                )
                connection.commit()
            try:
                self.db_path.chmod(0o600)
            except OSError:
                pass
            self._initialized = True

    def append(
        self,
        *,
        source: str,
        title: str,
        message: str,
        tone: str = "info",
        session_id: str = "",
        campaign_key: str = "",
        job_id: str = "",
        recipient: str = "",
        row_number: int | None = None,
        created_at: str | None = None,
    ) -> None:
        self.ensure_ready()
        created_at_value = str(created_at or utc_now())
        tone_value = str(tone or "info").strip().lower()
        if tone_value not in ALLOWED_LOG_TONES:
            tone_value = "info"
        with self._conn_lock:
            connection = self._get_connection()
            connection.execute(
                """
                INSERT INTO activity_logs (
                    created_at,
                    source,
                    session_id,
                    campaign_key,
                    job_id,
                    tone,
                    title,
                    message,
                    recipient,
                    row_number
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at_value,
                    str(source or "ui").strip() or "ui",
                    str(session_id or "").strip(),
                    str(campaign_key or "").strip(),
                    str(job_id or "").strip(),
                    tone_value,
                    str(title or "").strip() or "Activity",
                    str(message or ""),
                    str(recipient or "").strip(),
                    row_number,
                ),
            )
            connection.commit()
        try:
            self.db_path.chmod(0o600)
        except OSError:
            pass

    def count(self) -> int:
        self.ensure_ready()
        with self._conn_lock:
            connection = self._get_connection()
            row = connection.execute("SELECT COUNT(*) FROM activity_logs").fetchone()
        return int(row[0] if row else 0)

    def export_csv_bytes(self) -> bytes:
        self.ensure_ready()
        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer)
        writer.writerow(
            [
                "id",
                "created_at",
                "source",
                "tone",
                "title",
                "message",
                "session_id",
                "campaign_key",
                "job_id",
                "recipient",
                "row_number",
            ]
        )
        with self._conn_lock:
            connection = self._get_connection()
            rows = connection.execute(
                """
                SELECT
                    id,
                    created_at,
                    source,
                    tone,
                    title,
                    message,
                    session_id,
                    campaign_key,
                    job_id,
                    recipient,
                    row_number
                FROM activity_logs
                ORDER BY id
                """
            ).fetchall()
        for row in rows:
            writer.writerow(row)
        return ("\ufeff" + buffer.getvalue()).encode("utf-8")

    def export_filename(self) -> str:
        timestamp = time.strftime("%Y%m%d-%H%M%S", time.localtime())
        return f"mail-in-the-shell-activity-{timestamp}.csv"


ACTIVITY_LOG_STORE = ActivityLogStore(LOG_DB_FILE)


def write_private_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        pass


def read_private_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Saved item not found: {path.name}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Saved item is not valid JSON: {path.name}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"Saved item is invalid: {path.name}")
    return payload


def list_saved_records(directory: Path, kind: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    index_directory = saved_item_index_dir(kind)
    for path in sorted(directory.glob("*.json")):
        index_path = index_directory / f"{path.stem}.json"
        summary: dict[str, Any] | None = None
        if index_path.exists():
            try:
                indexed_payload = read_private_json(index_path)
            except SystemExit:
                indexed_payload = {}
            if indexed_payload:
                summary = {
                    "id": str(indexed_payload.get("id") or path.stem),
                    "name": str(indexed_payload.get("name") or path.stem),
                    "savedAt": str(indexed_payload.get("savedAt") or indexed_payload.get("saved_at") or ""),
                    "bodyMode": str(indexed_payload.get("bodyMode") or indexed_payload.get("body_mode") or "html"),
                    "attachmentCount": int(indexed_payload.get("attachmentCount") or 0),
                    "recipientCount": int(indexed_payload.get("recipientCount") or 0),
                    "kind": kind,
                }
        if summary is None:
            payload: dict[str, Any]
            try:
                payload = read_private_json(path)
            except SystemExit:
                continue
            summary = saved_item_summary(payload, path, kind)
            try:
                write_private_json(index_path, summary)
            except OSError:
                pass
        records.append(summary)
    records.sort(key=lambda item: (item["savedAt"], item["name"].lower()), reverse=True)
    return records


def library_payload() -> dict[str, list[dict[str, Any]]]:
    return {
        "templates": list_saved_records(TEMPLATE_DIR, "template"),
        "campaigns": list_saved_records(CAMPAIGN_DIR, "campaign"),
    }


def load_saved_profile() -> dict[str, Any]:
    if not SETTINGS_FILE.exists():
        return {}
    try:
        data = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    return migrate_profile_defaults(data)


def save_profile(profile: dict[str, Any]) -> None:
    write_private_json(SETTINGS_FILE, profile)


def keychain_available() -> bool:
    return shutil.which("security") is not None


def keychain_has_password(account: str) -> bool:
    if not keychain_available() or not account:
        return False
    result = subprocess.run(
        ["security", "find-generic-password", "-s", KEYCHAIN_SERVICE, "-a", account],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def keychain_get_password(account: str) -> str | None:
    if not keychain_available() or not account:
        return None
    result = subprocess.run(
        ["security", "find-generic-password", "-w", "-s", KEYCHAIN_SERVICE, "-a", account],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def keychain_set_password(account: str, password: str) -> None:
    if not keychain_available():
        raise SystemExit("macOS Keychain is not available on this machine.")
    if not account:
        raise SystemExit("Enter the SMTP username before saving a password to Keychain.")
    subprocess.run(
        ["security", "add-generic-password", "-U", "-s", KEYCHAIN_SERVICE, "-a", account, "-w", password],
        capture_output=True,
        text=True,
        check=True,
    )


def keychain_delete_password(account: str) -> bool:
    if not keychain_available() or not account:
        return False
    result = subprocess.run(
        ["security", "delete-generic-password", "-s", KEYCHAIN_SERVICE, "-a", account],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    if value is None:
        return default
    return bool(value)


def parse_int(value: Any, default: int) -> int:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise SystemExit("Expected a whole number.") from exc


def parse_float(value: Any, default: float) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise SystemExit("Expected a numeric value.") from exc


def merged_profile() -> dict[str, Any]:
    profile = DEFAULT_PROFILE.copy()
    profile.update(load_saved_profile())
    return profile


def normalize_smtp_payload(payload: dict[str, Any]) -> dict[str, Any]:
    smtp = payload.get("smtp") if isinstance(payload, dict) else {}
    smtp = smtp if isinstance(smtp, dict) else {}
    base = merged_profile()
    normalized = {
        "host": str(smtp.get("host", base["host"])).strip(),
        "port": parse_int(smtp.get("port", base["port"]), int(base["port"])),
        "username": str(smtp.get("username", base["username"])).strip(),
        "password": str(smtp.get("password", "")).strip(),
        "from_email": str(smtp.get("from_email", base["from_email"])).strip(),
        "from_name": str(smtp.get("from_name", base["from_name"])).strip(),
        "reply_to": str(smtp.get("reply_to", base["reply_to"])).strip(),
        "use_starttls": as_bool(smtp.get("use_starttls", base["use_starttls"]), True),
        "use_ssl": as_bool(smtp.get("use_ssl", base["use_ssl"]), False),
        "verify_tls": as_bool(smtp.get("verify_tls", base.get("verify_tls", True)), True),
        "unsubscribe_email": str(smtp.get("unsubscribe_email", base["unsubscribe_email"])).strip(),
        "unsubscribe_url": str(smtp.get("unsubscribe_url", base["unsubscribe_url"])).strip(),
    }
    if not normalized["username"] and normalized["from_email"]:
        normalized["username"] = normalized["from_email"]
    if not normalized["reply_to"] and normalized["from_email"]:
        normalized["reply_to"] = normalized["from_email"]
    return normalized


def normalize_sent_copy_payload(payload: dict[str, Any], smtp_profile: dict[str, Any]) -> dict[str, Any]:
    sent_copy = payload.get("sent_copy") if isinstance(payload, dict) else {}
    sent_copy = sent_copy if isinstance(sent_copy, dict) else {}
    base = merged_profile()
    normalized = {
        "enabled": as_bool(sent_copy.get("enabled", base.get("save_sent_copy", False)), False),
        "host": str(sent_copy.get("host", base.get("imap_host", ""))).strip(),
        "port": parse_int(sent_copy.get("port", base.get("imap_port", 993)), int(base.get("imap_port", 993))),
        "username": str(sent_copy.get("username", base.get("imap_username", smtp_profile.get("username", "")))).strip(),
        "password": str(sent_copy.get("password", "")).strip(),
        "sent_folder": str(sent_copy.get("sent_folder", base.get("imap_sent_folder", "Sent"))).strip() or "Sent",
        "use_ssl": as_bool(sent_copy.get("use_ssl", base.get("imap_use_ssl", True)), True),
        "verify_tls": as_bool(sent_copy.get("verify_tls", base.get("imap_verify_tls", True)), True),
    }
    if not normalized["username"]:
        normalized["username"] = str(smtp_profile.get("username") or "")
    if not normalized["password"]:
        normalized["password"] = str(smtp_profile.get("password") or "")
    return normalized


def profile_without_password(profile: dict[str, Any]) -> dict[str, Any]:
    result = profile.copy()
    result.pop("password", None)
    return result


def resolve_password(profile: dict[str, Any]) -> dict[str, Any]:
    resolved = profile.copy()
    if resolved.get("password"):
        return resolved
    saved_password = keychain_get_password(str(resolved.get("username") or resolved.get("from_email") or ""))
    if saved_password:
        resolved["password"] = saved_password
    return resolved


def sanitize_campaign_name(raw_value: str) -> str:
    cleaned = "".join(character.lower() if character.isalnum() else "-" for character in raw_value.strip())
    cleaned = "-".join(part for part in cleaned.split("-") if part)
    return cleaned[:64]


def saved_item_key(raw_value: str, label: str) -> str:
    slug = sanitize_campaign_name(raw_value)
    if not slug:
        raise SystemExit(f"Enter a {label} name with letters or numbers.")
    return slug


def normalize_body_mode(raw_value: Any) -> str:
    mode = str(raw_value or "html").strip().lower()
    if mode not in {"html", "text", "both"}:
        raise SystemExit("Email format must be html, text, or both.")
    return mode


def template_path(template_id: str) -> Path:
    return TEMPLATE_DIR / f"{template_id}.json"


def saved_campaign_path(campaign_id: str) -> Path:
    return CAMPAIGN_DIR / f"{campaign_id}.json"


def decode_attachment_items(raw_value: Any) -> tuple[list[AttachmentSpec], list[dict[str, Any]]]:
    if raw_value in (None, ""):
        return [], []
    if not isinstance(raw_value, list):
        raise SystemExit("Attachments payload must be a list.")

    specs: list[AttachmentSpec] = []
    records: list[dict[str, Any]] = []
    total_bytes = 0

    for index, item in enumerate(raw_value, start=1):
        if not isinstance(item, dict):
            raise SystemExit(f"Attachment #{index} is invalid.")
        filename = Path(str(item.get("filename") or "")).name.strip()
        if not filename:
            raise SystemExit(f"Attachment #{index} is missing a file name.")
        encoded = str(item.get("data_base64") or "").strip()
        if not encoded:
            raise SystemExit(f"Attachment '{filename}' has no file data.")
        try:
            data = base64.b64decode(encoded, validate=True)
        except ValueError as exc:
            raise SystemExit(f"Attachment '{filename}' could not be decoded.") from exc
        total_bytes += len(data)
        if total_bytes > MAX_ATTACHMENT_BYTES:
            raise SystemExit("Attachments are too large. Keep the total attachment size under 20 MB.")
        content_type = str(item.get("content_type") or mimetypes.guess_type(filename)[0] or "application/octet-stream").strip()
        specs.append(AttachmentSpec(filename=filename, content_type=content_type, data=data))
        records.append(
            {
                "filename": filename,
                "content_type": content_type,
                "data_base64": encoded,
                "size": len(data),
            }
        )

    return specs, records


def normalize_message_payload(values: dict[str, Any], require_subject: bool = True) -> dict[str, Any]:
    subject = str(values.get("subject") or "").strip()
    body_mode = normalize_body_mode(values.get("body_mode"))
    html_content = str(values.get("html_content") or "")
    text_content = str(values.get("text_content") or "")
    attachment_specs, attachment_records = decode_attachment_items(values.get("attachments"))

    if require_subject and not subject:
        raise SystemExit("Enter an email subject.")

    if body_mode == "html":
        if not html_content.strip():
            raise SystemExit("Paste or load the HTML email first.")
        text_content = ""
    elif body_mode == "text":
        if not text_content.strip():
            raise SystemExit("Paste the plain-text email first.")
        html_content = ""
    else:
        if not text_content.strip():
            raise SystemExit("Paste the plain-text email first.")
        if not html_content.strip():
            raise SystemExit("Paste or load the HTML email first.")

    return {
        "subject": subject,
        "body_mode": body_mode,
        "html_content": html_content,
        "text_content": text_content,
        "attachment_specs": attachment_specs,
        "attachment_records": attachment_records,
    }


def test_email_render_contact(payload: dict[str, Any]) -> dict[str, str]:
    campaign = payload.get("campaign") if isinstance(payload, dict) else {}
    campaign = campaign if isinstance(campaign, dict) else {}
    recipient_list = str(campaign.get("recipient_list") or "")
    if recipient_list.strip():
        contact = parse_email_list(recipient_list)[0].copy()
        contact["email"] = normalize_email(contact.get("email")) or TEST_EMAIL_RECIPIENT
        return contact
    return {
        "email": TEST_EMAIL_RECIPIENT,
        "__row_number": "1",
    }


def campaign_key(campaign_name: str, recipient_source: str) -> str:
    if campaign_name.strip():
        slug = sanitize_campaign_name(campaign_name)
        if not slug:
            raise SystemExit("Campaign name contains no usable characters.")
        return slug
    if not recipient_source.strip():
        raise SystemExit("Enter a campaign name or paste the recipient list before continuing.")
    digest = hashlib.sha256(recipient_source.encode("utf-8")).hexdigest()[:16]
    return f"list-{digest}"


def campaign_source_id(campaign_key_value: str, recipient_source: str) -> str:
    digest = hashlib.sha256(recipient_source.encode("utf-8")).hexdigest()[:16]
    return f"web:{campaign_key_value}:{digest}"


def campaign_key_from_payload(payload: dict[str, Any]) -> str:
    campaign = payload.get("campaign") if isinstance(payload, dict) else {}
    campaign = campaign if isinstance(campaign, dict) else {}
    return campaign_key(str(campaign.get("campaign_name") or ""), str(campaign.get("recipient_list") or ""))


def build_campaign_context(payload: dict[str, Any]) -> dict[str, Any]:
    campaign = payload.get("campaign") if isinstance(payload, dict) else {}
    campaign = campaign if isinstance(campaign, dict) else {}

    recipient_list = str(campaign.get("recipient_list") or "")
    batch_size = parse_int(campaign.get("batch_size"), int(DEFAULT_PROFILE["batch_size"]))
    max_attempts = parse_int(campaign.get("max_attempts_per_row"), int(DEFAULT_PROFILE["max_attempts_per_row"]))
    retry_exhausted = as_bool(campaign.get("retry_exhausted"), False)
    pause_seconds = parse_float(campaign.get("pause_seconds"), float(DEFAULT_PROFILE["pause_seconds"]))
    campaign_name = str(campaign.get("campaign_name") or "").strip()
    message_fields = normalize_message_payload(campaign)

    if not recipient_list.strip():
        raise SystemExit("Paste at least one recipient email.")
    if batch_size <= 0:
        raise SystemExit("Batch size must be greater than 0.")
    if max_attempts <= 0:
        raise SystemExit("Max attempts must be greater than 0.")
    if pause_seconds < 0:
        raise SystemExit("Pause between emails cannot be negative.")

    templates = build_templates(
        subject=message_fields["subject"],
        body_text=message_fields["text_content"],
        body_html=message_fields["html_content"],
        generate_missing_alternative=False,
    )
    contacts = parse_email_list(recipient_list)
    key = campaign_key(campaign_name, recipient_list)
    state_file = STATE_DIR / f"{key}.json"
    state = load_state(state_file, campaign_source_id(key, recipient_list))
    analysis = analyze_contacts(
        contacts=contacts,
        state=state,
        email_column="email",
        required_fields=templates.required_fields,
        max_attempts_per_row=max_attempts,
        retry_exhausted=retry_exhausted,
    )

    return {
        "campaign_name": campaign_name,
        "campaign_key": key,
        "state_file": state_file,
        "state": state,
        "templates": templates,
        "contacts": contacts,
        "analysis": analysis,
        "batch_size": batch_size,
        "pause_seconds": pause_seconds,
        "max_attempts_per_row": max_attempts,
        "retry_exhausted": retry_exhausted,
        "body_mode": message_fields["body_mode"],
        "attachment_specs": message_fields["attachment_specs"],
        "attachment_records": message_fields["attachment_records"],
    }


def preview_payload_from_context(context: dict[str, Any]) -> dict[str, Any]:
    analysis = context["analysis"]
    templates = context["templates"]
    batch_size = context["batch_size"]
    next_batch = analysis.eligible[:batch_size]
    smtp_utf8_rows = [contact for contact in next_batch if contact.get("__requires_smtputf8") == "true"]

    sample_contact = next_batch[0] if next_batch else None
    sample = None
    if sample_contact:
        rendered_text = render_template(templates.body_text, sample_contact)
        rendered_html = render_template(templates.body_html, sample_contact)
        preview_html = rendered_html or wrap_text_as_html(rendered_text)
        sample = {
            "subject": render_template(templates.subject, sample_contact).replace("\n", " ").strip(),
            "text": rendered_text,
            "html": sanitize_preview_html(preview_html),
        }

    return {
        "campaignKey": context["campaign_key"],
        "stateFile": str(context["state_file"]),
        "batchSize": batch_size,
        "bodyMode": context["body_mode"],
        "attachments": [
            {
                "filename": item["filename"],
                "size": item["size"],
                "contentType": item["content_type"],
            }
            for item in context["attachment_records"]
        ],
        "summary": {
            "totalContacts": len(context["contacts"]),
            "readyNow": len(next_batch),
            "eligibleRemaining": len(analysis.eligible),
            "alreadySent": len(analysis.already_sent),
            "invalidEmails": len(analysis.invalid_email),
            "duplicateEmails": len(analysis.duplicate_email),
            "missingFields": len(analysis.missing_fields),
            "pausedFailures": len(analysis.exhausted_failures),
        },
        "nextRecipients": [
            {
                "row": contact["__row_number"],
                "email": str(contact.get("email") or "").strip(),
            }
            for contact in next_batch[:20]
        ],
        "missingFieldRows": [
            {
                "row": contact["__row_number"],
                "email": str(contact.get("email") or "").strip(),
                "missing": contact.get("__missing_fields", ""),
            }
            for contact in analysis.missing_fields[:10]
        ],
        "warnings": [
            {
                "row": contact["__row_number"],
                "message": smtp_utf8_requirement_message(str(contact.get("email") or "").strip()),
            }
            for contact in smtp_utf8_rows
        ],
        "sample": sample,
    }


def template_record_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    template = payload.get("template") if isinstance(payload, dict) else {}
    template = template if isinstance(template, dict) else {}
    template_name = str(template.get("template_name") or template.get("name") or "").strip()
    template_id = saved_item_key(template_name, "template")
    message_fields = normalize_message_payload(template)
    return {
        "version": 1,
        "id": template_id,
        "name": template_name,
        "saved_at": utc_now(),
        "subject": message_fields["subject"],
        "body_mode": message_fields["body_mode"],
        "html_content": message_fields["html_content"],
        "text_content": message_fields["text_content"],
        "attachments": message_fields["attachment_records"],
    }


def campaign_record_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    context = build_campaign_context(payload)
    campaign = payload.get("campaign") if isinstance(payload, dict) else {}
    campaign = campaign if isinstance(campaign, dict) else {}
    campaign_name = context["campaign_name"]
    if not campaign_name:
        raise SystemExit("Enter a campaign name before saving it for resume.")
    campaign_id = saved_item_key(campaign_name, "campaign")
    return {
        "version": 1,
        "id": campaign_id,
        "name": campaign_name,
        "saved_at": utc_now(),
        "updated_at": utc_now(),
        "campaign_name": campaign_name,
        "batch_size": context["batch_size"],
        "pause_seconds": context["pause_seconds"],
        "max_attempts_per_row": context["max_attempts_per_row"],
        "retry_exhausted": context["retry_exhausted"],
        "subject": context["templates"].subject,
        "body_mode": context["body_mode"],
        "html_content": context["templates"].body_html if context["body_mode"] != "text" else "",
        "text_content": context["templates"].body_text if context["body_mode"] != "html" else "",
        "recipient_list": str(campaign.get("recipient_list") or ""),
        "recipient_count": len(context["contacts"]),
        "attachments": context["attachment_records"],
    }


def current_password_status(profile: dict[str, Any]) -> bool:
    account = str(profile.get("username") or profile.get("from_email") or "")
    return keychain_has_password(account)


class SessionStore:
    _PURGE_INTERVAL = 60.0

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sessions: dict[str, dict[str, Any]] = {}
        self._last_purge: float = 0.0

    def get(self, session_id: str | None) -> dict[str, Any] | None:
        if not session_id:
            return None
        with self._lock:
            self._purge_expired()
            session = self._sessions.get(session_id)
            if session is None:
                return None
            session["last_seen"] = time.time()
            return session

    def get_or_create(self, session_id: str | None) -> tuple[dict[str, Any], bool]:
        with self._lock:
            self._purge_expired()
            now = time.time()
            if session_id and session_id in self._sessions:
                session = self._sessions[session_id]
                session["last_seen"] = now
                return session, False

            new_id = secrets.token_urlsafe(32)
            session = {
                "id": new_id,
                "csrf_token": secrets.token_urlsafe(24),
                "created_at": now,
                "last_seen": now,
            }
            self._sessions[new_id] = session
            return session, True

    def _purge_expired(self) -> None:
        now = time.time()
        if now - self._last_purge < self._PURGE_INTERVAL:
            return
        self._last_purge = now
        cutoff = now - SESSION_TTL_SECONDS
        expired = [session_id for session_id, data in self._sessions.items() if data["last_seen"] < cutoff]
        for session_id in expired:
            self._sessions.pop(session_id, None)


SESSION_STORE = SessionStore()


class SendJobStore:
    _PURGE_INTERVAL = 30.0

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, dict[str, Any]] = {}
        self._active_by_session: dict[str, str] = {}
        self._active_by_campaign: dict[str, str] = {}
        self._last_purge: float = 0.0

    def start(self, session_id: str, campaign_key_value: str, state_file: Path, payload: dict[str, Any]) -> dict[str, Any]:
        campaign_lock: CampaignLock | None = None
        with self._lock:
            self._purge_finished_locked()
            active_job_id = self._active_by_session.get(session_id)
            active_job = self._jobs.get(active_job_id or "")
            if active_job and active_job["status"] in {"queued", "running"}:
                raise SystemExit("A batch send is already running in this browser session.")

            active_campaign_job_id = self._active_by_campaign.get(campaign_key_value)
            active_campaign_job = self._jobs.get(active_campaign_job_id or "")
            if active_campaign_job and active_campaign_job["status"] in {"queued", "running"}:
                raise SystemExit("This campaign is already sending in another browser session.")

            try:
                campaign_lock = acquire_campaign_lock_nowait(state_file)
            except CampaignLockError as exc:
                raise SystemExit(str(exc)) from exc

            job_id = secrets.token_urlsafe(12)
            job = {
                "id": job_id,
                "session_id": session_id,
                "campaign_key": campaign_key_value,
                "status": "queued",
                "created_at": utc_now(),
                "updated_at": utc_now(),
                "created_at_epoch": time.time(),
                "updated_at_epoch": time.time(),
                "finished_at_epoch": None,
                "lock": campaign_lock,
                "message": "Preparing batch send...",
                "batch_total": 0,
                "processed": 0,
                "sent_count": 0,
                "failed_count": 0,
                "current_recipient": "",
                "sent": [],
                "failed": [],
                "warnings": [],
                "preview": None,
                "error": "",
            }
            self._jobs[job_id] = job
            self._active_by_session[session_id] = job_id
            self._active_by_campaign[campaign_key_value] = job_id

        thread = threading.Thread(target=run_send_job, args=(job_id, payload), daemon=True)
        try:
            thread.start()
        except Exception:
            with self._lock:
                self._jobs.pop(job_id, None)
                if self._active_by_session.get(session_id) == job_id:
                    self._active_by_session.pop(session_id, None)
                if self._active_by_campaign.get(campaign_key_value) == job_id:
                    self._active_by_campaign.pop(campaign_key_value, None)
            release_campaign_lock(campaign_lock)
            raise
        ACTIVITY_LOG_STORE.append(
            source="send",
            session_id=session_id,
            campaign_key=campaign_key_value,
            job_id=job_id,
            tone="info",
            title="Send queued",
            message=f"Queued batch send for campaign '{campaign_key_value}'.",
        )
        return self.public(job_id, session_id)

    def public(self, job_id: str, session_id: str) -> dict[str, Any]:
        with self._lock:
            self._purge_finished_locked()
            job = self._jobs.get(job_id)
            if not job or job["session_id"] != session_id:
                raise SystemExit("Send job was not found for this browser session.")
            return {
                "id": job["id"],
                "status": job["status"],
                "message": job["message"],
                "batchTotal": job["batch_total"],
                "processed": job["processed"],
                "sentCount": job["sent_count"],
                "failedCount": job["failed_count"],
                "currentRecipient": job["current_recipient"],
                "sent": list(job["sent"]),
                "failed": list(job["failed"]),
                "warnings": list(job["warnings"]),
                "preview": job["preview"],
                "error": job["error"],
            }

    def update(self, job_id: str, **changes: Any) -> None:
        with self._lock:
            self._purge_finished_locked()
            job = self._jobs.get(job_id)
            if not job:
                return
            job.update(changes)
            job["updated_at"] = utc_now()
            job["updated_at_epoch"] = time.time()

    def append_sent(self, job_id: str, item: dict[str, Any]) -> None:
        log_payload: dict[str, Any] | None = None
        with self._lock:
            self._purge_finished_locked()
            job = self._jobs.get(job_id)
            if not job:
                return
            job["sent"].append(item)
            job["sent_count"] = len(job["sent"])
            job["updated_at"] = utc_now()
            job["updated_at_epoch"] = time.time()
            log_payload = {
                "session_id": job["session_id"],
                "campaign_key": job["campaign_key"],
                "job_id": job["id"],
                "recipient": str(item.get("email") or "").strip(),
                "row_number": item.get("row"),
            }
        if log_payload:
            ACTIVITY_LOG_STORE.append(
                source="send",
                tone="success",
                title="Sent",
                message=f"Delivered email to {log_payload['recipient']} (row {log_payload['row_number']}).",
                **log_payload,
            )

    def append_failed(self, job_id: str, item: dict[str, Any]) -> None:
        log_payload: dict[str, Any] | None = None
        with self._lock:
            self._purge_finished_locked()
            job = self._jobs.get(job_id)
            if not job:
                return
            job["failed"].append(item)
            job["failed_count"] = len(job["failed"])
            job["updated_at"] = utc_now()
            job["updated_at_epoch"] = time.time()
            log_payload = {
                "session_id": job["session_id"],
                "campaign_key": job["campaign_key"],
                "job_id": job["id"],
                "recipient": str(item.get("email") or "").strip(),
                "row_number": item.get("row"),
                "error": str(item.get("error") or "").strip(),
            }
        if log_payload:
            ACTIVITY_LOG_STORE.append(
                source="send",
                tone="error",
                title="Failed",
                message=f"Failed to send to {log_payload['recipient']} (row {log_payload['row_number']}): {log_payload['error']}",
                session_id=log_payload["session_id"],
                campaign_key=log_payload["campaign_key"],
                job_id=log_payload["job_id"],
                recipient=log_payload["recipient"],
                row_number=log_payload["row_number"],
            )

    def append_warning(self, job_id: str, warning: str) -> None:
        log_payload: dict[str, Any] | None = None
        with self._lock:
            self._purge_finished_locked()
            job = self._jobs.get(job_id)
            if not job:
                return
            job["warnings"].append(warning)
            job["updated_at"] = utc_now()
            job["updated_at_epoch"] = time.time()
            log_payload = {
                "session_id": job["session_id"],
                "campaign_key": job["campaign_key"],
                "job_id": job["id"],
                "message": str(warning or "").strip(),
            }
        if log_payload:
            ACTIVITY_LOG_STORE.append(
                source="send",
                tone="warning",
                title="Send warning",
                message=log_payload["message"],
                session_id=log_payload["session_id"],
                campaign_key=log_payload["campaign_key"],
                job_id=log_payload["job_id"],
            )

    def finish(self, job_id: str, *, status: str, message: str, preview: dict[str, Any] | None = None, error: str = "") -> None:
        lock_to_release: CampaignLock | None = None
        log_payload: dict[str, Any] | None = None
        with self._lock:
            self._purge_finished_locked()
            job = self._jobs.get(job_id)
            if not job:
                return
            job["status"] = status
            job["message"] = message
            job["preview"] = preview
            job["error"] = error
            job["current_recipient"] = ""
            job["updated_at"] = utc_now()
            job["updated_at_epoch"] = time.time()
            job["finished_at_epoch"] = job["updated_at_epoch"]
            active_job_id = self._active_by_session.get(job["session_id"])
            if active_job_id == job_id:
                self._active_by_session.pop(job["session_id"], None)
            active_campaign_job_id = self._active_by_campaign.get(job["campaign_key"])
            if active_campaign_job_id == job_id:
                self._active_by_campaign.pop(job["campaign_key"], None)
            lock_to_release = job.pop("lock", None)
            log_payload = {
                "session_id": job["session_id"],
                "campaign_key": job["campaign_key"],
                "job_id": job["id"],
            }

        release_campaign_lock(lock_to_release)
        if log_payload:
            ACTIVITY_LOG_STORE.append(
                source="send",
                tone="error" if status == "failed" else "success",
                title="Send failed" if status == "failed" else "Send finished",
                message=error or message,
                session_id=log_payload["session_id"],
                campaign_key=log_payload["campaign_key"],
                job_id=log_payload["job_id"],
            )

    def _purge_finished_locked(self) -> None:
        now = time.time()
        if now - self._last_purge < self._PURGE_INTERVAL:
            return
        self._last_purge = now
        cutoff = now - FINISHED_JOB_TTL_SECONDS
        expired_job_ids = [
            job_id
            for job_id, job in self._jobs.items()
            if job.get("status") in {"completed", "failed"}
            and float(job.get("finished_at_epoch") or job.get("updated_at_epoch") or 0.0) < cutoff
        ]
        for job_id in expired_job_ids:
            self._jobs.pop(job_id, None)


SEND_JOB_STORE = SendJobStore()


def run_send_job(job_id: str, payload: dict[str, Any]) -> None:
    imap_client = None

    try:
        SEND_JOB_STORE.update(job_id, status="running", message="Validating SMTP and campaign settings...")
        profile = resolve_password(normalize_smtp_payload(payload))
        config = smtp_config_from_values(profile, timeout=30.0, require_password=True)
        sent_copy = normalize_sent_copy_payload(payload, profile)
        context = build_campaign_context(payload)
        batch = context["analysis"].eligible[: context["batch_size"]]
        SEND_JOB_STORE.update(job_id, batch_total=len(batch), message=f"Queued {len(batch)} email(s) for delivery.")

        if not batch:
            SEND_JOB_STORE.finish(
                job_id,
                status="completed",
                message="No eligible contacts remain for this campaign.",
                preview=preview_payload_from_context(context),
            )
            return

        state = context["state"]
        state_file = context["state_file"]
        templates = context["templates"]
        attachment_specs = context["attachment_specs"]
        imap_config = None

        if sent_copy["enabled"]:
            imap_config = imap_config_from_values(
                sent_copy,
                timeout=30.0,
                fallback_username=config.username,
                fallback_password=config.password,
            )
            SEND_JOB_STORE.update(job_id, message="Connecting to IMAP sent-folder append...")
            try:
                imap_client = open_imap_connection(imap_config)
                ensure_imap_sent_folder(imap_client, imap_config)
            except Exception as exc:
                if imap_client is not None:
                    try:
                        imap_client.logout()
                    except Exception:
                        pass
                raise SystemExit(describe_imap_exception(exc, imap_config)) from exc

        try:
            SEND_JOB_STORE.update(job_id, message="Connecting to SMTP...")
            with open_smtp_connection(config) as smtp_client:
                supports_smtputf8 = smtp_supports_smtputf8(smtp_client)
                result = send_batch(
                    smtp_client=smtp_client,
                    supports_smtputf8=supports_smtputf8,
                    config=config,
                    batch=batch,
                    state=state,
                    state_file=state_file,
                    email_column="email",
                    templates=templates,
                    pause_seconds=context["pause_seconds"],
                    attachments=attachment_specs,
                    imap_client=imap_client,
                    imap_config=imap_config,
                    on_contact_start=lambda index, total, recipient: SEND_JOB_STORE.update(
                        job_id,
                        current_recipient=recipient,
                        message=f"Sending {index} of {total} to {recipient}",
                    ),
                    on_contact_complete=lambda index, total: SEND_JOB_STORE.update(
                        job_id,
                        processed=index,
                        message=f"Processed {index} of {total} email(s).",
                    ),
                    on_sent=lambda item: SEND_JOB_STORE.append_sent(job_id, item),
                    on_failed=lambda item: SEND_JOB_STORE.append_failed(job_id, item),
                    on_warning=lambda warning: SEND_JOB_STORE.append_warning(job_id, warning),
                )
        except Exception as exc:
            raise SystemExit(describe_smtp_exception(exc, config)) from exc
        finally:
            if imap_client is not None:
                try:
                    imap_client.logout()
                except Exception:
                    pass

        refreshed_analysis = analyze_contacts(
            contacts=context["contacts"],
            state=state,
            email_column="email",
            required_fields=context["templates"].required_fields,
            max_attempts_per_row=context["max_attempts_per_row"],
            retry_exhausted=context["retry_exhausted"],
        )
        context["analysis"] = refreshed_analysis
        sent = result["sent"]
        failed = result["failed"]
        warnings = result["warnings"]
        summary_message = f"Batch finished. Sent {len(sent)} email(s), failed {len(failed)}."
        if sent_copy["enabled"]:
            saved_count = sum(1 for item in sent if item.get("savedToSent") is True)
            summary_message += f" Saved {saved_count} sent copy/copies to IMAP."
            if warnings:
                summary_message += f" IMAP warnings: {len(warnings)}."

        SEND_JOB_STORE.finish(
            job_id,
            status="completed",
            message=summary_message,
            preview=preview_payload_from_context(context),
        )
    except SystemExit as exc:
        SEND_JOB_STORE.finish(job_id, status="failed", message=str(exc), error=str(exc))
    except Exception as exc:  # pragma: no cover - defensive worker boundary.
        SEND_JOB_STORE.finish(job_id, status="failed", message=str(exc), error=str(exc))


class MailerAppHandler(BaseHTTPRequestHandler):
    server_version = "MailInTheShell/1.0"

    def do_GET(self) -> None:
        self._session = None
        self._queued_cookie = None
        path = urlparse(self.path).path
        if path.startswith("/api/") and not self._require_remote_access_token():
            return
        if path == BOOTSTRAP_PATH:
            self._session, is_new = self._get_or_create_session()
            if is_new:
                self._queued_cookie = self._session["id"]
            profile = merged_profile()
            self._json_response(
                HTTPStatus.OK,
                {
                    "csrfToken": self._session["csrf_token"],
                    "profile": profile_without_password(profile),
                    "keychainAvailable": keychain_available(),
                    "hasSavedPassword": current_password_status(profile),
                    "remoteAccessEnabled": bool(getattr(self.server, "remote_access_token", "")),
                    "settingsPath": str(SETTINGS_FILE),
                    "logStorePath": str(ACTIVITY_LOG_STORE.db_path),
                    "logEntryCount": ACTIVITY_LOG_STORE.count(),
                    "libraries": library_payload(),
                },
            )
            return
        if path == LOG_EXPORT_PATH:
            self._session = self._require_session()
            if self._session is None:
                return
            self._handle_log_export()
            return
        if path == "/":
            self._serve_static(WEB_DIR / "index.html", "text/html; charset=utf-8")
            return
        if path == "/app.css":
            self._serve_static(WEB_DIR / "app.css", "text/css; charset=utf-8")
            return
        if path == "/app.js":
            self._serve_static(WEB_DIR / "app.js", "application/javascript; charset=utf-8")
            return
        if path == "/top-right.gif":
            self._serve_static(TOP_RIGHT_GIF, "image/gif")
            return
        if path == "/title-mark.png":
            self._serve_static(TITLE_IMAGE, "image/png")
            return

        self._json_response(HTTPStatus.NOT_FOUND, {"error": "Not found."})

    def do_POST(self) -> None:
        self._session = None
        self._queued_cookie = None
        if not self._require_remote_access_token():
            return
        self._session = self._require_session()
        if self._session is None:
            return
        if not self._valid_csrf():
            self._json_response(HTTPStatus.FORBIDDEN, {"error": "Invalid CSRF token."})
            return

        try:
            payload = self._read_json_body()
            path = urlparse(self.path).path
            if path == "/api/save-settings":
                self._handle_save_settings(payload)
                return
            if path == "/api/clear-saved-password":
                self._handle_clear_saved_password(payload)
                return
            if path == "/api/test-connection":
                self._handle_test_connection(payload)
                return
            if path == "/api/send-test-email":
                self._handle_send_test_email(payload)
                return
            if path == "/api/save-template":
                self._handle_save_template(payload)
                return
            if path == "/api/load-template":
                self._handle_load_template(payload)
                return
            if path == "/api/save-campaign":
                self._handle_save_campaign(payload)
                return
            if path == "/api/load-campaign":
                self._handle_load_campaign(payload)
                return
            if path == "/api/preview":
                self._handle_preview(payload)
                return
            if path == "/api/send":
                self._handle_send(payload)
                return
            if path == "/api/send-status":
                self._handle_send_status(payload)
                return
            if path == "/api/reset-state":
                self._handle_reset_state(payload)
                return
            if path == "/api/log-event":
                self._handle_log_event(payload)
                return
            self._json_response(HTTPStatus.NOT_FOUND, {"error": "Not found."})
        except SystemExit as exc:
            self._json_response(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except subprocess.CalledProcessError as exc:
            message = exc.stderr.strip() if exc.stderr else str(exc)
            self._json_response(HTTPStatus.BAD_REQUEST, {"error": message})
        except Exception as exc:  # pragma: no cover - defensive API boundary.
            self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})

    def log_message(self, format: str, *args: object) -> None:
        return

    def _session_id(self) -> str | None:
        cookie_header = self.headers.get("Cookie", "")
        jar = SimpleCookie()
        if cookie_header:
            jar.load(cookie_header)
        return jar.get(SESSION_COOKIE_NAME).value if jar.get(SESSION_COOKIE_NAME) else None

    def _get_or_create_session(self) -> tuple[dict[str, Any], bool]:
        return SESSION_STORE.get_or_create(self._session_id())

    def _get_existing_session(self) -> dict[str, Any] | None:
        return SESSION_STORE.get(self._session_id())

    def _require_session(self) -> dict[str, Any] | None:
        session = self._get_existing_session()
        if session is None:
            self._json_response(HTTPStatus.FORBIDDEN, {"error": "Session expired. Reload the page."})
            return None
        return session

    def _require_remote_access_token(self) -> bool:
        expected_token = str(getattr(self.server, "remote_access_token", "") or "").strip()
        if not expected_token:
            return True
        presented_token = extract_request_access_token(self.path, self.headers.get("X-Remote-Access-Token"))
        if presented_token and secrets.compare_digest(expected_token, presented_token):
            return True
        self._json_response(HTTPStatus.UNAUTHORIZED, {"error": "Remote access token required."})
        return False

    def _valid_csrf(self) -> bool:
        header_token = self.headers.get("X-CSRF-Token", "")
        return bool(header_token and header_token == self._session["csrf_token"])

    def _common_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; connect-src 'self'; frame-src 'self'; object-src 'none'; base-uri 'none';",
        )
        if getattr(self, "_queued_cookie", None):
            self.send_header(
                "Set-Cookie",
                f"{SESSION_COOKIE_NAME}={self._queued_cookie}; HttpOnly; SameSite=Strict; Path=/",
            )

    def _serve_static(self, path: Path, content_type: str | None = None) -> None:
        if not path.exists() or not path.is_file():
            self._json_response(HTTPStatus.NOT_FOUND, {"error": "Not found."})
            return
        content = path.read_bytes()
        mime_type = content_type or mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self._common_headers()
        self.send_header("Content-Type", mime_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _json_response(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self._common_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _csv_response(self, filename: str, content: bytes) -> None:
        self.send_response(HTTPStatus.OK)
        self._common_headers()
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _read_json_body(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise SystemExit("Missing request body.")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise SystemExit("Invalid Content-Length header.") from exc
        if length <= 0:
            raise SystemExit("Empty request body.")
        if length > MAX_REQUEST_BYTES:
            raise SystemExit("Request body is too large.")
        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise SystemExit("Request body must be valid JSON.") from exc
        if not isinstance(payload, dict):
            raise SystemExit("JSON payload must be an object.")
        return payload

    def _handle_save_settings(self, payload: dict[str, Any]) -> None:
        profile = normalize_smtp_payload(payload)
        sent_copy = normalize_sent_copy_payload(payload, profile)
        save_password_to_keychain = as_bool(payload.get("savePasswordToKeychain"), False)
        clear_saved_password = as_bool(payload.get("clearSavedPassword"), False)

        save_profile(
            {
                **profile_without_password(profile),
                "save_sent_copy": sent_copy["enabled"],
                "imap_host": sent_copy["host"],
                "imap_port": sent_copy["port"],
                "imap_username": sent_copy["username"],
                "imap_sent_folder": sent_copy["sent_folder"],
                "imap_use_ssl": sent_copy["use_ssl"],
                "imap_verify_tls": sent_copy["verify_tls"],
            }
        )

        if save_password_to_keychain:
            if not profile["password"]:
                raise SystemExit("Enter the SMTP password before saving it to Keychain.")
            keychain_set_password(profile["username"], profile["password"])
        elif clear_saved_password:
            keychain_delete_password(profile["username"])

        self._json_response(
            HTTPStatus.OK,
            {
                "ok": True,
                "message": "SMTP defaults saved locally.",
                "profile": {
                    **profile_without_password(profile),
                    "save_sent_copy": sent_copy["enabled"],
                    "imap_host": sent_copy["host"],
                    "imap_port": sent_copy["port"],
                    "imap_username": sent_copy["username"],
                    "imap_sent_folder": sent_copy["sent_folder"],
                    "imap_use_ssl": sent_copy["use_ssl"],
                    "imap_verify_tls": sent_copy["verify_tls"],
                },
                "hasSavedPassword": current_password_status(profile),
                "settingsPath": str(SETTINGS_FILE),
            },
        )

    def _handle_clear_saved_password(self, payload: dict[str, Any]) -> None:
        profile = normalize_smtp_payload(payload)
        account = profile["username"] or profile["from_email"]
        removed = keychain_delete_password(account)
        self._json_response(
            HTTPStatus.OK,
            {
                "ok": True,
                "message": "Saved password removed from Keychain." if removed else "No saved password was found.",
                "hasSavedPassword": False,
            },
        )

    def _handle_test_connection(self, payload: dict[str, Any]) -> None:
        profile = resolve_password(normalize_smtp_payload(payload))
        config = smtp_config_from_values(profile, timeout=30.0, require_password=True)
        test_smtp_connection(config)
        sent_copy = normalize_sent_copy_payload(payload, profile)
        if sent_copy["enabled"]:
            imap_config = imap_config_from_values(
                sent_copy,
                timeout=30.0,
                fallback_username=config.username,
                fallback_password=config.password,
            )
            test_imap_connection(imap_config)
            message = (
                f"SMTP and IMAP login succeeded. Sent copies will be saved to "
                f"'{imap_config.sent_folder}' on {imap_config.host}:{imap_config.port}."
            )
        else:
            message = f"SMTP login succeeded for {config.username} on {config.host}:{config.port}."
        self._json_response(
            HTTPStatus.OK,
            {
                "ok": True,
                "message": message,
                "hasSavedPassword": current_password_status(profile),
            },
        )

    def _handle_send_test_email(self, payload: dict[str, Any]) -> None:
        profile = resolve_password(normalize_smtp_payload(payload))
        config = smtp_config_from_values(profile, timeout=30.0, require_password=True)
        sent_copy = normalize_sent_copy_payload(payload, profile)

        campaign = payload.get("campaign") if isinstance(payload, dict) else {}
        campaign = campaign if isinstance(campaign, dict) else {}
        message_fields = normalize_message_payload(campaign)
        templates = build_templates(
            subject=message_fields["subject"],
            body_text=message_fields["text_content"],
            body_html=message_fields["html_content"],
            generate_missing_alternative=False,
        )
        render_contact = test_email_render_contact(payload)
        missing_fields = [field for field in templates.required_fields if not str(render_contact.get(field, "")).strip()]
        if missing_fields:
            raise SystemExit("Test email is missing template fields: " + ", ".join(missing_fields))

        imap_client = None
        imap_config = None
        try:
            if sent_copy["enabled"]:
                imap_config = imap_config_from_values(
                    sent_copy,
                    timeout=30.0,
                    fallback_username=config.username,
                    fallback_password=config.password,
                )
                imap_client = open_imap_connection(imap_config)
                ensure_imap_sent_folder(imap_client, imap_config)

            with open_smtp_connection(config) as smtp_client:
                message = build_message(
                    config,
                    render_contact,
                    "email",
                    templates,
                    attachments=message_fields["attachment_specs"],
                    recipient_override=TEST_EMAIL_RECIPIENT,
                )
                smtp_client.send_message(message)

                sent_copy_saved = None
                sent_copy_error = None
                if imap_client is not None and imap_config is not None:
                    try:
                        append_sent_copy(imap_client, imap_config, message.as_bytes())
                        sent_copy_saved = True
                    except Exception as exc:
                        sent_copy_saved = False
                        sent_copy_error = describe_imap_exception(exc, imap_config)

            rendered_subject = render_template(templates.subject, render_contact).replace("\n", " ").strip()
            response_message = f"Test email sent to {TEST_EMAIL_RECIPIENT}."
            if rendered_subject:
                response_message += f" Subject: {rendered_subject}"
            if sent_copy_saved is True:
                response_message += " A sent copy was appended to IMAP."
            elif sent_copy_error:
                response_message += f" Sent copy warning: {sent_copy_error}"

            ACTIVITY_LOG_STORE.append(
                source="send",
                title="Test email sent",
                message=response_message,
                tone="success" if not sent_copy_error else "warning",
                session_id=self._session["id"],
                recipient=TEST_EMAIL_RECIPIENT,
            )
            self._json_response(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "message": response_message,
                    "recipient": TEST_EMAIL_RECIPIENT,
                    "subject": rendered_subject,
                },
            )
        finally:
            if imap_client is not None:
                try:
                    imap_client.logout()
                except Exception:
                    pass

    def _handle_save_template(self, payload: dict[str, Any]) -> None:
        record = template_record_from_payload(payload)
        write_saved_item(template_path(record["id"]), record, "template")
        self._json_response(
            HTTPStatus.OK,
            {
                "ok": True,
                "message": f"Template '{record['name']}' saved locally.",
                "template": record,
                "libraries": library_payload(),
            },
        )

    def _handle_load_template(self, payload: dict[str, Any]) -> None:
        template_id = saved_item_key(str(payload.get("template_id") or ""), "template")
        record = read_private_json(template_path(template_id))
        self._json_response(
            HTTPStatus.OK,
            {
                "ok": True,
                "template": record,
                "libraries": library_payload(),
            },
        )

    def _handle_save_campaign(self, payload: dict[str, Any]) -> None:
        record = campaign_record_from_payload(payload)
        write_saved_item(saved_campaign_path(record["id"]), record, "campaign")
        self._json_response(
            HTTPStatus.OK,
            {
                "ok": True,
                "message": f"Campaign '{record['name']}' saved locally.",
                "campaign": record,
                "libraries": library_payload(),
            },
        )

    def _handle_load_campaign(self, payload: dict[str, Any]) -> None:
        campaign_id = saved_item_key(str(payload.get("campaign_id") or ""), "campaign")
        record = read_private_json(saved_campaign_path(campaign_id))
        self._json_response(
            HTTPStatus.OK,
            {
                "ok": True,
                "campaign": record,
                "libraries": library_payload(),
            },
        )

    def _handle_preview(self, payload: dict[str, Any]) -> None:
        context = build_campaign_context(payload)
        self._json_response(
            HTTPStatus.OK,
            {
                "ok": True,
                "message": "Preview generated.",
                "preview": preview_payload_from_context(context),
            },
        )

    def _handle_send(self, payload: dict[str, Any]) -> None:
        context = build_campaign_context(payload)
        batch = context["analysis"].eligible[: context["batch_size"]]
        if not batch:
            self._json_response(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "message": "No eligible contacts remain for this campaign.",
                    "preview": preview_payload_from_context(context),
                    "sent": [],
                    "failed": [],
                },
            )
            return

        profile = resolve_password(normalize_smtp_payload(payload))
        config = smtp_config_from_values(profile, timeout=30.0, require_password=True)
        sent_copy = normalize_sent_copy_payload(payload, profile)
        if sent_copy["enabled"]:
            imap_config_from_values(
                sent_copy,
                timeout=30.0,
                fallback_username=config.username,
                fallback_password=config.password,
            )

        job = SEND_JOB_STORE.start(self._session["id"], context["campaign_key"], context["state_file"], payload)
        SEND_JOB_STORE.update(job["id"], batch_total=len(batch), message=f"Queued {len(batch)} email(s) for delivery.")
        self._json_response(
            HTTPStatus.ACCEPTED,
            {
                "ok": True,
                "message": f"Started batch send for {len(batch)} email(s).",
                "job": SEND_JOB_STORE.public(job["id"], self._session["id"]),
                "preview": preview_payload_from_context(context),
            },
        )

    def _handle_send_status(self, payload: dict[str, Any]) -> None:
        job_id = str(payload.get("job_id") or "").strip()
        if not job_id:
            raise SystemExit("Missing send job id.")
        self._json_response(
            HTTPStatus.OK,
            {
                "ok": True,
                "job": SEND_JOB_STORE.public(job_id, self._session["id"]),
            },
        )

    def _handle_reset_state(self, payload: dict[str, Any]) -> None:
        key = campaign_key_from_payload(payload)
        state_file = STATE_DIR / f"{key}.json"
        try:
            with held_campaign_lock(state_file):
                if state_file.exists():
                    state_file.unlink()
                    message = f"Campaign progress reset for {key}."
                else:
                    message = "No saved progress existed for this campaign."
        except CampaignLockError as exc:
            raise SystemExit(str(exc)) from exc
        self._json_response(
            HTTPStatus.OK,
            {
                "ok": True,
                "message": message,
                "campaignKey": key,
            },
        )

    def _handle_log_event(self, payload: dict[str, Any]) -> None:
        title = str(payload.get("title") or "").strip()
        message = str(payload.get("message") or "")
        tone = str(payload.get("tone") or "info").strip().lower()
        if not title:
            raise SystemExit("Log title is required.")
        if tone not in ALLOWED_LOG_TONES:
            tone = "info"
        if len(message) > 20000:
            raise SystemExit("Log message is too large.")
        ACTIVITY_LOG_STORE.append(
            source="ui",
            title=title,
            message=message,
            tone=tone,
            session_id=self._session["id"],
        )
        self._json_response(HTTPStatus.OK, {"ok": True})

    def _handle_log_export(self) -> None:
        self._csv_response(
            ACTIVITY_LOG_STORE.export_filename(),
            ACTIVITY_LOG_STORE.export_csv_bytes(),
        )


def build_server(host: str, port: int, remote_access_token: str = "") -> ThreadingHTTPServer:
    ensure_private_storage()
    ACTIVITY_LOG_STORE.ensure_ready()
    server = ThreadingHTTPServer((host, port), MailerAppHandler)
    server.remote_access_token = remote_access_token
    return server


def main() -> None:
    parser = argparse.ArgumentParser(description="Local web UI for the Mail in the Shell batch mailer.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind. Default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind. Default: 8765")
    parser.add_argument(
        "--allow-remote",
        action="store_true",
        help="Allow binding to a non-loopback interface. Use only on a trusted network.",
    )
    parser.add_argument(
        "--remote-access-token",
        default="",
        help="Optional access token required for remote API use when --allow-remote is enabled on a non-loopback host.",
    )
    parser.add_argument(
        "--open-browser",
        action="store_true",
        help="Open the local UI in your default browser after the server starts.",
    )
    args = parser.parse_args()

    if args.port <= 0 or args.port > 65535:
        raise SystemExit("Port must be between 1 and 65535.")
    if not args.allow_remote and not is_loopback_host(args.host):
        raise SystemExit("Refusing to bind outside localhost without --allow-remote.")

    remote_access_enabled = args.allow_remote and not is_loopback_host(args.host)
    remote_access_token = (
        str(args.remote_access_token).strip() or secrets.token_urlsafe(24)
        if remote_access_enabled
        else ""
    )

    server = build_server(args.host, args.port, remote_access_token=remote_access_token)
    app_url = f"http://{args.host}:{args.port}"
    browser_url = f"{app_url}/?access_token={remote_access_token}" if remote_access_enabled else app_url
    print(f"Local web interface running at {app_url}")
    print("Credentials stay on this machine. Only SMTP requests leave your Mac.")
    if remote_access_enabled:
        print("WARNING: remote access is enabled. A valid access token is required for API use.")
        print(f"Remote access URL: {browser_url}")

    if args.open_browser:
        webbrowser.open(browser_url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
