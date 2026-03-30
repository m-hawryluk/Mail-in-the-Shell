#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import csv
import html
import imaplib
import io
import json
import mimetypes
import os
import re
import socket
import smtplib
import ssl
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email import policy as email_policy
from email.message import EmailMessage
from email.utils import formataddr, formatdate, make_msgid
from pathlib import Path
from typing import Callable

try:
    import fcntl
except ImportError:  # pragma: no cover - only used on non-POSIX platforms.
    fcntl = None


EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PLACEHOLDER_PATTERN = re.compile(r"{{\s*([A-Za-z0-9_]+)\s*}}")
TRUE_VALUES = {"1", "true", "yes", "on"}


@dataclass
class SMTPConfig:
    host: str
    port: int
    username: str
    password: str
    from_email: str
    from_name: str | None
    reply_to: str | None
    use_starttls: bool
    use_ssl: bool
    timeout: float
    unsubscribe_email: str | None
    unsubscribe_url: str | None
    verify_tls: bool


@dataclass
class IMAPConfig:
    host: str
    port: int
    username: str
    password: str
    sent_folder: str
    use_ssl: bool
    verify_tls: bool
    timeout: float


@dataclass
class TemplateBundle:
    subject: str
    body_text: str
    body_html: str
    required_fields: list[str]


@dataclass
class AttachmentSpec:
    filename: str
    content_type: str
    data: bytes


@dataclass
class AnalysisResult:
    eligible: list[dict[str, str]]
    already_sent: list[dict[str, str]]
    invalid_email: list[dict[str, str]]
    duplicate_email: list[dict[str, str]]
    missing_fields: list[dict[str, str]]
    exhausted_failures: list[dict[str, str]]


@dataclass
class CampaignLock:
    path: Path
    handle: io.TextIOWrapper


class CampaignLockError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_text_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise SystemExit(f"Required file not found: {path}") from exc


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in TRUE_VALUES


def env_first(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is not None:
            return value
    return None


def strip_html_to_text(html_content: str) -> str:
    text = re.sub(r"(?is)<!--.*?-->", "", html_content)
    text = re.sub(r"(?is)<head\b.*?>.*?</head>", "", text)
    text = re.sub(r"(?is)<(script|style)\b.*?>.*?</\1>", "", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n\n", text)
    text = re.sub(r"(?i)</div\s*>", "\n", text)
    text = re.sub(r"(?i)</li\s*>", "\n", text)
    text = re.sub(r"(?i)</tr\s*>", "\n", text)
    text = re.sub(r"(?i)</td\s*>", " ", text)
    text = re.sub(r"(?i)<li\b[^>]*>", "- ", text)
    text = re.sub(r"(?is)<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", r"\2 (\1)", text)
    text = re.sub(r"(?s)<[^>]+>", "", text)
    text = html.unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    cleaned_lines: list[str] = []
    blank_streak = 0
    for raw_line in text.split("\n"):
        normalized_line = re.sub(r"[ \t]+", " ", raw_line).strip()
        if not normalized_line:
            if blank_streak == 0:
                cleaned_lines.append("")
            blank_streak += 1
            continue
        blank_streak = 0
        cleaned_lines.append(normalized_line)

    return "\n".join(cleaned_lines).strip()


def wrap_text_as_html(body_text: str) -> str:
    escaped_body = html.escape(body_text)
    return (
        "<!DOCTYPE html><html><body>"
        "<pre style=\"font-family:Arial,sans-serif;white-space:pre-wrap;\">"
        f"{escaped_body}"
        "</pre></body></html>"
    )


def build_templates(
    subject: str,
    body_text: str = "",
    body_html: str = "",
    generate_missing_alternative: bool = True,
) -> TemplateBundle:
    if not body_text and not body_html:
        raise SystemExit("Provide at least one email body template with --html-file or --body-file.")

    if body_html and not body_text and generate_missing_alternative:
        body_text = strip_html_to_text(body_html)

    if body_text and not body_html and generate_missing_alternative:
        body_html = wrap_text_as_html(body_text)

    required = set(extract_placeholders(subject))
    required.update(extract_placeholders(body_text))
    required.update(extract_placeholders(body_html))

    return TemplateBundle(
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        required_fields=sorted(required),
    )


def load_templates(subject_file: Path, body_file: Path | None, html_file: Path | None) -> TemplateBundle:
    subject = read_text_file(subject_file).strip()
    body_text = read_text_file(body_file) if body_file else ""
    body_html = read_text_file(html_file) if html_file else ""
    return build_templates(subject=subject, body_text=body_text, body_html=body_html, generate_missing_alternative=True)


def extract_placeholders(template_text: str) -> list[str]:
    return [match.group(1) for match in PLACEHOLDER_PATTERN.finditer(template_text)]


def split_email_address(value: str) -> tuple[str, str]:
    local_part, domain_part = value.rsplit("@", 1)
    return local_part.strip(), domain_part.strip()


def normalize_email_domain(domain_part: str) -> tuple[str, bool]:
    cleaned_domain = domain_part.strip().rstrip(".")
    if not cleaned_domain:
        return "", False
    try:
        return cleaned_domain.encode("idna").decode("ascii").lower(), False
    except UnicodeError:
        return cleaned_domain.lower(), True


def normalize_email(value: str | None) -> str:
    cleaned_value = (value or "").strip()
    if not cleaned_value:
        return ""
    if "@" not in cleaned_value:
        return cleaned_value.lower()

    local_part, domain_part = split_email_address(cleaned_value)
    normalized_domain, _ = normalize_email_domain(domain_part)
    return f"{local_part.lower()}@{normalized_domain}"


def email_requires_smtputf8(value: str | None) -> bool:
    cleaned_value = (value or "").strip()
    if not cleaned_value or "@" not in cleaned_value:
        return False

    local_part, domain_part = split_email_address(cleaned_value)
    _, domain_requires_utf8 = normalize_email_domain(domain_part)
    return (not local_part.isascii()) or domain_requires_utf8


def smtp_utf8_requirement_message(recipient: str) -> str:
    return (
        f"{recipient} requires SMTPUTF8 support because its address contains non-ASCII characters "
        "that cannot be sent safely through a plain ASCII envelope."
    )


def is_valid_email(value: str) -> bool:
    return bool(EMAIL_PATTERN.match(value))


def sniff_csv_dialect(sample: str) -> csv.Dialect:
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        return csv.excel


def parse_contacts_csv(csv_text: str, email_column: str) -> list[dict[str, str]]:
    handle = io.StringIO(csv_text)
    sample = handle.read(4096)
    handle.seek(0)
    dialect = sniff_csv_dialect(sample)
    reader = csv.DictReader(handle, dialect=dialect)

    if not reader.fieldnames:
        raise SystemExit("CSV file has no header row.")

    normalized_headers = [header.strip() if header else "" for header in reader.fieldnames]
    if email_column not in normalized_headers:
        raise SystemExit(
            f"CSV must contain an '{email_column}' column. Available columns: {', '.join(normalized_headers)}"
        )

    contacts: list[dict[str, str]] = []
    for row_number, row in enumerate(reader, start=2):
        normalized_row: dict[str, str] = {}
        for key, value in row.items():
            clean_key = (key or "").strip()
            clean_value = value.strip() if isinstance(value, str) else ""
            normalized_row[clean_key] = clean_value
        normalized_row["__row_number"] = str(row_number)
        contacts.append(normalized_row)

    return contacts


def load_contacts(csv_path: Path, email_column: str) -> list[dict[str, str]]:
    if not csv_path.exists():
        raise SystemExit(f"CSV file not found: {csv_path}")
    csv_text = csv_path.read_text(encoding="utf-8-sig")
    return parse_contacts_csv(csv_text, email_column)


def parse_email_list(email_list_text: str) -> list[dict[str, str]]:
    raw_items = re.split(r"[,\n;]+", email_list_text)
    contacts: list[dict[str, str]] = []
    row_number = 1

    for raw_item in raw_items:
        email = raw_item.strip()
        if not email:
            continue
        contacts.append(
            {
                "email": email,
                "__row_number": str(row_number),
            }
        )
        row_number += 1

    if not contacts:
        raise SystemExit("Enter at least one recipient email.")

    return contacts


def default_state(source_id: str) -> dict:
    return {
        "version": 1,
        "source_id": source_id,
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "sent_rows": {},
        "failed_rows": {},
    }


def campaign_lock_path(state_file: Path) -> Path:
    return state_file.with_name(f"{state_file.name}.lock")


def acquire_campaign_lock_nowait(state_file: Path) -> CampaignLock:
    lock_path = campaign_lock_path(state_file)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")

    if fcntl is None:  # pragma: no cover - Windows fallback.
        return CampaignLock(path=lock_path, handle=handle)

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise CampaignLockError(
            f"Campaign state is locked by another send process: {state_file.resolve()}"
        ) from exc

    handle.seek(0)
    handle.truncate(0)
    handle.write(f"pid={os.getpid()} acquired_at={utc_now()}\n")
    handle.flush()
    return CampaignLock(path=lock_path, handle=handle)


def release_campaign_lock(lock: CampaignLock | None) -> None:
    if lock is None:
        return

    try:
        if fcntl is not None:
            fcntl.flock(lock.handle.fileno(), fcntl.LOCK_UN)
    finally:
        lock.handle.close()


@contextlib.contextmanager
def held_campaign_lock(state_file: Path):
    lock = acquire_campaign_lock_nowait(state_file)
    try:
        yield lock
    finally:
        release_campaign_lock(lock)


def load_state(state_file: Path, source_id: str) -> dict:
    if not state_file.exists():
        return default_state(source_id)

    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"State file is not valid JSON: {state_file}") from exc

    data.setdefault("version", 1)
    data.setdefault("sent_rows", {})
    data.setdefault("failed_rows", {})
    data.setdefault("created_at", utc_now())
    data.setdefault("updated_at", utc_now())

    stored_source_id = data.get("source_id")
    has_history = bool(data["sent_rows"] or data["failed_rows"])
    if stored_source_id and stored_source_id != source_id and has_history:
        raise SystemExit(
            "State file belongs to a different campaign source. Use a separate --state-file or run reset-state first."
        )

    data["source_id"] = source_id
    return data


def save_state(state_file: Path, state: dict) -> None:
    state["updated_at"] = utc_now()
    state_file.parent.mkdir(parents=True, exist_ok=True)
    temp_file_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=state_file.parent,
            prefix=f"{state_file.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_file_path = temp_file.name
            json.dump(state, temp_file, indent=2, sort_keys=True)
            temp_file.write("\n")
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_file_path, state_file)
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


def analyze_contacts(
    contacts: list[dict[str, str]],
    state: dict,
    email_column: str,
    required_fields: list[str],
    max_attempts_per_row: int,
    retry_exhausted: bool,
) -> AnalysisResult:
    sent_rows = state.get("sent_rows", {})
    failed_rows = state.get("failed_rows", {})
    claimed_emails: set[str] = set()

    eligible: list[dict[str, str]] = []
    already_sent: list[dict[str, str]] = []
    invalid_email: list[dict[str, str]] = []
    duplicate_email: list[dict[str, str]] = []
    missing_fields: list[dict[str, str]] = []
    exhausted_failures: list[dict[str, str]] = []

    for contact in contacts:
        row_number = contact["__row_number"]

        if row_number in sent_rows:
            already_sent.append(contact)
            sent_email = normalize_email(sent_rows[row_number].get("email", ""))
            if sent_email:
                claimed_emails.add(sent_email)
            continue

        email = normalize_email(contact.get(email_column, ""))
        if not email or not is_valid_email(email):
            invalid_email.append(contact)
            continue

        if email_requires_smtputf8(contact.get(email_column, "")):
            contact["__requires_smtputf8"] = "true"

        missing = [field for field in required_fields if not str(contact.get(field, "")).strip()]
        if missing:
            contact["__missing_fields"] = ", ".join(missing)
            missing_fields.append(contact)
            continue

        failure = failed_rows.get(row_number)
        attempts = int(failure.get("attempts", 0)) if failure else 0
        contact["__attempts"] = str(attempts)
        if failure and attempts >= max_attempts_per_row and not retry_exhausted:
            exhausted_failures.append(contact)
            continue

        if email in claimed_emails:
            duplicate_email.append(contact)
            continue
        claimed_emails.add(email)
        eligible.append(contact)

    return AnalysisResult(
        eligible=eligible,
        already_sent=already_sent,
        invalid_email=invalid_email,
        duplicate_email=duplicate_email,
        missing_fields=missing_fields,
        exhausted_failures=exhausted_failures,
    )


def render_template(template_text: str, row: dict[str, str]) -> str:
    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        return str(row.get(key, "")).strip()

    return PLACEHOLDER_PATTERN.sub(replace, template_text)


def smtp_config_from_values(values: dict[str, object], timeout: float, require_password: bool = True) -> SMTPConfig:
    host = str(values.get("host") or "").strip()
    raw_port = str(values.get("port") or "587").strip()
    from_email = str(values.get("from_email") or "").strip()
    username = str(values.get("username") or from_email).strip()
    password = str(values.get("password") or "").strip()
    from_name = str(values.get("from_name") or "").strip() or None
    reply_to = str(values.get("reply_to") or "").strip() or None
    unsubscribe_email = str(values.get("unsubscribe_email") or "").strip() or None
    unsubscribe_url = str(values.get("unsubscribe_url") or "").strip() or None

    try:
        port = int(raw_port)
    except ValueError as exc:
        raise SystemExit("SMTP port must be a number.") from exc

    use_starttls = bool(values.get("use_starttls", True))
    use_ssl = bool(values.get("use_ssl", False))
    verify_tls = bool(values.get("verify_tls", True))

    missing = []
    if not host:
        missing.append("host")
    if not from_email:
        missing.append("from_email")
    if not username:
        missing.append("username")
    if require_password and not password:
        missing.append("password")

    if missing:
        raise SystemExit("Missing SMTP configuration values: " + ", ".join(missing))

    if use_ssl and use_starttls:
        raise SystemExit("Use either SSL or STARTTLS, not both. Update .env accordingly.")

    return SMTPConfig(
        host=host,
        port=port,
        username=username,
        password=password,
        from_email=from_email,
        from_name=from_name,
        reply_to=reply_to,
        use_starttls=use_starttls,
        use_ssl=use_ssl,
        timeout=timeout,
        unsubscribe_email=unsubscribe_email,
        unsubscribe_url=unsubscribe_url,
        verify_tls=verify_tls,
    )


def load_smtp_config(timeout: float) -> SMTPConfig:
    return smtp_config_from_values(
        {
            "host": env_first("MAILER_SMTP_HOST"),
            "port": env_first("MAILER_SMTP_PORT") or "587",
            "username": env_first("MAILER_SMTP_USERNAME", "MAILER_FROM_EMAIL") or "",
            "password": env_first("MAILER_SMTP_PASSWORD"),
            "from_email": env_first("MAILER_FROM_EMAIL"),
            "from_name": env_first("MAILER_FROM_NAME"),
            "reply_to": env_first("MAILER_REPLY_TO"),
            "use_starttls": env_bool("MAILER_SMTP_USE_STARTTLS", True),
            "use_ssl": env_bool("MAILER_SMTP_USE_SSL", False),
            "verify_tls": env_bool("MAILER_VERIFY_TLS", True),
            "unsubscribe_email": env_first("MAILER_UNSUBSCRIBE_EMAIL"),
            "unsubscribe_url": env_first("MAILER_UNSUBSCRIBE_URL"),
        },
        timeout=timeout,
        require_password=True,
    )


def imap_config_from_values(
    values: dict[str, object],
    timeout: float,
    fallback_username: str = "",
    fallback_password: str = "",
) -> IMAPConfig:
    host = str(values.get("host") or "").strip()
    raw_port = str(values.get("port") or "993").strip()
    username = str(values.get("username") or fallback_username).strip()
    password = str(values.get("password") or fallback_password).strip()
    sent_folder = str(values.get("sent_folder") or "Sent").strip() or "Sent"
    use_ssl = bool(values.get("use_ssl", True))
    verify_tls = bool(values.get("verify_tls", True))

    try:
        port = int(raw_port)
    except ValueError as exc:
        raise SystemExit("IMAP port must be a number.") from exc

    missing = []
    if not host:
        missing.append("imap_host")
    if not username:
        missing.append("imap_username")
    if not password:
        missing.append("imap_password")

    if missing:
        raise SystemExit("Missing IMAP configuration values: " + ", ".join(missing))

    return IMAPConfig(
        host=host,
        port=port,
        username=username,
        password=password,
        sent_folder=sent_folder,
        use_ssl=use_ssl,
        verify_tls=verify_tls,
        timeout=timeout,
    )


def open_smtp_connection(config: SMTPConfig) -> smtplib.SMTP:
    context = ssl.create_default_context() if config.verify_tls else ssl._create_unverified_context()
    if config.use_ssl:
        client = smtplib.SMTP_SSL(config.host, config.port, timeout=config.timeout, context=context)
        client.ehlo()
    else:
        client = smtplib.SMTP(config.host, config.port, timeout=config.timeout)
        client.ehlo()
        if config.use_starttls:
            client.starttls(context=context)
            client.ehlo()

    client.login(config.username, config.password)
    return client


def smtp_supports_smtputf8(client: smtplib.SMTP) -> bool:
    return bool(getattr(client, "does_esmtp", False) and client.has_extn("smtputf8"))


def open_imap_connection(config: IMAPConfig) -> imaplib.IMAP4:
    if config.use_ssl:
        context = ssl.create_default_context() if config.verify_tls else ssl._create_unverified_context()
        client = imaplib.IMAP4_SSL(config.host, config.port, ssl_context=context, timeout=config.timeout)
    else:
        client = imaplib.IMAP4(config.host, config.port, timeout=config.timeout)

    client.login(config.username, config.password)
    return client


def ensure_imap_sent_folder(client: imaplib.IMAP4, config: IMAPConfig) -> None:
    status, _ = client.select(config.sent_folder, readonly=True)
    if status != "OK":
        raise RuntimeError(f"IMAP sent folder '{config.sent_folder}' was not found or is not accessible.")
    client.close()


def append_sent_copy(client: imaplib.IMAP4, config: IMAPConfig, message_bytes: bytes) -> None:
    status, response = client.append(
        config.sent_folder,
        "\\Seen",
        imaplib.Time2Internaldate(time.time()),
        message_bytes,
    )
    if status != "OK":
        detail = response[0].decode("utf-8", errors="replace") if response else "unknown error"
        raise RuntimeError(f"IMAP append to '{config.sent_folder}' failed: {detail}")


def describe_smtp_exception(exc: Exception, config: SMTPConfig) -> str:
    if isinstance(exc, smtplib.SMTPAuthenticationError):
        return "SMTP login failed. Check the username, password, or app password for this mailbox."
    if isinstance(exc, ssl.SSLCertVerificationError):
        return (
            "TLS certificate verification failed. Use the provider's official SMTP hostname, "
            "or, if you trust this server and it uses a self-signed or broken cert chain, "
            "turn off TLS verification in the UI and try again."
        )
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return (
            f"SMTP connection to {config.host}:{config.port} timed out. "
            "Check the host, port, and whether SSL or STARTTLS matches your provider."
        )
    if isinstance(exc, smtplib.SMTPServerDisconnected):
        return (
            "SMTP server closed the connection unexpectedly. "
            "This usually means the host, port, or SSL/STARTTLS mode is wrong."
        )
    if isinstance(exc, ssl.SSLError):
        return "TLS handshake failed. Try SSL on port 465 or STARTTLS on port 587, depending on your provider."
    if isinstance(exc, ConnectionRefusedError):
        return f"SMTP connection to {config.host}:{config.port} was refused. Check the host and port."
    if isinstance(exc, OSError):
        return f"SMTP network error: {exc.strerror or str(exc)}"
    return str(exc)


def describe_imap_exception(exc: Exception, config: IMAPConfig) -> str:
    if isinstance(exc, imaplib.IMAP4.error):
        return (
            "IMAP login or folder access failed. Check the IMAP username, password, "
            f"and sent folder name '{config.sent_folder}'."
        )
    if isinstance(exc, ssl.SSLCertVerificationError):
        return (
            "IMAP TLS certificate verification failed. Use the provider's official IMAP hostname, "
            "or turn off IMAP TLS verification only if you trust that server."
        )
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return f"IMAP connection to {config.host}:{config.port} timed out. Check the IMAP host, port, and SSL setting."
    if isinstance(exc, ssl.SSLError):
        return "IMAP TLS handshake failed. Try IMAP SSL on port 993 if your provider supports it."
    if isinstance(exc, ConnectionRefusedError):
        return f"IMAP connection to {config.host}:{config.port} was refused. Check the IMAP host and port."
    if isinstance(exc, OSError):
        return f"IMAP network error: {exc.strerror or str(exc)}"
    return str(exc)


def test_smtp_connection(config: SMTPConfig) -> None:
    try:
        with open_smtp_connection(config):
            return
    except Exception as exc:
        raise SystemExit(describe_smtp_exception(exc, config)) from exc


def test_imap_connection(config: IMAPConfig) -> None:
    client: imaplib.IMAP4 | None = None
    try:
        client = open_imap_connection(config)
        ensure_imap_sent_folder(client, config)
    except Exception as exc:
        raise SystemExit(describe_imap_exception(exc, config)) from exc
    finally:
        if client is not None:
            try:
                client.logout()
            except Exception:
                pass


def build_message(
    config: SMTPConfig,
    contact: dict[str, str],
    email_column: str,
    templates: TemplateBundle,
    attachments: list[AttachmentSpec] | None = None,
    recipient_override: str | None = None,
) -> EmailMessage:
    subject = render_template(templates.subject, contact).replace("\n", " ").strip()
    body_text = render_template(templates.body_text, contact)
    body_html = render_template(templates.body_html, contact)
    recipient = normalize_email(recipient_override or contact[email_column])

    message = EmailMessage()
    message["To"] = recipient
    message["From"] = (
        formataddr((config.from_name, config.from_email)) if config.from_name else config.from_email
    )
    message["Subject"] = subject
    message["Date"] = formatdate(localtime=True)
    message["Message-ID"] = make_msgid(domain=config.from_email.split("@", 1)[-1])

    if config.reply_to:
        message["Reply-To"] = config.reply_to

    unsubscribe_values = []
    if config.unsubscribe_email:
        unsubscribe_values.append(f"<mailto:{config.unsubscribe_email}>")
    if config.unsubscribe_url:
        unsubscribe_values.append(f"<{config.unsubscribe_url}>")
        message["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"
    if unsubscribe_values:
        message["List-Unsubscribe"] = ", ".join(unsubscribe_values)

    if body_text and body_html:
        message.set_content(body_text)
        message.add_alternative(body_html, subtype="html")
    elif body_html:
        message.set_content(body_html, subtype="html")
    elif body_text:
        message.set_content(body_text)
    else:
        raise SystemExit("Rendered message body is empty.")

    for attachment in attachments or []:
        content_type = attachment.content_type.strip() or mimetypes.guess_type(attachment.filename)[0] or "application/octet-stream"
        maintype, subtype = content_type.split("/", 1) if "/" in content_type else ("application", "octet-stream")
        subtype = subtype.split(";", 1)[0].strip() or "octet-stream"
        message.add_attachment(
            attachment.data,
            maintype=maintype.strip() or "application",
            subtype=subtype,
            filename=attachment.filename,
        )

    return message


def print_summary(analysis: AnalysisResult, batch_size: int, state_file: Path) -> None:
    next_batch = analysis.eligible[:batch_size]
    print(f"State file: {state_file.resolve()}")
    print(f"Ready to send now: {len(next_batch)}")
    print(f"Eligible remaining total: {len(analysis.eligible)}")
    print(f"Already sent: {len(analysis.already_sent)}")
    print(f"Invalid emails: {len(analysis.invalid_email)}")
    print(f"Duplicate emails skipped: {len(analysis.duplicate_email)}")
    print(f"Rows missing template fields: {len(analysis.missing_fields)}")
    print(f"Rows paused after too many failures: {len(analysis.exhausted_failures)}")


def preview_contacts(
    analysis: AnalysisResult,
    templates: TemplateBundle,
    email_column: str,
    batch_size: int,
    preview_limit: int,
    state_file: Path,
) -> None:
    batch = analysis.eligible[:batch_size]
    print_summary(analysis, batch_size, state_file)
    print()

    if not batch:
        print("No eligible contacts remain for the next batch.")
        return

    print(f"Next batch recipients (showing up to {preview_limit}):")
    for contact in batch[:preview_limit]:
        print(f"- row {contact['__row_number']}: {normalize_email(contact[email_column])}")
    if len(batch) > preview_limit:
        print(f"- ... and {len(batch) - preview_limit} more")

    sample = batch[0]
    print()
    print("Rendered sample for the first pending contact:")
    print(f"Subject: {render_template(templates.subject, sample).replace(chr(10), ' ').strip()}")
    print("---")
    print(render_template(templates.body_text, sample).rstrip())
    print("---")

    if analysis.missing_fields:
        print()
        print("Rows missing template fields (first 5):")
        for contact in analysis.missing_fields[:5]:
            print(
                f"- row {contact['__row_number']}: {normalize_email(contact.get(email_column, '')) or '<empty>'} "
                f"missing [{contact['__missing_fields']}]"
            )

    smtp_utf8_rows = [contact for contact in analysis.eligible if contact.get("__requires_smtputf8") == "true"]
    if smtp_utf8_rows:
        print()
        print("Rows that require SMTPUTF8 support (first 5):")
        for contact in smtp_utf8_rows[:5]:
            recipient = normalize_email(contact.get(email_column, ""))
            print(f"- row {contact['__row_number']}: {smtp_utf8_requirement_message(recipient)}")


def mark_sent(
    state: dict,
    row_number: str,
    email: str,
    message_id: str,
    sent_copy_saved: bool | None = None,
    sent_copy_error: str | None = None,
) -> None:
    state["sent_rows"][row_number] = {
        "email": email,
        "message_id": message_id,
        "sent_at": utc_now(),
    }
    if sent_copy_saved is not None:
        state["sent_rows"][row_number]["sent_copy_saved"] = sent_copy_saved
    if sent_copy_error:
        state["sent_rows"][row_number]["sent_copy_error"] = sent_copy_error
    state["failed_rows"].pop(row_number, None)


def mark_failed(state: dict, row_number: str, email: str, error_message: str) -> None:
    existing = state["failed_rows"].get(row_number, {})
    attempts = int(existing.get("attempts", 0)) + 1
    state["failed_rows"][row_number] = {
        "email": email,
        "attempts": attempts,
        "last_error": error_message,
        "last_failed_at": utc_now(),
    }


def send_batch(
    *,
    smtp_client: smtplib.SMTP,
    supports_smtputf8: bool,
    config: SMTPConfig,
    batch: list[dict[str, str]],
    state: dict,
    state_file: Path,
    email_column: str,
    templates: TemplateBundle,
    pause_seconds: float,
    attachments: list[AttachmentSpec] | None = None,
    imap_client: imaplib.IMAP4 | None = None,
    imap_config: IMAPConfig | None = None,
    fail_fast: bool = False,
    on_contact_start: Callable[[int, int, str], None] | None = None,
    on_contact_complete: Callable[[int, int], None] | None = None,
    on_sent: Callable[[dict[str, object]], None] | None = None,
    on_failed: Callable[[dict[str, object]], None] | None = None,
    on_warning: Callable[[str], None] | None = None,
) -> dict[str, list[object]]:
    sent_items: list[dict[str, object]] = []
    failed_items: list[dict[str, object]] = []
    warning_messages: list[str] = []
    total = len(batch)

    for index, contact in enumerate(batch, start=1):
        row_number = contact["__row_number"]
        recipient = normalize_email(contact.get(email_column))
        if on_contact_start is not None:
            on_contact_start(index, total, recipient)

        requires_smtputf8 = contact.get("__requires_smtputf8") == "true"
        if requires_smtputf8 and not supports_smtputf8:
            error_message = smtp_utf8_requirement_message(recipient)
            mark_failed(state, row_number, recipient, error_message)
            failed_item = {
                "row": row_number,
                "email": recipient,
                "status": "failed",
                "error": error_message,
            }
            failed_items.append(failed_item)
            if on_failed is not None:
                on_failed(failed_item)
            save_state(state_file, state)
            if on_contact_complete is not None:
                on_contact_complete(index, total)
            if fail_fast:
                raise SystemExit("Stopping after first failure because --fail-fast was supplied.")
            if index < total and pause_seconds > 0:
                time.sleep(pause_seconds)
            continue

        message = build_message(config, contact, email_column, templates, attachments=attachments)
        message_bytes = message.as_bytes(policy=email_policy.SMTP) if imap_client is not None else None

        try:
            smtp_client.send_message(message)
            sent_copy_saved = None
            sent_copy_error = None
            if imap_client is not None and imap_config is not None and message_bytes is not None:
                try:
                    append_sent_copy(imap_client, imap_config, message_bytes)
                    sent_copy_saved = True
                except Exception as exc:
                    sent_copy_saved = False
                    sent_copy_error = describe_imap_exception(exc, imap_config)
                    warning_text = f"{recipient}: {sent_copy_error}"
                    warning_messages.append(warning_text)
                    if on_warning is not None:
                        on_warning(warning_text)

            mark_sent(
                state,
                row_number,
                recipient,
                str(message["Message-ID"]),
                sent_copy_saved=sent_copy_saved,
                sent_copy_error=sent_copy_error,
            )
            sent_item = {
                "row": row_number,
                "email": recipient,
                "status": "sent",
                "savedToSent": sent_copy_saved,
                "warning": sent_copy_error,
            }
            sent_items.append(sent_item)
            if on_sent is not None:
                on_sent(sent_item)
        except Exception as exc:  # pragma: no cover - SMTP failures are environment-specific.
            error_message = describe_smtp_exception(exc, config)
            mark_failed(state, row_number, recipient, error_message)
            failed_item = {
                "row": row_number,
                "email": recipient,
                "status": "failed",
                "error": error_message,
            }
            failed_items.append(failed_item)
            if on_failed is not None:
                on_failed(failed_item)
            save_state(state_file, state)
            if on_contact_complete is not None:
                on_contact_complete(index, total)
            if fail_fast:
                raise SystemExit("Stopping after first failure because --fail-fast was supplied.") from exc
            if index < total and pause_seconds > 0:
                time.sleep(pause_seconds)
            continue

        save_state(state_file, state)
        if on_contact_complete is not None:
            on_contact_complete(index, total)
        if index < total and pause_seconds > 0:
            time.sleep(pause_seconds)

    return {
        "sent": sent_items,
        "failed": failed_items,
        "warnings": warning_messages,
    }


def command_preview(args: argparse.Namespace) -> None:
    load_env_file(Path(args.env_file))
    templates = load_templates(Path(args.subject_file), path_or_none(args.body_file), path_or_none(args.html_file))
    contacts = load_contacts(Path(args.csv), args.email_column)
    state = load_state(Path(args.state_file), str(Path(args.csv).resolve()))
    analysis = analyze_contacts(
        contacts=contacts,
        state=state,
        email_column=args.email_column,
        required_fields=templates.required_fields,
        max_attempts_per_row=args.max_attempts_per_row,
        retry_exhausted=args.retry_exhausted,
    )
    preview_contacts(
        analysis=analysis,
        templates=templates,
        email_column=args.email_column,
        batch_size=args.batch_size,
        preview_limit=args.preview_limit,
        state_file=Path(args.state_file),
    )


def command_send(args: argparse.Namespace) -> None:
    if not args.confirm_live_send:
        raise SystemExit("Refusing to send live email without --confirm-live-send.")

    load_env_file(Path(args.env_file))
    templates = load_templates(Path(args.subject_file), path_or_none(args.body_file), path_or_none(args.html_file))
    contacts = load_contacts(Path(args.csv), args.email_column)
    state_file = Path(args.state_file)
    try:
        with held_campaign_lock(state_file):
            state = load_state(state_file, str(Path(args.csv).resolve()))
            analysis = analyze_contacts(
                contacts=contacts,
                state=state,
                email_column=args.email_column,
                required_fields=templates.required_fields,
                max_attempts_per_row=args.max_attempts_per_row,
                retry_exhausted=args.retry_exhausted,
            )
            batch = analysis.eligible[: args.batch_size]

            print_summary(analysis, args.batch_size, state_file)
            if not batch:
                print("Nothing to send.")
                return

            config = load_smtp_config(timeout=args.smtp_timeout)
            print()
            print(f"SMTP host: {config.host}:{config.port}")
            print(f"From address: {config.from_email}")
            print(f"Starting batch send of {len(batch)} message(s)...")

            sent_count = 0
            failed_count = 0

            with open_smtp_connection(config) as smtp_client:
                supports_smtputf8 = smtp_supports_smtputf8(smtp_client)
                progress = {"index": 0}

                def on_contact_start(index: int, total: int, recipient: str) -> None:
                    progress["index"] = index

                def on_sent(item: dict[str, object]) -> None:
                    print(f"[{progress['index']}/{len(batch)}] SENT row {item['row']} -> {item['email']}")

                def on_failed(item: dict[str, object]) -> None:
                    print(
                        f"[{progress['index']}/{len(batch)}] FAILED row {item['row']} -> {item['email']}: {item['error']}",
                        file=sys.stderr,
                    )

                result = send_batch(
                    smtp_client=smtp_client,
                    supports_smtputf8=supports_smtputf8,
                    config=config,
                    batch=batch,
                    state=state,
                    state_file=state_file,
                    email_column=args.email_column,
                    templates=templates,
                    pause_seconds=args.pause_seconds,
                    fail_fast=args.fail_fast,
                    on_contact_start=on_contact_start,
                    on_sent=on_sent,
                    on_failed=on_failed,
                )
                sent_count = len(result["sent"])
                failed_count = len(result["failed"])
    except CampaignLockError as exc:
        raise SystemExit(str(exc)) from exc

    print()
    print(f"Batch finished. Sent: {sent_count}. Failed: {failed_count}.")
    if failed_count:
        print("Failed rows stay in the state file and can be retried on the next run.")


def command_reset_state(args: argparse.Namespace) -> None:
    if not args.confirm_reset:
        raise SystemExit("Refusing to reset state without --confirm-reset.")

    state_file = Path(args.state_file)
    try:
        with held_campaign_lock(state_file):
            if state_file.exists():
                state_file.unlink()
                print(f"Removed state file: {state_file.resolve()}")
            else:
                print(f"No state file found at: {state_file.resolve()}")
    except CampaignLockError as exc:
        raise SystemExit(str(exc)) from exc


def path_or_none(raw_path: str | None) -> Path | None:
    return Path(raw_path) if raw_path else None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send CSV-based email batches through SMTP, defaulting to 100 contacts per run."
    )
    parser.set_defaults(handler=None)

    message_parent = argparse.ArgumentParser(add_help=False)
    message_parent.add_argument("--csv", required=True, help="Path to the contacts CSV file.")
    message_parent.add_argument("--subject-file", required=True, help="Text file used for the email subject.")
    message_parent.add_argument(
        "--body-file",
        help="Optional plain text fallback body. If omitted, one is generated from --html-file.",
    )
    message_parent.add_argument(
        "--html-file",
        help="HTML email body file. Recommended for live campaigns.",
    )
    message_parent.add_argument("--env-file", default=".env", help="Path to the SMTP env file.")
    message_parent.add_argument(
        "--state-file",
        default="state/send_state.json",
        help="Path to the JSON state file used for resume tracking.",
    )
    message_parent.add_argument(
        "--email-column",
        default="email",
        help="CSV column name containing the recipient email address.",
    )
    message_parent.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Maximum number of emails to send in one run. Default: 100.",
    )
    message_parent.add_argument(
        "--max-attempts-per-row",
        type=int,
        default=3,
        help="After this many failures, a row is skipped until --retry-exhausted is used.",
    )
    message_parent.add_argument(
        "--retry-exhausted",
        action="store_true",
        help="Allow rows that previously hit the max failure count back into the queue.",
    )

    subparsers = parser.add_subparsers(dest="command")

    preview_parser = subparsers.add_parser(
        "preview",
        parents=[message_parent],
        help="Show the next batch without sending anything.",
    )
    preview_parser.add_argument(
        "--preview-limit",
        type=int,
        default=10,
        help="How many recipients to print from the next pending batch.",
    )
    preview_parser.set_defaults(handler=command_preview)

    send_parser = subparsers.add_parser(
        "send",
        parents=[message_parent],
        help="Send the next batch through SMTP.",
    )
    send_parser.add_argument(
        "--confirm-live-send",
        action="store_true",
        help="Required safety flag for live sending.",
    )
    send_parser.add_argument(
        "--pause-seconds",
        type=float,
        default=1.0,
        help="Delay between each email send. Default: 1 second.",
    )
    send_parser.add_argument(
        "--smtp-timeout",
        type=float,
        default=30.0,
        help="SMTP socket timeout in seconds.",
    )
    send_parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop the run after the first SMTP failure.",
    )
    send_parser.set_defaults(handler=command_send)

    reset_parser = subparsers.add_parser(
        "reset-state",
        help="Delete the local state file so the CSV can be reprocessed from the top.",
    )
    reset_parser.add_argument(
        "--state-file",
        default="state/send_state.json",
        help="Path to the JSON state file used for resume tracking.",
    )
    reset_parser.add_argument(
        "--confirm-reset",
        action="store_true",
        help="Required safety flag for deleting the state file.",
    )
    reset_parser.set_defaults(handler=command_reset_state)

    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.handler is None:
        raise SystemExit("Choose one command: preview, send, or reset-state.")

    if hasattr(args, "body_file") and hasattr(args, "html_file"):
        if not args.body_file and not args.html_file:
            raise SystemExit("Provide --html-file, --body-file, or both.")

    if hasattr(args, "batch_size") and args.batch_size <= 0:
        raise SystemExit("--batch-size must be greater than 0.")

    if hasattr(args, "max_attempts_per_row") and args.max_attempts_per_row <= 0:
        raise SystemExit("--max-attempts-per-row must be greater than 0.")

    if hasattr(args, "pause_seconds") and args.pause_seconds < 0:
        raise SystemExit("--pause-seconds cannot be negative.")


def main() -> None:
    parser = build_parser()
    parsed_args = parser.parse_args()
    validate_args(parsed_args)
    parsed_args.handler(parsed_args)


if __name__ == "__main__":
    main()
