import csv
import io
import json
import tempfile
import time
import unittest
from pathlib import Path

import web_mailer_app as web_mailer_app_module
from web_mailer_app import (
    ActivityLogStore,
    FINISHED_JOB_TTL_SECONDS,
    SendJobStore,
    TEST_EMAIL_RECIPIENT,
    extract_request_access_token,
    is_loopback_host,
    list_saved_records,
    migrate_profile_defaults,
    sanitize_preview_html,
    session_mode_for_request,
    test_email_render_contact,
)


class PreviewSanitizationTests(unittest.TestCase):
    def test_sanitize_preview_html_blocks_remote_assets_but_keeps_inline_content(self) -> None:
        html = """
        <html>
          <head>
            <script>alert(1)</script>
            <link rel="stylesheet" href="https://example.com/font.css">
            <style>@import url("https://example.com/font.css"); .hero { background-image: url('https://tracker.test/a.png'); }</style>
          </head>
          <body style="background:url('https://tracker.test/bg.png')">
            <img src="https://tracker.test/pixel.png">
            <img src="data:image/png;base64,AAAA">
            <p>Hello</p>
          </body>
        </html>
        """

        sanitized = sanitize_preview_html(html)

        self.assertNotIn("<script", sanitized.lower())
        self.assertNotIn("https://tracker.test", sanitized)
        self.assertNotIn("@import", sanitized)
        self.assertIn("data:image/png;base64,AAAA", sanitized)
        self.assertIn("Hello", sanitized)


class HostBindingTests(unittest.TestCase):
    def test_loopback_hosts_are_allowed(self) -> None:
        self.assertTrue(is_loopback_host("127.0.0.1"))
        self.assertTrue(is_loopback_host("localhost"))
        self.assertTrue(is_loopback_host("::1"))

    def test_non_loopback_hosts_are_rejected(self) -> None:
        self.assertFalse(is_loopback_host("0.0.0.0"))
        self.assertFalse(is_loopback_host("192.168.1.20"))


class ProfileMigrationTests(unittest.TestCase):
    def test_reply_to_migration_uses_from_email_when_reply_to_is_invalid(self) -> None:
        migrated = migrate_profile_defaults({"reply_to": "invalid-address", "from_email": "hello@example.com"})
        self.assertEqual("hello@example.com", migrated["reply_to"])


class ActivityLogStoreTests(unittest.TestCase):
    def test_activity_logs_append_and_export_as_csv(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = ActivityLogStore(Path(temp_dir) / "activity_logs.db")
            store.append(
                source="ui",
                title="Preview ready",
                message="Campaign preview generated.",
                tone="success",
                session_id="session-1",
            )
            store.append(
                source="send",
                title="Failed",
                message="Failed to send to anna@example.com, reason: timeout",
                tone="error",
                session_id="session-1",
                campaign_key="march-batch",
                job_id="job-1",
                recipient="anna@example.com",
                row_number=3,
            )

            self.assertEqual(2, store.count())

            exported = store.export_csv_bytes().decode("utf-8-sig")
            rows = list(csv.DictReader(io.StringIO(exported)))

            self.assertEqual(2, len(rows))
            self.assertEqual("Preview ready", rows[0]["title"])
            self.assertEqual("Failed", rows[1]["title"])
            self.assertEqual("march-batch", rows[1]["campaign_key"])
            self.assertEqual("anna@example.com", rows[1]["recipient"])
            self.assertEqual("3", rows[1]["row_number"])


class RequestRoutingTests(unittest.TestCase):
    def test_session_mode_routes_bootstrap_and_posts(self) -> None:
        self.assertEqual("create", session_mode_for_request("GET", "/api/bootstrap"))
        self.assertEqual("require", session_mode_for_request("POST", "/api/preview"))
        self.assertEqual("require", session_mode_for_request("GET", "/api/log-export.csv"))
        self.assertEqual("none", session_mode_for_request("GET", "/app.js"))

    def test_extract_request_access_token_prefers_header(self) -> None:
        self.assertEqual(
            "header-token",
            extract_request_access_token("/?access_token=query-token", "header-token"),
        )
        self.assertEqual(
            "query-token",
            extract_request_access_token("/?access_token=query-token", None),
        )


class TestEmailRenderingTests(unittest.TestCase):
    def test_test_email_uses_first_campaign_recipient_as_render_context(self) -> None:
        contact = test_email_render_contact(
            {
                "campaign": {
                    "recipient_list": "first@example.com, second@example.com",
                }
            }
        )
        self.assertEqual("first@example.com", contact["email"])

    def test_test_email_falls_back_to_fixed_test_recipient_without_campaign_list(self) -> None:
        contact = test_email_render_contact({"campaign": {"recipient_list": ""}})
        self.assertEqual(TEST_EMAIL_RECIPIENT, contact["email"])


class LibraryIndexTests(unittest.TestCase):
    def test_list_saved_records_uses_index_without_loading_full_record(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            templates_dir = Path(temp_dir) / "templates"
            index_dir = Path(temp_dir) / "index" / "templates"
            templates_dir.mkdir(parents=True)
            index_dir.mkdir(parents=True)

            record_path = templates_dir / "welcome.json"
            record_path.write_text("{ this is not valid json", encoding="utf-8")
            (index_dir / "welcome.json").write_text(
                json.dumps(
                    {
                        "id": "welcome",
                        "name": "Welcome",
                        "savedAt": "2026-03-30T00:00:00+00:00",
                        "bodyMode": "html",
                        "attachmentCount": 2,
                        "recipientCount": 0,
                    }
                ),
                encoding="utf-8",
            )

            original_template_index_dir = web_mailer_app_module.TEMPLATE_INDEX_DIR
            web_mailer_app_module.TEMPLATE_INDEX_DIR = index_dir
            try:
                records = list_saved_records(templates_dir, "template")
            finally:
                web_mailer_app_module.TEMPLATE_INDEX_DIR = original_template_index_dir

            self.assertEqual(1, len(records))
            self.assertEqual("welcome", records[0]["id"])
            self.assertEqual("Welcome", records[0]["name"])
            self.assertEqual(2, records[0]["attachmentCount"])

    def test_list_saved_records_backfills_missing_index(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            templates_dir = Path(temp_dir) / "templates"
            index_dir = Path(temp_dir) / "index" / "templates"
            templates_dir.mkdir(parents=True)
            index_dir.mkdir(parents=True)

            record_path = templates_dir / "follow-up.json"
            record_path.write_text(
                json.dumps(
                    {
                        "id": "follow-up",
                        "name": "Follow-up",
                        "saved_at": "2026-03-30T12:00:00+00:00",
                        "body_mode": "both",
                        "recipient_count": 14,
                        "attachments": [{"filename": "brochure.pdf", "data_base64": "Zm9v"}],
                    }
                ),
                encoding="utf-8",
            )

            original_template_index_dir = web_mailer_app_module.TEMPLATE_INDEX_DIR
            web_mailer_app_module.TEMPLATE_INDEX_DIR = index_dir
            try:
                records = list_saved_records(templates_dir, "template")
            finally:
                web_mailer_app_module.TEMPLATE_INDEX_DIR = original_template_index_dir

            self.assertEqual("follow-up", records[0]["id"])
            self.assertTrue((index_dir / "follow-up.json").exists())


class SendJobStoreTests(unittest.TestCase):
    def test_finished_jobs_are_evicted_after_ttl(self) -> None:
        store = SendJobStore()
        job_id = "job-1"
        store._jobs[job_id] = {
            "id": job_id,
            "session_id": "session-1",
            "campaign_key": "campaign-1",
            "status": "completed",
            "created_at": "2026-03-30T00:00:00+00:00",
            "updated_at": "2026-03-30T00:00:00+00:00",
            "created_at_epoch": time.time() - FINISHED_JOB_TTL_SECONDS - 10,
            "updated_at_epoch": time.time() - FINISHED_JOB_TTL_SECONDS - 10,
            "finished_at_epoch": time.time() - FINISHED_JOB_TTL_SECONDS - 10,
            "message": "done",
            "batch_total": 1,
            "processed": 1,
            "sent_count": 1,
            "failed_count": 0,
            "current_recipient": "",
            "sent": [],
            "failed": [],
            "warnings": [],
            "preview": None,
            "error": "",
        }

        with self.assertRaises(SystemExit):
            store.public(job_id, "session-1")


if __name__ == "__main__":
    unittest.main()
