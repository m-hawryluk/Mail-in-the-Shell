import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from bulk_mailer import (
    CampaignLockError,
    acquire_campaign_lock_nowait,
    analyze_contacts,
    email_requires_smtputf8,
    normalize_email,
    release_campaign_lock,
    save_state,
)


class AnalyzeContactsTests(unittest.TestCase):
    def test_valid_duplicate_after_missing_fields_stays_sendable(self) -> None:
        contacts = [
            {"__row_number": "1", "email": "hello@example.com", "name": ""},
            {"__row_number": "2", "email": "hello@example.com", "name": "Alice"},
        ]
        state = {"sent_rows": {}, "failed_rows": {}}

        result = analyze_contacts(
            contacts=contacts,
            state=state,
            email_column="email",
            required_fields=["name"],
            max_attempts_per_row=3,
            retry_exhausted=False,
        )

        self.assertEqual(["2"], [contact["__row_number"] for contact in result.eligible])
        self.assertEqual(["1"], [contact["__row_number"] for contact in result.missing_fields])
        self.assertEqual([], result.duplicate_email)

    def test_valid_duplicate_after_exhausted_failure_stays_sendable(self) -> None:
        contacts = [
            {"__row_number": "1", "email": "hello@example.com", "name": "Blocked"},
            {"__row_number": "2", "email": "hello@example.com", "name": "Ready"},
        ]
        state = {"sent_rows": {}, "failed_rows": {"1": {"attempts": 3}}}

        result = analyze_contacts(
            contacts=contacts,
            state=state,
            email_column="email",
            required_fields=["name"],
            max_attempts_per_row=3,
            retry_exhausted=False,
        )

        self.assertEqual(["2"], [contact["__row_number"] for contact in result.eligible])
        self.assertEqual(["1"], [contact["__row_number"] for contact in result.exhausted_failures])


class EmailNormalizationTests(unittest.TestCase):
    def test_idn_domain_is_punycoded_without_requiring_smtputf8(self) -> None:
        self.assertEqual("hallo@xn--supermarch-k7a.de", normalize_email("HALLO@supermarché.de"))
        self.assertFalse(email_requires_smtputf8("HALLO@supermarché.de"))

    def test_non_ascii_local_part_requires_smtputf8(self) -> None:
        self.assertTrue(email_requires_smtputf8("żółw@example.com"))


class StatePersistenceTests(unittest.TestCase):
    def test_save_state_leaves_valid_json_and_no_temp_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_file = Path(temp_dir) / "campaign.json"
            state = {
                "version": 1,
                "source_id": "demo",
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "sent_rows": {"1": {"email": "hello@example.com"}},
                "failed_rows": {},
            }

            save_state(state_file, state)

            self.assertTrue(state_file.exists())
            loaded = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual("demo", loaded["source_id"])
            leftovers = list(Path(temp_dir).glob("campaign.json.*.tmp"))
            self.assertEqual([], leftovers)


class CampaignLockTests(unittest.TestCase):
    def test_second_process_cannot_acquire_campaign_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_file = Path(temp_dir) / "campaign.json"
            lock = acquire_campaign_lock_nowait(state_file)
            try:
                child_code = """
from pathlib import Path
import sys
from bulk_mailer import CampaignLockError, acquire_campaign_lock_nowait, release_campaign_lock

try:
    lock = acquire_campaign_lock_nowait(Path(sys.argv[1]))
except CampaignLockError:
    print("locked")
    raise SystemExit(2)
else:
    release_campaign_lock(lock)
    print("acquired")
"""
                result = subprocess.run(
                    [sys.executable, "-c", child_code, str(state_file)],
                    capture_output=True,
                    text=True,
                    check=False,
                    cwd=Path(__file__).resolve().parents[1],
                )
            finally:
                release_campaign_lock(lock)

        self.assertEqual(2, result.returncode)
        self.assertIn("locked", result.stdout)


if __name__ == "__main__":
    unittest.main()
