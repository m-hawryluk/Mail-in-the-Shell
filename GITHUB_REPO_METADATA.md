# GitHub Repository Metadata

## Suggested repository name

`mail-in-the-shell`

## Short description

Local-first batch email tool with a browser UI and Python CLI for SMTP previews, resumable sends, activity logging, and optional macOS Keychain-backed local credentials.

## Suggested topics

- `python`
- `smtp`
- `email`
- `bulk-email`
- `cli`
- `web-ui`
- `local-first`
- `imap`
- `sqlite`
- `automation`

## Suggested initial release title

`Initial public release`

## Suggested release summary

MAIL IN THE SHELL v.2.0 is a local-first mailer for controlled batch sends. It includes a browser-based operator UI, a scriptable Python CLI, preview and dry-run safeguards, resumable state tracking, optional IMAP sent-copy support, and local activity logging without bundling mailbox credentials in the repository.

## Pre-publish checklist

- Confirm `.env` is not present in the commit.
- Confirm `storage/` and `state/` are ignored and not staged.
- Run `python3 -m py_compile bulk_mailer.py web_mailer_app.py`.
- Run `python3 -m unittest discover -s tests -v`.
- Create the GitHub repository and paste the description above.
- Add the suggested topics in the GitHub repository settings.
