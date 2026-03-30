# Contributing

## Before You Start

- Keep the project local-first. Do not introduce cloud dependencies for the core send flow.
- Do not commit mailbox credentials, `.env`, `storage/`, `state/`, or exported logs.
- Keep the default web app binding on `127.0.0.1` unless a change is explicitly about controlled remote access.

## Local Setup

```bash
cp .env.example .env
python3 -m py_compile bulk_mailer.py web_mailer_app.py
python3 -m unittest discover -s tests -v
```

Run the web UI locally:

```bash
python3 web_mailer_app.py --open-browser
```

## Change Guidelines

- Prefer small pull requests with one clear purpose.
- Add or update tests when changing send logic, state handling, preview sanitization, or web API behavior.
- Keep the web UI compatible with local use on current desktop browsers.
- Preserve the repository's sanitized state for public GitHub publishing.

## Pull Requests

Before opening a pull request:

- Run `python3 -m py_compile bulk_mailer.py web_mailer_app.py`.
- Run `python3 -m unittest discover -s tests -v`.
- Confirm no secrets or local runtime files are staged.
- Summarize user-visible changes and any risk areas in the PR description.
