# Support

## Before Opening an Issue

- Read [README.md](README.md) for setup and validation commands.
- Confirm you are running the app locally and not from a public network exposure by default.
- Re-run:

```bash
python3 -m py_compile bulk_mailer.py web_mailer_app.py
python3 -m unittest discover -s tests -v
```

## Good Issue Reports Include

- your Python version
- the command you ran
- whether you used the web UI or CLI
- the exact error message
- whether the problem reproduces with `contacts.example.csv` and the starter templates

## Security Issues

Do not file public issues for vulnerabilities. Use [SECURITY.md](SECURITY.md).
