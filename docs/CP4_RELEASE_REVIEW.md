# Project 07 CP4 — release review and evidence declaration

## Verified prior to this release preparation

- Original locked baseline: commit `f2866b49408f61362d7398ce9e6e96df986802e0`, 20 passing tests.
- CP2 local Ubuntu/Docker: 47/47 tests, nine dbt models, 24 dbt tests, 12/12 warehouse checks, 50 reconciled synthetic records, repeat run with zero new writes.
- CP3 evidence ZIP: `Project07_CP3_Evidence_20261027_002228_UTC.zip` — 67/67 Python tests, 12/12 negative synthetic input scenarios, read-only PostgreSQL identity parity, Docker initial and replay PASS, 15/15 evidence hashes verified.
- Eight visual summary PNGs are derived from the saved evidence; they are not independent console captures. `README_EVIDENCE_IMAGES.txt` labels them explicitly.

## Release procedure

1. Run the CP4 installer **without** `-Stage`. It saves the complete updated README, `.gitignore`, this document, offline CI workflow, and staged-index auditor to your separate CP2 working copy. It verifies all eight screenshot hashes.
2. Read the README, screenshots and source. Review any displayed usernames/paths or unintended data before committing.
3. Run the installer again with `-Stage`. It stages **only** allowlisted upgrade files and audits the actual **Git index blobs** for sensitive filenames/content, file sizes and extra staged paths. Review the manifest and terminal report.
4. Send the staged-file summary and audit results for independent prepublication review. Do **not** commit or push before explicit approval.
5. When publication is approved, verify `origin` points to the intended GitHub URL, not the original local clone. Check upstream SHA before push. Never use `git push --force` or modify the original checkout.
6. After the push, inspect the GitHub Actions jobs on the published SHA. The new CI workflow separates Ubuntu full offline Python/Airflow checks from Windows tests that exclude Linux-only Airflow DAG imports. CI is **not a substitute** for the verified local Docker/dbt integration.

## Deployment / professional-experience limitations

All inputs are synthetic. The warehouse is a local Docker/dbt showcase; database password defaults are for local development only, with Postgres bound to `127.0.0.1`. This is **not** a paid cloud deployment or a customer system. Do not publish real `.env`, tokens, employer data, HR/I-9/payroll records, raw local logs, `warehouse/artifacts`, Docker volumes, CP patch directories or unreleased files. No live automated DAG scheduling has been verified. SQL work here is portfolio practice, not paid professional SQL experience.

## Verification log

- CP4 screenshot integrity: **pending your Windows installer run**.
- CP4 staged Git-index audit: **pending your Windows `-Stage` run**.
- GitHub publication: **not performed**.
- GitHub Actions: **not yet run on release commit**.
