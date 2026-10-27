"""CP3 release-candidate file audit. Never emit detected secret values."""
from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

BAD_NAMES = {'.env', 'id_rsa', 'id_ed25519', 'credentials.json', 'secrets.json'}
PRIVATE_EXTENSIONS = ('.pem', '.p12', '.pfx', '.key')
SENSITIVE_ASSIGNMENT = re.compile(
    r'(?im)^\s*(?:export\s+)?(?:OPENAI_API_KEY|AWS_SECRET_ACCESS_KEY|GITHUB_TOKEN|GH_TOKEN)\s*=\s*["\']?(?!\s*(?:\$|<|replace|example|changeme|your_))[A-Za-z0-9_./+\-=]{16,}'
)
TOKEN_PATTERN = re.compile(r'\bsk-(?:proj-)?[A-Za-z0-9_-]{24,}')
CP3_FILES = (
    'warehouse/cp3_checks.py', 'warehouse/cp3_security.py',
    'scripts/project07_cp3_evidence.py', 'tests/test_cp3_warehouse_quality.py',
    'docs/CP3_QUALITY_RECOVERY.md',
)


def audit_paths(paths: list[str], root: Path, *, include_content: bool = True) -> dict:
    findings = []
    checked = 0
    for rel in sorted(set(paths)):
        path = root / rel
        name = path.name.lower()
        if name in BAD_NAMES or name.endswith(PRIVATE_EXTENSIONS):
            findings.append({'file':rel,'reason':'sensitive_filename'})
            continue
        if not path.is_file():
            findings.append({'file':rel,'reason':'missing_file'})
            continue
        checked += 1
        if not include_content or path.stat().st_size > 1_000_000 or path.suffix.lower() not in ('.py','.md','.sql','.ps1','.sh','.txt','.yml','.yaml','.toml','.json'):
            continue
        body = path.read_text(encoding='utf-8', errors='replace')
        if SENSITIVE_ASSIGNMENT.search(body) or TOKEN_PATTERN.search(body):
            findings.append({'file':rel,'reason':'possible_secret_pattern'})
    return {'status':'PASS' if not findings else 'BLOCK','inspected_files':checked,'findings':findings,
            'explanation':'No file contents or secret values included; heuristic scan is not proof of absence.'}


def git_tracked_paths(root: Path) -> list[str]:
    proc = subprocess.run(['git','ls-files','-z','--cached'],cwd=root,capture_output=True,check=False)
    if proc.returncode:
        raise RuntimeError('Cannot enumerate tracked Git files')
    return [x.decode('utf-8','replace') for x in proc.stdout.split(b'\x00') if x]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--out',type=Path,required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    try:
        files = git_tracked_paths(root)
        report = audit_paths(files + list(CP3_FILES),root)
        report['tracked_files'] = len(files)
        report['scope'] = 'git tracked files plus five new CP3 files; ignored untracked files excluded'
    except (OSError,RuntimeError) as exc:
        report = {'status':'BLOCK','error_type':type(exc).__name__}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    print(json.dumps(report,indent=2,sort_keys=True))
    return 0 if report['status']=='PASS' else 2


if __name__ == '__main__':
    raise SystemExit(main())
