"""Verify exact staged Git blobs before CP4 release. Never reveal secret values."""
from __future__ import annotations
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

DENY_NAMES={'.env','credentials.json','secrets.json','id_rsa','id_ed25519'}
DENY_SUFFIXES={'.pem','.p12','.pfx','.key','.xlsx','.xls','.docx','.zip','.sqlite','.db'}
DENY_PATH_FRAGMENTS=('warehouse/artifacts/','cp1_recovery/','cp2_patch/','cp3_patch/','cp3_import_repair/','cp4_release_patch/','payload/','__pycache__/','.venv/')
TEXT_EXTENSIONS={'.md','.py','.yml','.yaml','.txt','.json','.sql','.sh','.ps1','.toml','.html','.js','.css','.example','.ini','.csv'}
TOKEN=re.compile(rb'\bsk-(?:proj-)?[A-Za-z0-9_-]{24,}')
ASSIGN=re.compile(rb'(?im)^\s*(?:export\s+)?(?:OPENAI_API_KEY|AWS_SECRET_ACCESS_KEY|GITHUB_TOKEN|GH_TOKEN)\s*=\s*["\']?(?!\s*(?:\$|<|replace|example|changeme|your_))[A-Za-z0-9_./+\-=]{16,}')
PRIVATE_KEY=re.compile(rb'-----BEGIN (?:RSA |OPENSSH |EC |DSA )?PRIVATE KEY-----')
MAX_BLOB=5_000_000

def git(root,*args):
    return subprocess.run(['git',*args],cwd=root,stdout=subprocess.PIPE,stderr=subprocess.PIPE)

def audit(repo:Path,manifest:Path):
    allowed={x.strip() for x in manifest.read_text(encoding='utf-8').splitlines() if x.strip() and not x.startswith('#')}
    staged=git(repo,'diff','--cached','--name-only','-z')
    tracked=git(repo,'ls-files','-z','--cached')
    findings=[]
    if staged.returncode or tracked.returncode:
        return {'status':'BLOCK','error':'Unable to enumerate Git index'}
    changed=[b.decode('utf-8','replace') for b in staged.stdout.split(b'\0') if b]
    files=[b.decode('utf-8','replace') for b in tracked.stdout.split(b'\0') if b]
    unexpected=sorted(set(changed)-allowed)
    for p in unexpected: findings.append({'file':p,'reason':'unexpected_staged_path'})
    missing=sorted(allowed-set(changed))
    # Files previously identical to HEAD may not show as changed; audit does not require diff for them.
    scan_count=0
    for p in files:
        low=p.lower().replace('\\','/')
        parts=Path(low).parts
        name=Path(low).name
        if (name in DENY_NAMES or Path(name).suffix in DENY_SUFFIXES or any(segment in low for segment in DENY_PATH_FRAGMENTS)):
            findings.append({'file':p,'reason':'blocked_filename_or_directory'}); continue
        item=git(repo,'cat-file','blob',':'+p)
        if item.returncode:
            findings.append({'file':p,'reason':'cannot_read_index_blob'}); continue
        body=item.stdout
        scan_count+=1
        if len(body)>MAX_BLOB:
            findings.append({'file':p,'reason':'oversized_blob'}); continue
        if Path(low).suffix in TEXT_EXTENSIONS or name in ('.env.example','.gitignore','dockerfile'):
            if TOKEN.search(body) or ASSIGN.search(body) or PRIVATE_KEY.search(body):
                findings.append({'file':p,'reason':'possible_secret_content'})
    status='PASS' if not findings and changed else 'BLOCK'
    return {'status':status,'tracked_index_files_scanned':scan_count,'staged_changed_files':len(changed),'staged_paths':changed,'findings':findings,'note':'Scans Git INDEX blobs and filename rules; heuristic, manually inspect images and logs. No credential values disclosed.'}

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--repo',type=Path,required=True)
    parser.add_argument('--manifest',type=Path,required=True)
    args=parser.parse_args()
    report=audit(args.repo.resolve(),args.manifest.resolve())
    print(json.dumps(report,indent=2,sort_keys=True))
    return 0 if report['status']=='PASS' else 2

if __name__=='__main__': raise SystemExit(main())
