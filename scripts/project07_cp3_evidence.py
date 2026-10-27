"""Project 07 CP3 WSL evidence runner: fail-closed, synthetic-only, non-destructive probes.

Uses the EXISTING local CP2 PostgreSQL Docker Compose stack and temporary Airflow DB.
Does not delete containers or Docker volumes, modify baseline SQL, or push to GitHub.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# Direct file execution sets sys.path[0] to scripts/, not the repository root.
# Include the root so the local, read-only warehouse helpers are importable
# whether invoked as a script or a Python module, from any working directory.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from warehouse.cp3_checks import compare_replay

ARTIFACTS = ROOT / 'warehouse' / 'artifacts'
BASELINE = 'f2866b49408f61362d7398ce9e6e96df986802e0'


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description='Project07 CP3 quality, recovery and read-only DB evidence')
    parser.add_argument('--offline-only', action='store_true',
                        help='Run tests and negative fixtures without Docker; cannot claim CP3 PASS')
    args = parser.parse_args()
    stamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_UTC')
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    directory = ARTIFACTS / f'cp3_quality_{stamp}'
    directory.mkdir(exist_ok=False)
    result = {'checkpoint':'CP3','baseline_commit': BASELINE, 'timestamp_utc':stamp,
              'steps': {}, 'docker_integration_verified':False}
    secrets = [os.environ.get('P07_DB_PASSWORD', '')]
    secrets = [x for x in secrets if x and x != 'portfolio_dev_only']

    def run(label: str, command: list[str], *, env=None, timeout=900) -> bool:
        print('RUN:',label,flush=True)
        try:
            proc = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True,
                                  timeout=timeout, check=False, errors='replace')
            message = proc.stdout + '\n' + proc.stderr
            exit_code = proc.returncode
        except (subprocess.TimeoutExpired, OSError) as error:
            message = f'{type(error).__name__}; command could not complete within {timeout} seconds.\n'
            exit_code = -1
        for value in secrets:
            message = message.replace(value, '<REDACTED>')
        (directory / f'{label}.log').write_text(message, encoding='utf-8')
        result['steps'][label] = exit_code
        print(label + ': exit=' + str(exit_code),flush=True)
        return exit_code == 0

    def copy_output(name: str, dest: str) -> bool:
        file = ARTIFACTS / name
        if file.is_file():
            shutil.copy2(file, directory / dest)
            return True
        return False

    try:
        with tempfile.TemporaryDirectory(prefix='p07_cp3_airflow_') as tmp_home:
            env = os.environ.copy()
            env['AIRFLOW_HOME'] = tmp_home
            airflow = Path(sys.executable).parent / 'airflow'
            if airflow.is_file():
                migrated = run('airflow_migration', [str(airflow), 'db', 'migrate'], env=env, timeout=180)
            else:
                migrated = False
                result['steps']['airflow_migration'] = -1
                (directory / 'airflow_migration.log').write_text('Airflow executable unavailable in Python environment.')
            if migrated:
                run('full_pytest', [sys.executable,'-m','pytest','-o','addopts=','-q'], env=env, timeout=900)
            else:
                result['steps']['full_pytest'] = -1
        run('negative_contracts', [sys.executable,'warehouse/cp3_checks.py','--mode','negative',
                                   '--out',str(directory/'negative_contracts.json')], timeout=120)
        run('source_security', [sys.executable,'warehouse/cp3_security.py','--root',str(ROOT),
                                '--out',str(directory/'source_security.json')], timeout=120)
        if not args.offline_only:
            docker = ['bash','warehouse/run_pipeline.sh']
            first_ok = run('docker_initial', docker, timeout=2400)
            first_captured = first_ok and copy_output('load_manifest.json','first_load_manifest.json') \
                                       and copy_output('quality_report.json','first_quality_report.json')
            if first_ok:
                run('docker_replay', docker, timeout=2400)
                second_captured = copy_output('load_manifest.json','replay_load_manifest.json') \
                             and copy_output('quality_report.json','replay_quality_report.json')
            else:
                second_captured = False
                result['steps']['docker_replay'] = -1
            if first_captured and second_captured and result['steps']['docker_replay'] == 0:
                # Stable model metrics and rerun idempotency must agree exactly.
                replay = compare_replay(directory/'first_quality_report.json',
                                        directory/'replay_quality_report.json',
                                        directory/'replay_load_manifest.json')
                (directory/'replay_check.json').write_text(json.dumps(replay,indent=2,sort_keys=True)+'\n')
                result['steps']['replay_check'] = 0 if replay['status'] == 'PASS' else 2
            else:
                result['steps']['replay_check'] = -1
            if first_ok and result['steps'].get('docker_replay') == 0:
                ok = run('database_identity', [
                    'docker', 'compose', '-f', 'warehouse/docker-compose.yml',
                    'run', '--rm', '-T', 'dbt', 'python', '/workspace/warehouse/cp3_checks.py',
                    '--mode','database','--out','/out/cp3_identity.json'],timeout=600)
                if ok and not copy_output('cp3_identity.json','cp3_identity.json'):
                    result['steps']['database_identity'] = -1
            else:
                result['steps']['database_identity'] = -1
            result['docker_integration_verified'] = (result['steps'].get('docker_initial') == 0 and
                result['steps'].get('docker_replay') == 0 and
                result['steps'].get('replay_check') == 0 and
                result['steps'].get('database_identity') == 0)
        else:
            result['steps']['docker_initial'] = -1
            result['steps']['docker_replay'] = -1
            result['steps']['database_identity'] = -1
    except Exception as exc:
        # Never leak exception args, since they can expose environment values.
        result['error_type'] = type(exc).__name__

    result['status'] = ('PASS' if result['steps'] and all(x == 0 for x in result['steps'].values())
                        and result['docker_integration_verified'] else 'INCOMPLETE_OR_FAIL')
    files = {p.name: sha256(p) for p in sorted(directory.iterdir()) if p.is_file()}
    (directory / 'EVIDENCE_SHA256.json').write_text(json.dumps(files,indent=2,sort_keys=True)+'\n')
    (directory / 'summary.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
    archive = ARTIFACTS/f'Project07_CP3_Evidence_{stamp}.zip'
    with zipfile.ZipFile(archive,'w',compression=zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(directory.iterdir()):
            if f.is_file():
                zf.write(f,arcname=f.name)
    print(json.dumps(result,indent=2,sort_keys=True),flush=True)
    print('EVIDENCE ZIP:',archive,flush=True)
    return 0 if result['status'] == 'PASS' else 2


if __name__ == '__main__':
    raise SystemExit(main())
