"""Collect CP2 local Windows/WSL quality evidence with no credentials included."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'warehouse' / 'artifacts'


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--skip-docker', action='store_true', help='Run offline pytest only if Docker is unavailable')
    args = parser.parse_args()
    stamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_UTC')
    evidence = OUTPUT / ('cp2_evidence_' + stamp)
    evidence.mkdir(parents=True, exist_ok=False)
    summary = {'timestamp': stamp, 'steps': {}, 'baseline_commit': 'f2866b49408f61362d7398ce9e6e96df986802e0'}
    secret = os.environ.get('P07_DB_PASSWORD', '')

    def run(label: str, command: list[str], env=None, timeout=1800):
        print('RUN:', label, flush=True)
        try:
            result = subprocess.run(command, cwd=ROOT, env=env, text=True,
                                    capture_output=True, timeout=timeout, check=False)
            out = (result.stdout + '\n' + result.stderr)
            if secret and secret != 'portfolio_dev_only':
                out = out.replace(secret, '<redacted>')
            (evidence / (label + '.log')).write_text(out, encoding='utf-8')
            summary['steps'][label] = result.returncode
            print(f'{label}: exit={result.returncode}', flush=True)
        except (OSError, subprocess.TimeoutExpired) as exc:
            (evidence / (label + '.log')).write_text(type(exc).__name__ + ': ' + str(exc), encoding='utf-8')
            summary['steps'][label] = -1
            print(f'{label}: ERROR', flush=True)
        return summary['steps'][label] == 0

    with tempfile.TemporaryDirectory(prefix='p07_airflow_cp2_') as airflow_home:
        env = os.environ.copy()
        env['AIRFLOW_HOME'] = airflow_home
        bin_dir = Path(sys.executable).parent
        airflow = bin_dir / 'airflow'
        if not airflow.is_file():
            print('Airflow missing in selected Python environment; see README_FIRST.txt')
            summary['steps']['airflow_db_migrate'] = -1
        else:
            run('airflow_db_migrate', [str(airflow), 'db', 'migrate'], env=env, timeout=120)
        if summary['steps'].get('airflow_db_migrate') == 0:
            run('pytest_all', [sys.executable, '-m', 'pytest', '-o', 'addopts=', '-q'], env=env, timeout=240)
        else:
            run('pytest_cp2_only', [sys.executable, '-m', 'pytest', '-o', 'addopts=', '-q',
                                   'tests/test_cp2_warehouse_contracts.py'], env=env, timeout=60)

    if not args.skip_docker:
        ok = run('docker_initial', ['bash', 'warehouse/run_pipeline.sh'], timeout=2400)
        if ok:
            load = OUTPUT / 'load_manifest.json'
            quality = OUTPUT / 'quality_report.json'
            for name, file in [('first_load_manifest.json', load), ('first_quality_report.json', quality)]:
                if file.exists():
                    (evidence / name).write_bytes(file.read_bytes())
            run('docker_idempotent_replay', ['bash', 'warehouse/run_pipeline.sh'], timeout=2400)
            for name, file in [('replay_load_manifest.json', load), ('replay_quality_report.json', quality)]:
                if file.exists():
                    (evidence / name).write_bytes(file.read_bytes())
            try:
                replay = json.loads((evidence / 'replay_load_manifest.json').read_text())
                summary['idempotent_replay'] = (replay['source_changed_rows'] == 0 and
                                                replay['outcome_changed_rows'] == 0)
            except (FileNotFoundError, ValueError, KeyError):
                summary['idempotent_replay'] = False

    summary['status'] = ('PASS' if all(v == 0 for v in summary['steps'].values()) and
                         (args.skip_docker or summary.get('idempotent_replay', False)) else 'INCOMPLETE_OR_FAIL')
    (evidence / 'summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n')
    zpath = OUTPUT / f'Project07_CP2_Evidence_{stamp}.zip'
    with zipfile.ZipFile(zpath, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for f in sorted(evidence.iterdir()):
            archive.write(f, arcname=f.name)
    print(json.dumps(summary, indent=2), flush=True)
    print('EVIDENCE ZIP:', zpath, flush=True)
    return 0 if summary['status'] == 'PASS' else 2


if __name__ == '__main__':
    raise SystemExit(main())
