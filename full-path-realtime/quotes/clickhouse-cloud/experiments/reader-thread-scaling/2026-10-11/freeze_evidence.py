#!/usr/bin/env python3
"""Freeze exact measured SQL and hashes after collection, analysis and reporting."""
import hashlib
import json
import pathlib
from run_test import queries, workload_sql

ROOT = pathlib.Path(__file__).resolve().parent
definitions = queries()
for path in (ROOT / 'raw').glob('*.jsonl'):
    for line in path.read_text().splitlines():
        row = json.loads(line)
        if row['phase'] != 'measured':
            continue
        sql = workload_sql(definitions[row['query']], row['max_threads'], row['run_id'])
        assert hashlib.sha256(sql.encode()).hexdigest() == row['sql_sha256']
        target = ROOT / 'sql' / f"measured-trial-{row['query']}-t{row['max_threads']}.sql"
        target.write_text(sql + '\n')
entries = []
for path in sorted(ROOT.rglob('*')):
    rel = path.relative_to(ROOT)
    if not path.is_file() or path.name == 'SHA256SUMS' or any(p in {'__pycache__', '.venv', '.DS_Store'} for p in rel.parts):
        continue
    entries.append(hashlib.sha256(path.read_bytes()).hexdigest() + '  ' + str(rel))
(ROOT / 'SHA256SUMS').write_text('\n'.join(entries) + '\n')
print('Verified exact measured SQL; froze', len(entries), 'file hashes.')
