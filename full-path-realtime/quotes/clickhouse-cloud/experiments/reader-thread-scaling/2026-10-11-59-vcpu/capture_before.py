#!/usr/bin/env python3
"""Refresh the pre-run context when reproducing in a new empty evidence folder."""
import csv
import pathlib
from connection import request

ROOT = pathlib.Path(__file__).resolve().parent
assert not list((ROOT / 'raw').glob('*.jsonl')), 'Use a new directory for a live rerun'
for name in ['00-identity-before', '01-connection', '02-settings', '03-tables', '04-columns',
             '05-parts-before', '06-capacity', '07-background-before',
             '08-indexes', '09-event-semantics', '10-log-fields']:
    response = request((ROOT / 'sql' / (name + '.sql')).read_text())
    (ROOT / 'results' / (name + '.csv')).write_text(response['body'])
    print(name, len(response['body']), 'bytes')
identity = list(csv.DictReader((ROOT / 'results/00-identity-before.csv').open()))[0]
assert float(identity['vCPUs']) == 59, 'This design requires a 59-vCPU service'
assert float(identity['memory_bytes']) == 236 * 2**30, 'Unexpected service memory'
tables = list(csv.DictReader((ROOT / 'results/03-tables.csv').open()))
raw_table = next(r for r in tables if r['database'] == 'quotes_async_t4' and r['name'] == 'quotes')
assert int(raw_table['total_rows']) == 113217743918, 'Dataset row count differs from this design'
