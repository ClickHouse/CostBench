#!/usr/bin/env python3
"""Refresh the pre-run context when reproducing in a new empty evidence folder."""
import pathlib
from connection import request

ROOT = pathlib.Path(__file__).resolve().parent
for name in ['01-connection', '02-settings', '03-tables', '04-columns',
             '05-parts-before', '06-capacity', '07-background-before',
             '08-indexes', '09-event-semantics', '10-log-fields']:
    response = request((ROOT / 'sql' / (name + '.sql')).read_text())
    (ROOT / 'results' / (name + '.csv')).write_text(response['body'])
    print(name, len(response['body']), 'bytes')
