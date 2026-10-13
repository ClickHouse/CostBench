#!/usr/bin/env python3
"""Convert saved SQL payloads to sanitized CSVs without contacting ClickHouse.

Save each retained query's FORMAT TSVRaw result as <source-key>.json in an
input directory. Source keys and SQL paths are listed in source-manifest.json.
"""
from pathlib import Path
import argparse
import csv
import json


def convert(input_dir, output_dir):
    here = Path(__file__).resolve().parent
    manifest = json.loads((here / 'source-manifest.json').read_text())
    payloads = {key: json.loads((input_dir / (key + '.json')).read_text())
                for key in manifest['sources']}
    # The full export set supplies both node identities, including short names.
    hosts = sorted({row[1] for row in payloads['memory-node-series']})
    if len(hosts) != 2:
        raise ValueError('Expected both writer nodes in memory-node-series.json')
    aliases = {host: 'writer-node-' + str(index + 1) for index, host in enumerate(hosts)}
    for host, alias in list(aliases.items()):
        aliases[host.split('.')[0]] = alias

    def sanitize(value):
        if isinstance(value, str):
            for host, alias in sorted(aliases.items(), key=lambda pair: -len(pair[0])):
                value = value.replace(host, alias)
            return value
        if isinstance(value, list):
            return [sanitize(item) for item in value]
        return value

    output_dir.mkdir(parents=True, exist_ok=True)
    for key, source in manifest['sources'].items():
        value = sanitize(payloads[key])
        removed = source['omitted_column_indexes']
        if removed:
            value = [[cell for index, cell in enumerate(row) if index not in removed]
                     for row in value]
        blocks = value if source['shape'] == 'sections' else [[value]] if source['shape'] == 'single_row' else [value]
        if len(blocks) != len(source['tables']):
            raise ValueError('Unexpected result sections: ' + key)
        for rows, table in zip(blocks, source['tables']):
            path = output_dir / Path(table['path']).name
            with path.open('w', newline='') as stream:
                writer = csv.writer(stream, lineterminator="\n", quoting=csv.QUOTE_ALL)
                writer.writerow(table['columns'])
                for row in rows:
                    if len(row) != len(table['columns']):
                        raise ValueError('Unexpected result columns: ' + key)
                    writer.writerow([cell if kind == 'string' else json.dumps(cell, separators=(',', ':'), ensure_ascii=False)
                                     for cell, kind in zip(row, table['cell_encoding'])])
    print('Converted ' + str(len(payloads)) + ' SQL payloads to CSV.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True,
                        help='Use a fresh directory outside the committed results.')
    args = parser.parse_args()
    here = Path(__file__).resolve().parent
    if args.output_dir.resolve() == (here / 'results').resolve():
        parser.error('Choose a separate directory for re-extraction comparisons.')
    convert(args.input_dir, args.output_dir)


if __name__ == '__main__':
    main()
