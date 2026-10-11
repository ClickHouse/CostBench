#!/usr/bin/env python3
"""Create or verify the experiment's file hashes; never include ignored caches."""
import argparse, hashlib, pathlib
ROOT=pathlib.Path(__file__).resolve().parent
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
    parser=argparse.ArgumentParser();parser.add_argument('--check',action='store_true');args=parser.parse_args();target=ROOT/'FILES.sha256'
    if args.check:
        rows=[line.split('  ',1)for line in target.read_text().splitlines()]
        for h,name in rows:assert digest(ROOT/name)==h,name
        print('Verified',len(rows),'files.');return
    files=sorted(p for p in ROOT.rglob('*')if p.is_file() and p.name!='FILES.sha256' and '__pycache__'not in p.parts and p.suffix!='.pyc')
    target.write_text(''.join(digest(p)+'  '+str(p.relative_to(ROOT))+'\n'for p in files));print('Hashed',len(files),'files.')
if __name__=='__main__':main()
