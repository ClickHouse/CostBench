"""Offline CostBench pricing helpers; no provider credentials required."""
from __future__ import annotations
import hashlib,json,math
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
def read(path): return json.loads(Path(path).read_text())
def records(path): return [json.loads(s) for s in Path(path).read_text().splitlines() if s.strip()]
def path_name(path):
    path=Path(path).resolve()
    try: return path.relative_to(ROOT).as_posix()
    except ValueError: return str(path)
def provenance(path):
    p=Path(path)
    return {"path":path_name(p),"sha256":hashlib.sha256(p.read_bytes()).hexdigest()}
def save(path,data):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(path.suffix+".tmp")
    tmp.write_text(json.dumps(data,indent=2,allow_nan=False)+"\n");tmp.replace(path)
def number(value,label):
    if isinstance(value,bool): raise ValueError(f"boolean {label}")
    v=float(value)
    if not math.isfinite(v) or v<0: raise ValueError(f"invalid {label}: {value}")
    return v
def price(path,cloud,region,plan):
    found=[p for p in read(path)["pricing"] if (p["cloud"],p["region"],p["plan"])==(cloud,region,plan)]
    if len(found)!=1: raise ValueError(f"expected one pricing block for {cloud}/{region}/{plan}")
    return found[0]
