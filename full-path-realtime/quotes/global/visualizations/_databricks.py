"""Databricks availability is explicit; incomplete cost never becomes zero."""
import json,math
from _common import resolve_source,sha256
LABEL="Databricks Serverless SQL"
def load(manifest,kind):
    config=manifest.get("databricks_integration")
    if not config: return None
    path=resolve_source(config["fresh_path_summary" if kind=="fresh" else "full_path_summary"])
    payload=json.loads(path.read_text())
    value=payload["total_cost_usd"] if kind=="fresh" else next(r["total_cost"] for r in payload["rows"] if r["label"]==LABEL)
    if value is not None and (not math.isfinite(value) or value<0): raise ValueError("invalid Databricks cost")
    if value is None and payload.get("missing_components") != ["mv_refresh"]:
        raise ValueError("Only explicitly missing MV usage may be absent")
    if value is not None and payload.get("missing_components"):
        raise ValueError("Full-path cost cannot have a value while a component is missing")
    return {"payload":payload,"available":value is not None,"path":str(path),"sha256":sha256(path),"label":LABEL}
def unavailable_note(fig,entry,wide):
    if entry and not entry["available"]:
        fig.text(.5,.035,"Databricks Serverless SQL: full-path cost pending MV-refresh usage",ha="center",color="#FF3621",fontsize=12 if wide else 9)
