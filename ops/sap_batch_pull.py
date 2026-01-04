import os
import time
import requests
from urllib.parse import urljoin

DEFAULT_TIMEOUT_S = 15

def run(payload: dict):
    """
    payload:
      - base_url: "https://s4.example.com/sap/opu/odata/sap/"
      - service:  "ZMYZEL_BATCH_OBS_SRV"
      - entity:   "JobSet"   (or whatever your entity set is)
      - user:     optional (else env SAP_USER)
      - password: optional (else env SAP_PASS)
      - since_ts: unix epoch seconds (optional; default last 24h)
      - limit:    int (optional; default 200)
    """
    base_url = payload.get("base_url") or os.getenv("SAP_ODATA_BASE_URL")
    service  = payload.get("service")  or os.getenv("SAP_ODATA_SERVICE")
    entity   = payload.get("entity")   or os.getenv("SAP_ODATA_ENTITY", "JobSet")

    user = payload.get("user") or os.getenv("SAP_USER")
    pw   = payload.get("password") or os.getenv("SAP_PASS")

    if not base_url or not service or not user or not pw:
        return {"error": "Missing base_url/service/user/password (payload or env)"}

    now = int(time.time())
    since_ts = int(payload.get("since_ts") or (now - 24 * 3600))
    limit = int(payload.get("limit") or 200)

    # You control the CDS -> OData mapping, so pick your actual field names.
    # Example assumes your CDS exposes:
    #   JobName, JobCount, Status, StartTs, EndTs, DurationMs, SpoolId, SpoolBytes, SpoolTitle, Owner
    select_fields = payload.get("select") or [
        "JobName", "JobCount", "Status", "StartTs", "EndTs", "DurationMs",
        "SpoolId", "SpoolBytes", "SpoolTitle", "Owner"
    ]

    # OData v2 typically uses datetime'...' (system-dependent). If you expose StartTs as epoch/num, even better.
    # Here we assume StartTs is epoch seconds in your CDS view for simplicity.
    odata_filter = payload.get("filter") or f"StartTs ge {since_ts}"

    # Build URL: .../ZMYZEL_BATCH_OBS_SRV/JobSet?$select=...&$filter=...&$top=...
    service_root = urljoin(base_url if base_url.endswith("/") else base_url + "/", service + "/")
    entity_url = urljoin(service_root, entity)

    params = {
        "$select": ",".join(select_fields),
        "$filter": odata_filter,
        "$top": str(limit),
        "$format": "json",
    }

    try:
        r = requests.get(
            entity_url,
            params=params,
            auth=(user, pw),
            timeout=float(payload.get("timeout_s") or DEFAULT_TIMEOUT_S),
            headers={"Accept": "application/json"},
        )
        if r.status_code >= 400:
            return {
                "error": "SAP OData request failed",
                "status_code": r.status_code,
                "body_snippet": r.text[:500],
                "url": r.url,
            }

        data = r.json()

        # OData v2 JSON usually: {"d": {"results": [ ... ]}}
        rows = data.get("d", {}).get("results")
        if rows is None:
            # OData v4 style: {"value":[...]}
            rows = data.get("value", [])
        if not isinstance(rows, list):
            return {"error": "Unexpected OData JSON shape", "body_snippet": str(data)[:500]}

        events = []
        for row in rows:
            events.append({
                "source": "sap",
                "system": payload.get("system") or os.getenv("SAP_SYSTEM", "S4"),
                "object": "BATCH_JOB",
                "action": "OBSERVE",
                "ts": int(row.get("StartTs") or now),
                "payload": {
                    "job_name": row.get("JobName"),
                    "job_count": row.get("JobCount"),
                    "status": row.get("Status"),
                    "start_ts": row.get("StartTs"),
                    "end_ts": row.get("EndTs"),
                    "duration_ms": row.get("DurationMs"),
                    "spool_id": row.get("SpoolId"),
                    "spool_bytes": row.get("SpoolBytes"),
                    "spool_title": row.get("SpoolTitle"),
                    "owner": row.get("Owner"),
                }
            })

        return {
            "since_ts": since_ts,
            "fetched": len(events),
            "events": events,
            "odata_url": r.url,
        }

    except requests.RequestException as e:
        return {"error": f"SAP OData network error: {str(e)}"}
    except Exception as e:
        return {"error": f"SAP OData parse error: {str(e)}"}
