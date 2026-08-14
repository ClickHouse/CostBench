# ops/

`cloudwatch_msk_throughput.json` — CloudWatch `get-metric-data` query summing, across all brokers:

* `MessagesInPerSec` → **EPS** into MSK
* `BytesInPerSec` (Topic=quotes) → **compressed** bytes/s on the wire

Both are DEFAULT-level (free) metrics. `BytesInPerSec` only starts emitting once a topic has
received data. This is how the ~28.5 MB/s compressed ingress at ~999K EPS was measured — the number
that showed the AWS MSK sizing sheet's 9-broker recommendation assumed *uncompressed* throughput.

```bash
aws cloudwatch get-metric-data --region eu-west-2 \
  --start-time "$(date -u -v-15M +%Y-%m-%dT%H:%M:%SZ)" \
  --end-time   "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --metric-data-queries file://ops/cloudwatch_msk_throughput.json --output json
```
