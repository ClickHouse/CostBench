#!/bin/bash
# Phased auto-stop (detached, survives logout, no cron/at daemon).
#   bash schedule_stop.sh <ingest_hours> [tail_hours]
#     ingest_hours : after this, stop INGEST + dashboard + drilldown (the costly parts;
#                    their warehouses then auto-suspend).
#     tail_hours   : keep the cheap metadata trackers (mv_latency + clustering_lag) running
#                    this many MORE hours, to capture the lag/clustering-depth COLLAPSE after
#                    ingest stops (AC sprints to catch up once the table stabilizes), THEN
#                    stop them. Omit -> stop everything at ingest_hours.
# Cancel: kill <pid from out/stop_schedule.log>
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench
ING="${1:?usage: bash schedule_stop.sh <ingest_hours> [tail_hours]}"
TAIL="${2:-}"
ISECS=$(python3 -c "print(int(float('$ING')*3600))")
mkdir -p out

if [ -z "$TAIL" ]; then
  setsid nohup bash -c "sleep ${ISECS}; cd /home/ubuntu/bench; \
    echo \"=== auto-stop ALL \$(date -u) ===\" >> out/stop.log; \
    bash stop_experiment.sh >> out/stop.log 2>&1" >/dev/null 2>&1 < /dev/null &
  PID=$!
  echo "armed: stop EVERYTHING in ${ING}h (pid ${PID})"
else
  TSECS=$(python3 -c "print(int(float('$TAIL')*3600))")
  setsid nohup bash -c "
    sleep ${ISECS}
    cd /home/ubuntu/bench
    echo \"=== phase1: stop ingest+readers \$(date -u) ===\" >> out/stop.log
    pkill -9 -f ingest.py;        echo '  stopped ingest'     >> out/stop.log
    pkill -9 -f run_dashboard.py; echo '  stopped dashboard'  >> out/stop.log
    pkill -9 -f run_drilldown.py; echo '  stopped drilldown'  >> out/stop.log
    echo '  trackers (mv_latency + clustering_lag) STILL RUNNING for tail' >> out/stop.log
    sleep ${TSECS}
    echo \"=== phase2: stop trackers \$(date -u) ===\" >> out/stop.log
    pkill -9 -f mv_latency;     echo '  stopped mv_latency'     >> out/stop.log
    pkill -9 -f clustering_lag; echo '  stopped clustering_lag' >> out/stop.log
  " >/dev/null 2>&1 < /dev/null &
  PID=$!
  echo "armed: stop ingest+readers in ${ING}h; trackers continue ${TAIL}h more, then stop (pid ${PID})"
fi
echo "$(date -u)  pid=${PID}  ingest=${ING}h tail=${TAIL:-0}h" >> out/stop_schedule.log
echo "Cancel with:  kill ${PID}"
