#!/bin/bash
# =============================================================================
# Stop a running Redshift T2 run (producer + controller + read-runners).
#
# WHY A SCRIPT: three traps burned us doing this by hand.
#   1. `pkill -f produce_quotes.py` also matches the SSH command string that contains the pattern,
#      so it kills its own shell before doing the work. This resolves PIDs first, then signals them.
#   2. The producer's multiprocessing forkserver children survive the parent and keep producing;
#      they must be signalled explicitly (they get reparented, so pgrep -P on the dead parent is
#      useless).
#   3. `( sleep N; cmd ) &` — killing the sleep makes the subshell PROCEED to cmd. Kill the subshell.
#
# IMPORTANT: this does NOT stop Redshift ingesting. quotes_streamed has AUTO REFRESH YES, so its
# background refresh keeps draining the topic server-side and keeps billing the writer. To actually
# halt ingest either drop the MV (reset_run.py does) or:
#     ALTER MATERIALIZED VIEW quotes_streamed AUTO REFRESH NO;
#
#   bash stop_run.sh
# =============================================================================
PAT='produce_quotes\.py|monitor_lag\.py|runner_redshift\.py'

pids() {  # resolve first; exclude this script, its shell, and grep itself
  ps -eo pid,args --no-headers \
    | grep -E "$PAT|sleep 1800" \
    | grep -v "bash -c" | grep -v "stop_run.sh" | grep -v grep \
    | awk '{print $1}'
}

n=$(pids | wc -l | tr -d ' ')
echo "found $n process(es) to stop"
[ "$n" -eq 0 ] && exit 0

pids | xargs -r kill -TERM 2>/dev/null
sleep 8

left=$(pids | wc -l | tr -d ' ')
if [ "$left" -gt 0 ]; then
  echo "escalating to KILL for $left survivor(s) (usually forkserver children)"
  pids | xargs -r kill -KILL 2>/dev/null
  sleep 3
fi

echo "remaining: $(pids | wc -l | tr -d ' ')"
echo "NOTE: quotes_streamed AUTO REFRESH is still consuming from MSK server-side (see header)."
