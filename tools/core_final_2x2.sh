#!/usr/bin/env bash
# core_final_2x2.sh - Final scenario in CORE: 2 servers x 2 streams, 5 clients.
#
# Runs on the HOST (not inside a node), as root, after pressing Start in CORE
# with topology/Cenario_final.xml. Uses CORE's vcmd to run ott.py in each node.
#
#   sudo bash tools/core_final_2x2.sh test [NODE]   # everything automatic (~75 s):
#                                                   # start, measure, kill NODE
#                                                   # (default n9), measure outage, stop
#   sudo bash tools/core_final_2x2.sh start         # only start all nodes
#   sudo bash tools/core_final_2x2.sh status        # frames/s of each client now
#   sudo bash tools/core_final_2x2.sh kill NODE     # crash one node (kill -9, no LEAVE)
#   sudo bash tools/core_final_2x2.sh stop          # stop everything
#
#   DRY=1 bash tools/core_final_2x2.sh start        # only print the commands
#
# Roles (checklist: "Decidir os papeis no cenario final"):
#   S1 = n16 (bootstrapper) serves n16/video1 = movie.Mjpeg,  n16/video2 = movie2.Mjpeg
#   S2 = n20                serves n20/video1 = movie2.Mjpeg, n20/video2 = movie.Mjpeg
#   n17 -> n16/video1   n18 -> n16/video2   n19 -> n20/video1
#   n22 -> n20/video2   n23 -> n16/video1 (same as n17: tests replication)
#   every other node in Mixed_testing/cenarioF.json just relays.
#
# Clients run with --no-gui (one log line per frame) so frames can be counted.
# Logs: tools/logs_2x2/<node>.log. If the routers have no routes
# ("Network is unreachable"), run tools/core_frr_ospf_fix.sh first.
# Needs streaming/videos/movie2.Mjpeg (python3 tools/make_video2.py).

set -u
CODE=$(cd "$(dirname "$0")/.." && pwd)
CONFIG="$CODE/Mixed_testing/cenarioF.json"
LOGS="$CODE/tools/logs_2x2"
# paths used INSIDE the nodes are relative to $CODE (the folder has spaces)
V1=streaming/videos/movie.Mjpeg
V2=streaming/videos/movie2.Mjpeg
QCODE=$(printf %q "$CODE")
BOOT_ID=n16
BOOT_ADDR=10.1.5.20:5000
declare -A SERVE=(
    [n16]="--serve video1=$V1 --serve video2=$V2"
    [n20]="--serve video1=$V2 --serve video2=$V1"
)
declare -A WATCH=( [n17]=n16/video1 [n18]=n16/video2 [n19]=n20/video1
                   [n22]=n20/video2 [n23]=n16/video1 )
CLIENTS="n17 n18 n19 n22 n23"
DRY=${DRY:-0}

# all node IDs from the JSON, bootstrapper first
NODES=$(python3 -c "
import json,sys; n=list(json.load(open(sys.argv[1]))['nodes'])
n.sort(key=lambda x: x != sys.argv[2]); print(' '.join(n))" "$CONFIG" "$BOOT_ID") || exit 1

SESSION=$(ls -dt /tmp/pycore.* 2>/dev/null | head -1)
if [ "$DRY" != 1 ]; then
    [ -n "$SESSION" ] || { echo "No running CORE session (/tmp/pycore.*). Press Start in CORE."; exit 1; }
    command -v vcmd >/dev/null || { echo "vcmd not found: run this on the CORE machine, as root"; exit 1; }
fi

in_node() {   # in_node NODE "shell command"
    if [ "$DRY" = 1 ]; then echo "[$1] $2"; return 0; fi
    [ -S "$SESSION/$1" ] || { echo "  $1: no such node in $SESSION"; return 1; }
    vcmd -c "$SESSION/$1" -- sh -c "$2"
}

frames() {    # frames NODE -> frames received so far
    local n; n=$(grep -c " frame " "$LOGS/$1.log" 2>/dev/null); echo "${n:-0}"
}

cmd_for() {
    local n=$1 c="python3 -u ott.py --id $n --port 5000"
    if [ "$n" = "$BOOT_ID" ]; then c="$c --config Mixed_testing/cenarioF.json"; else c="$c --bootstrapper $BOOT_ADDR"; fi
    [ -n "${SERVE[$n]:-}" ] && c="$c ${SERVE[$n]}"
    [ -n "${WATCH[$n]:-}" ] && c="$c --watch ${WATCH[$n]} --no-gui"
    echo "$c"
}

start() {
    [ -f "$CODE/$V2" ] || { echo "Missing $V2: run python3 tools/make_video2.py"; exit 1; }
    mkdir -p "$LOGS"
    for n in $NODES; do
        # stdin = /dev/null: the console sees EOF and the node keeps running in the background
        in_node "$n" "cd $QCODE && nohup $(cmd_for "$n") </dev/null >tools/logs_2x2/$n.log 2>&1 & echo \$! >tools/logs_2x2/$n.pid" \
            && echo "  started $n${WATCH[$n]:+  (watching ${WATCH[$n]})}${SERVE[$n]:+  (server)}"
        [ "$n" = "$BOOT_ID" ] && [ "$DRY" != 1 ] && sleep 1
    done
    echo "Logs in $LOGS"
}

stop() {
    for n in $NODES; do
        [ -f "$LOGS/$n.pid" ] || continue
        in_node "$n" "kill \$(cat $QCODE/tools/logs_2x2/$n.pid) 2>/dev/null"; rm -f "$LOGS/$n.pid"
    done
    echo "All nodes stopped."
}

kill_node() {
    [ -f "$LOGS/$1.pid" ] || { echo "$1 is not running"; return 1; }
    in_node "$1" "kill -9 \$(cat $QCODE/tools/logs_2x2/$1.pid)"; rm -f "$LOGS/$1.pid"
    echo "$(date +%T) killed $1 (crash, no LEAVE)"
}

status() {    # frames/s of each client over 2 s
    declare -A a
    for c in $CLIENTS; do a[$c]=$(frames "$c"); done
    sleep 2
    printf "%-5s %-11s %8s %8s\n" client stream "frames/s" total
    for c in $CLIENTS; do
        b=$(frames "$c")
        printf "%-5s %-11s %8d %8d\n" "$c" "${WATCH[$c]}" $(((b - a[$c]) / 2)) "$b"
    done
}

# Each client should only see its own stream: <server>/<number> frame ...
check_streams() {
    for c in $CLIENTS; do
        want="${WATCH[$c]%%/*}/${WATCH[$c]: -1}"
        seen=$(grep -oE "^[^ ]+/[0-9]+ frame" "$LOGS/$c.log" 2>/dev/null | cut -d' ' -f1 | sort -u | tr '\n' ' ')
        if [ "$seen" = "$want " ]; then echo "  OK    $c only received $want"
        else echo "  FAIL  $c wanted $want, received: ${seen:-nothing}"; fi
    done
}

test_all() {
    local victim=${1:-n9}
    echo "== 1. starting all nodes"; start
    [ "$DRY" = 1 ] && return
    echo "== 2. waiting 25 s for ANNOUNCE / ACTIVATE (routes take a few rounds)"; sleep 25
    echo "== 3. normal operation"; status; check_streams
    echo "   (routing-table prints for the report: run 'start' instead of 'test' and"
    echo "    use the 'routes' console command by hand - the script can't type into it)"

    echo "== 4. killing $victim, then watching each client second by second for 30 s"
    declare -A last gap maxgap
    for c in $CLIENTS; do last[$c]=$(frames "$c"); gap[$c]=0; maxgap[$c]=0; done
    kill_node "$victim"
    for _ in $(seq 30); do
        sleep 1
        for c in $CLIENTS; do
            now=$(frames "$c")
            if [ "$now" -eq "${last[$c]}" ]; then gap[$c]=$((gap[$c] + 1))
                [ "${gap[$c]}" -gt "${maxgap[$c]}" ] && maxgap[$c]=${gap[$c]}
            else gap[$c]=0; fi
            last[$c]=$now
        done
    done
    echo "longest time without video after killing $victim:"
    for c in $CLIENTS; do printf "  %-5s %-11s %2d s%s\n" "$c" "${WATCH[$c]}" "${maxgap[$c]}" \
        "$([ "${gap[$c]}" -gt 0 ] && echo '   <- STILL no video at the end')"; done

    echo "== 5. after the failure"; status; check_streams
    echo "== 6. stopping"; stop
    echo "Logs kept in $LOGS (copy them before the next run, it overwrites them)"
}

case "${1:-}" in
    test)   test_all "${2:-}" ;;
    start)  start ;;
    stop)   stop ;;
    status) status; check_streams ;;
    kill)   [ -n "${2:-}" ] || { echo "usage: $0 kill NODE"; exit 1; }; kill_node "$2" ;;
    *)      sed -n '2,30p' "$0"; exit 1 ;;
esac
