#!/usr/bin/env bash
# Hourly copy of a node's results off /local (wiped at the reservation's end) to a project dir.
# usage: sync-results.sh <outdir> <dest>   (run under a loop: while true; do sync-results.sh …; sleep 3600; done)
rsync -a --exclude 'target' "$1/" "$2/$(hostname)/"
