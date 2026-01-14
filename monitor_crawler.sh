#!/bin/bash
# ============================================================================
# Monitor Crawler Job Script
# ============================================================================
# 
# USAGE:
#   ./monitor_crawler.sh [jobid]
#
# If jobid not provided, finds the most recent crawler job
#
# ============================================================================

if [ -z "$1" ]; then
    # Find most recent job
    JOBID=$(squeue -u $USER -o "%i %j" | grep desy_crawler | head -1 | awk '{print $1}')
    if [ -z "$JOBID" ]; then
        echo "No running crawler job found."
        echo "Recent completed jobs:"
        ls -lt crawler_*.out 2>/dev/null | head -5
        exit 1
    fi
    echo "Found job ID: $JOBID"
else
    JOBID=$1
fi

OUTFILE="crawler_${JOBID}.out"
ERRFILE="crawler_${JOBID}.err"

echo "=========================================="
echo "Monitoring Job: $JOBID"
echo "Output file: $OUTFILE"
echo "Error file: $ERRFILE"
echo "=========================================="
echo ""

# Check job status
echo "Job Status:"
squeue -j $JOBID 2>/dev/null || echo "Job completed or not found"
echo ""

# Show last 20 lines of output
if [ -f "$OUTFILE" ]; then
    echo "=== Last 20 lines of output ==="
    tail -20 "$OUTFILE"
    echo ""
else
    echo "Output file not found yet (job may be starting)"
    echo ""
fi

# Show errors if any
if [ -f "$ERRFILE" ] && [ -s "$ERRFILE" ]; then
    echo "=== Errors ==="
    tail -20 "$ERRFILE"
    echo ""
fi

# Show file count (if crawler is writing files)
if [ -d "desy_crawled" ]; then
    FILE_COUNT=$(find desy_crawled -name "*.md" 2>/dev/null | wc -l)
    echo "Files crawled so far: $FILE_COUNT"
fi

echo ""
echo "To follow output in real-time: tail -f $OUTFILE"
echo "To cancel job: scancel $JOBID"
