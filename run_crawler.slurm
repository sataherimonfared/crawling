#!/bin/bash
#SBATCH --job-name=desy_crawler
#SBATCH --output=crawler_%j.out
#SBATCH --error=crawler_%j.err
#SBATCH --time=72:00:00          # 24 hours max (adjust if needed)
#SBATCH --mem=48G                 # 32GB RAM: safe margin for PARALLEL_WORKERS=64 at depth=4 (~1500 pages)
                                  # Increase to 64G if running depth=5+ (~10k pages)
#SBATCH --cpus-per-task=64       # 64 CPUs: matches PARALLEL_WORKERS=64 (CPU-bound workers)
                                  # Reduce to 32 if 64 CPUs not available; set PARALLEL_WORKERS to match
#SBATCH --partition=allcpu       # CPU partition — crawler uses no GPU (pure network I/O + CPU)
# NOTE: CONCURRENT_TASKS is auto-scaled by MAX_DEPTH in crawl_desy_all_urls.py.
#       PARALLEL_WORKERS is set in code; keep --cpus-per-task equal to or greater than it.

# ============================================================================
# SLURM Job Script for DESY Crawler
# ============================================================================
# 
# WHAT THIS DOES:
# - Submits crawl_desy_all_urls.py as a SLURM job
# - Runs in background even after you disconnect
# - Saves output to crawler_<jobid>.out
# - Saves errors to crawler_<jobid>.err
#
# HOW TO USE:
#   0. squeue -u $USER ==> check if you have any jobs running
#   1. salloc -p maxgpu --time=10:00:00 ==> ssh ...
#   2. Submit job: sbatch run_crawler.slurm
#   3. Check status: squeue -u $USER
#   4. Monitor output: tail -f crawler_<jobid>.out
#   5. Cancel if needed: scancel <jobid>
# next update (step 1-2 is optional, you can submit directly from your local machine if you have access to the cluster-it may cause job get canceled after researved time):
# ============================================================================

# Print job info
echo "=========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Job Name: $SLURM_JOB_NAME"
echo "Node: $SLURM_NODELIST"
echo "Start Time: $(date)"
echo "Working Directory: $(pwd)"
echo "=========================================="

# Load Python module (adjust if your cluster uses different module names)
# Uncomment the line that matches your cluster setup:
# module load python/3.9
# module load python3
# source /path/to/your/venv/bin/activate  # If using virtual environment

# Change to script directory
cd /home/taheri/crawl4ai

# Run the crawler
echo "Starting crawler..."
python -u crawl_desy_all_urls.py

# Print completion info
echo "=========================================="
echo "End Time: $(date)"
echo "Job completed!"
echo "=========================================="
