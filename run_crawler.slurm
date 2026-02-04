#!/bin/bash
#SBATCH --job-name=desy_crawler
#SBATCH --output=crawler_%j.out
#SBATCH --error=crawler_%j.err
#SBATCH --time=24:00:00          # 24 hours max (adjust if needed)
#SBATCH --mem=24G                 # 24GB RAM (60 tasks × ~300MB each + overhead)
#SBATCH --cpus-per-task=20      # 60 CPUs to match CONCURRENT_TASKS=60 (one CPU per concurrent browser task)python -c "import os; print(os.cpu_count())"
#SBATCH --partition=allcpu       # Use CPU partition (not GPU) - check available partitions with: sinfo
# NOTE: If 60 CPUs not available, reduce both:
#       - CONCURRENT_TASKS in crawl_desy_all_urls.py (e.g., to 30 or 40)
#       - --cpus-per-task here to match (e.g., 30 or 40)
#       - --mem can be reduced proportionally (e.g., 12G for 30 tasks, 16G for 40 tasks)

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
#
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
python crawl_desy_all_urls.py

# Print completion info
echo "=========================================="
echo "End Time: $(date)"
echo "Job completed!"
echo "=========================================="
