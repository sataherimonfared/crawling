#!/bin/bash
#SBATCH --job-name=desy_crawler
#SBATCH --output=crawler_%j.out
#SBATCH --error=crawler_%j.err
#SBATCH --time=24:00:00          # 24 hours max (adjust if needed)
#SBATCH --mem=8G                 # 8GB RAM (adjust if needed)
#SBATCH --cpus-per-task=4        # 4 CPUs (adjust if needed)
#SBATCH --partition=allcpu       # Available: allcpu, allgpu, comcpu, comgpu, etc. (check with: sinfo)

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
#   1. Submit job: sbatch run_crawler.slurm
#   2. Check status: squeue -u $USER
#   3. Monitor output: tail -f crawler_<jobid>.out
#   4. Cancel if needed: scancel <jobid>
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
