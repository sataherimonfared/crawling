# SLURM Job Submission Guide

## Quick Start

### 1. Submit the Job
```bash
sbatch run_crawler.slurm
```
Output: `Submitted batch job 12345` (remember the job ID)

### 2. Monitor the Job
```bash
# Auto-find and monitor
./monitor_crawler.sh

# Or specify job ID
./monitor_crawler.sh 12345

# Or manually check status
squeue -u $USER
```

### 3. Watch Output in Real-Time
```bash
tail -f crawler_12345.out
```

### 4. Check Progress
```bash
# Count crawled files
find desy_crawled -name "*.md" | wc -l

# Check latest files
ls -lt desy_crawled/depth_*/ | head -10
```

## Useful Commands

### Check Job Status
```bash
squeue -u $USER                    # Your running jobs
squeue -j 12345                    # Specific job
squeue -u $USER -o "%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R"  # Detailed
```

### View Job Output
```bash
cat crawler_12345.out              # Full output
tail -100 crawler_12345.out        # Last 100 lines
tail -f crawler_12345.out          # Follow live
```

### View Errors
```bash
cat crawler_12345.err              # Full errors
tail -50 crawler_12345.err         # Last 50 lines
```

### Cancel Job
```bash
scancel 12345                      # Cancel specific job
scancel -u $USER                   # Cancel all your jobs
```

### Check Job Details
```bash
scontrol show job 12345             # Detailed job info
sacct -j 12345                     # Job accounting info
```

## After Disconnecting

The job continues running! When you reconnect:

1. **Find your job:**
   ```bash
   squeue -u $USER
   ```

2. **If job completed, check output:**
   ```bash
   ls -lt crawler_*.out | head -1   # Most recent output file
   ```

3. **Check results:**
   ```bash
   find desy_crawled -name "*.md" | wc -l
   ```

## Customizing Resources

Edit `run_crawler.slurm` to adjust:

```bash
#SBATCH --time=48:00:00          # Longer time limit
#SBATCH --mem=16G                 # More memory
#SBATCH --cpus-per-task=8         # More CPUs
#SBATCH --partition=gpu           # Different partition
```

## Troubleshooting

### Job Won't Start
- Check partition: `sinfo`
- Check resource limits: `sacctmgr show user $USER`
- Check errors: `cat crawler_<jobid>.err`

### Job Failed
- Check error file: `cat crawler_<jobid>.err`
- Check if Python modules are loaded
- Verify script path is correct

### Need More Resources
- Edit `run_crawler.slurm` and increase `--mem` or `--time`
- Resubmit: `sbatch run_crawler.slurm`
