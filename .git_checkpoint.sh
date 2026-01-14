#!/bin/bash
# Quick checkpoint before AI changes
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BRANCH_NAME="checkpoint-${TIMESTAMP}"

git checkout -b "$BRANCH_NAME"
git add -A
git commit -m "Checkpoint before AI changes - ${TIMESTAMP}"
git checkout main

echo "✅ Checkpoint created on branch: $BRANCH_NAME"
echo "To revert: git checkout $BRANCH_NAME"
echo "To merge: git merge $BRANCH_NAME"



# # Option 1: View what changed
# git diff checkpoint-20260114_150725 crawl_desy_all_urls.py

# # Option 2: Revert entire file to checkpoint
# git checkout checkpoint-20260114_150725 -- crawl_desy_all_urls.py

# # Option 3: Switch to checkpoint branch to see the old version
# git checkout checkpoint-20260114_150725
# # (then switch back: git checkout main)