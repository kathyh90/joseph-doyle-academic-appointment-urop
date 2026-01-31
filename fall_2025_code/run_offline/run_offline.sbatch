#!/bin/bash
#SBATCH --job-name=openalex
#SBATCH --output=/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/output_%j.out
#SBATCH --error=/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/output_%j.err
#SBATCH -t 1-00:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH -p sched_mit_sloan_batch

source /orcd/software/core/001/centos7/pkg/miniforge/24.3.0-0/bin/activate
conda activate urop_sloan_env

python3 "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/main_pipeline/2_checkaffiliations.py"
echo "All scripts completed successfully."
