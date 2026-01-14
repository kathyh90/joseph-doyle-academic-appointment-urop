#!/bin/bash
#SBATCH --job-name=openalex
#SBATCH --output=/home/kathyh90/joseph-doyle-academic-appointment-urop/results/output_%j.out
#SBATCH --error=/home/kathyh90/joseph-doyle-academic-appointment-urop/results/output_%j.err
#SBATCH -t 1-00:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH -p sched_mit_sloan_batch

source /orcd/software/core/001/centos7/pkg/miniforge/24.3.0-0/bin/activate
conda activate urop_sloan_env

python3 "/home/kathyh90/joseph-doyle-academic-appointment-urop/5_orcid_get_years.py"
echo "All scripts completed successfully."
