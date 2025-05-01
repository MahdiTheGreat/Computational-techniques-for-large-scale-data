#!/bin/bash

# Default values
CPUS=1
SCRIPT="./run_job.sh"
PARTITION="short"
POSITIONAL_ARGS=()

#-c: CPU count
#-s: Script to run
#-p: Partition name
#--: Everything after this is passed as arguments to the Python script

# Parse flags
while [[ $# -gt 0 ]]; do
  case $1 in
    -c)
      CPUS="$2"
      shift 2
      ;;
    -s)
      SCRIPT="$2"
      shift 2
      ;;
    -p)
      PARTITION="$2"
      shift 2
      ;;
    --)
      shift
      POSITIONAL_ARGS+=("$@")
      break
      ;;
    *)
      echo "Unknown parameter: $1"
      exit 1
      ;;
  esac
done

# Generate Slurm script on the fly
sbatch <<EOF
#!/bin/bash
#SBATCH --job-name=lettercount
#SBATCH --cpus-per-task=$CPUS
#SBATCH --partition=$PARTITION
#SBATCH --output=slurm-%j.out

python3 $SCRIPT "${POSITIONAL_ARGS[@]}"
EOF
