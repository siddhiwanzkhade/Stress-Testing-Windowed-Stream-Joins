# Environment shared by every SLURM job (sourced, not run).
# Python 3.11 venv (.venv, made with uv) + Java 21 (tools/jdk); SeaWulf has no Java 17+ module.
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if type module >/dev/null 2>&1; then module purge >/dev/null 2>&1 || true; fi
if type module >/dev/null 2>&1; then module load slurm >/dev/null 2>&1 || true; fi
source "$REPO/.venv/bin/activate"
export JAVA_HOME="$REPO/tools/jdk"
export PATH="$JAVA_HOME/bin:$PATH"
PYSPARK_PYTHON="$(command -v python)"
SPARK_LOCAL_DIRS="${TMPDIR:-/tmp}"
export PYSPARK_PYTHON SPARK_LOCAL_DIRS

