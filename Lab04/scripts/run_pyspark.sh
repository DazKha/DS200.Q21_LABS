#!/bin/bash
# =======================================================
#  Fecom E-Commerce Analysis - PySpark Runner
#  Runs all tasks (1-5 mandatory + 6,7,10 optional)
# =======================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON_DIR="$PROJECT_DIR/spark/python"
VENV_DIR="$PROJECT_DIR/.venv"

export JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

echo "======================================================="
echo "  FECOM E-COMMERCE ANALYSIS"
echo "  PySpark DataFrame"
echo "======================================================="
echo ""
echo "  Project dir : $PROJECT_DIR"
echo "  Python dir  : $PYTHON_DIR"
echo ""

source "$VENV_DIR/bin/activate"
cd "$PYTHON_DIR"

python run_all.py

echo ""
echo "Done. Check results/ for output files."
