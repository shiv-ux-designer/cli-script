#!/bin/bash
# Helper script to activate virtual environment and run products_cli.py

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found!"
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "Installing dependencies..."
    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
else
    # Activate virtual environment
    source venv/bin/activate
fi

# Run the CLI script with all arguments passed to this script
python3 products_cli.py "$@"

# Deactivate virtual environment when done
deactivate

