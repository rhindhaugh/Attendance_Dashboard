#!/bin/bash
# Setup script for Attendance Dashboard
# This script creates a virtual environment and installs all dependencies

echo "Setting up Attendance Dashboard environment..."

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

echo "Detected Python $PYTHON_VERSION"

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv dashboard_venv
source dashboard_venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "Installing dependencies..."
if [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -ge 12 ]; then
    echo "Using Python 3.12+, installing packages with --prefer-binary option..."
    pip install --prefer-binary -r requirements.txt
else
    echo "Using Python $PYTHON_VERSION, installing packages normally..."
    pip install -r requirements.txt
fi

echo "Setup complete! To activate the environment run:"
echo "source dashboard_venv/bin/activate"
echo ""
echo "To run the dashboard:"
echo "streamlit run src/dashboard.py"