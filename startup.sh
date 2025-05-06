#!/bin/bash
set -e

echo "Starting Prefect worker setup..."

# Install Python dependencies from requirements.txt
echo "Installing Python dependencies..."
pip install -r /opt/prefect/requirements.txt
pip install python-json-logger  # Required for JSON logging


# Create work pool if it doesn't exist
# echo "Creating work pool if needed..."
# prefect work-pool create programdefaultworkers --type process || echo "Work pool already exists"

# Deploy using prefect.yaml
echo "Creating/updating deployments from prefect.yaml..."
cd /opt/prefect
prefect deploy --all

# Start the worker
echo "Starting Prefect worker..."
prefect worker start -p default-agent-pool

# Keep the container running
echo "Worker setup complete!"