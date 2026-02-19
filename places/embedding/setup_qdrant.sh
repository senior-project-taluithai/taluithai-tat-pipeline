#!/bin/bash
# ============================================================
# Setup Qdrant on GCP VM
# Run this script on the VM after SSH-ing in:
#   gcloud compute ssh --zone "asia-southeast1-a" \
#     "chaiyawut_t@instance-20260212-064539" --project "taluithai"
# ============================================================

set -e

echo "========================================"
echo "  Setting up Qdrant Vector Database"
echo "========================================"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker not found. Installing Docker..."
    sudo apt-get update
    sudo apt-get install -y docker.io
    sudo systemctl start docker
    sudo systemctl enable docker
    sudo usermod -aG docker $USER
    echo "Docker installed. You may need to log out and log back in for group changes."
fi

# Stop existing Qdrant container if running
if docker ps -a --format '{{.Names}}' | grep -q '^qdrant$'; then
    echo "Stopping existing Qdrant container..."
    docker stop qdrant 2>/dev/null || true
    docker rm qdrant 2>/dev/null || true
fi

# Pull latest Qdrant image
echo "Pulling Qdrant image..."
docker pull qdrant/qdrant:latest

# Run Qdrant container
echo "Starting Qdrant container..."
docker run -d \
    --name qdrant \
    --restart unless-stopped \
    -p 6333:6333 \
    -p 6334:6334 \
    -v qdrant_storage:/qdrant/storage:z \
    qdrant/qdrant:latest

# Wait for Qdrant to be ready
echo "Waiting for Qdrant to start..."
for i in {1..30}; do
    if curl -s http://localhost:6333/healthz > /dev/null 2>&1; then
        echo "✓ Qdrant is running and healthy!"
        echo "  REST API: http://localhost:6333"
        echo "  gRPC:     localhost:6334"
        echo "  Dashboard: http://localhost:6333/dashboard"
        exit 0
    fi
    sleep 1
done

echo "✗ Qdrant failed to start within 30 seconds"
docker logs qdrant
exit 1
