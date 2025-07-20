#!/bin/bash

echo "🔨 Building Airflow Docker image for AnimeVerse..."

# Set build context
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Create necessary directories
mkdir -p logs plugins dags

# Set proper permissions
chmod 755 logs plugins dags

echo "📁 Directory structure created"

# Build Docker image
echo "🐳 Building Airflow image..."
docker build -t animeverse-airflow:latest .

if [ $? -eq 0 ]; then
    echo "✅ Airflow image built successfully!"
    echo "🚀 You can now run: docker-compose up -d"
else
    echo "❌ Failed to build Airflow image"
    exit 1
fi

echo "📋 Next steps:"
echo "1. Run: docker-compose up -d"
echo "2. Access Airflow UI: http://localhost:8082"
echo "3. Login: admin/admin"