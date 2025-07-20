# AnimeVerse Data Engineering Pipeline

## 🎯 Overview
A comprehensive, production-ready data engineering pipeline for AnimeVerse - an anime merchandise store with integrated cinema operations. This project demonstrates modern data engineering practices using real-time streaming, batch processing, and advanced analytics.

## 🏗️ Architecture
Kafka Streams → Bronze Layer (Iceberg) → Silver Layer → Gold Layer → Analytics
↓              ↓                      ↓             ↓
Raw Data      Staging Data         Clean Data    Star Schema

## 🚀 Key Features

### ✅ **Real-time Data Streaming**
- 4 Kafka producers generating realistic business data
- Point-of-sale transactions, cinema sales, inventory updates, customer reviews
- Sub-second latency processing with Spark Streaming

### ✅ **Modern Data Lake Architecture**
- **Bronze Layer**: Raw data ingestion with minimal transformation
- **Silver Layer**: Cleaned, validated, and enriched data
- **Gold Layer**: Star schema optimized for analytics and ML

### ✅ **Advanced Data Processing**
- Late arrival data handling (up to 48 hours)
- SCD Type 2 implementation for historical tracking
- Comprehensive data quality monitoring
- Real-time and batch processing capabilities

### ✅ **Production-Ready Infrastructure**
- Containerized deployment with Docker Compose
- Apache Iceberg for ACID transactions and time travel
- Apache Airflow for workflow orchestration
- MinIO for S3-compatible object storage

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Streaming** | Apache Kafka | Real-time data ingestion |
| **Processing** | Apache Spark | Data transformation & analytics |
| **Storage** | MinIO + Apache Iceberg | Data lake with ACID properties |
| **Orchestration** | Apache Airflow | Workflow automation |
| **Containerization** | Docker & Docker Compose | Service deployment |

## 📊 Data Model

### Business Context
AnimeVerse operates both online and physical stores selling anime merchandise, plus a small cinema screening anime movies. The pipeline tracks:

- **Customer Journey**: From product browsing to purchase to cinema attendance
- **Inventory Management**: Real-time stock levels and supplier updates
- **Cross-channel Analytics**: Understanding customer behavior across touchpoints
- **Business Intelligence**: Daily KPIs, trends, and predictive insights

### Data Sources
1. **POS Transactions** (Real-time): Sales from online and physical stores
2. **Cinema Sales** (Real-time): Ticket and concession purchases  
3. **Inventory Updates** (Batch): Supplier deliveries and stock movements
4. **Customer Reviews** (Real-time): Product and experience feedback

## 🚀 Quick Start

### Prerequisites
- Docker Engine 20.10+ & Docker Compose 2.0+
- 8GB+ RAM (16GB recommended)
- 20GB+ free disk space
- Python 3.8+ (for data producers)

### 1. Clone and Setup
```bash
git clone <repository-url>
cd animeverse-data-pipeline

# Run setup script
chmod +x setup.sh
./setup.sh

2. Start Infrastructure
# Create Docker network
docker network create animeverse_network

# Start all services
docker-compose up -d

# Verify services are running
docker-compose ps

3. Start Data Producers
# Start all data producers
./start_producers.sh

# Or start individually
cd streaming/producers
python pos_producer.py &
python cinema_producer.py &
python inventory_producer.py &
python reviews_producer.py &

4. Monitor Services
4. Monitor Services
ServiceURLCredentialsMinIO Consolehttp://localhost:9001admin / password123Kafka UIhttp://localhost:8080-Spark Masterhttp://localhost:8081-Airflowhttp://localhost:8082admin / admin
📈 Data Pipeline Flow
1. Ingestion Layer

Kafka topics receive real-time business events
Python producers simulate realistic transaction patterns
Schema validation and serialization

2. Bronze Layer (Raw Data)

Minimal transformation, preserving original data
Partitioned by ingestion date for performance
Full audit trail with ingestion timestamps

3. Silver Layer (Cleaned Data)

Data quality validation and cleansing
Business rule application
Late arrival data reconciliation
Type conversion and standardization

4. Gold Layer (Analytics Ready)

Star schema dimensional modeling
SCD Type 2 for historical tracking
Pre-aggregated metrics and KPIs
Optimized for BI tools and ML workloads

🔧 Key Components
Data Quality Framework

Schema Validation: Automatic schema drift detection
Data Profiling: Statistical analysis of data distributions
Business Rules: Custom validation logic
Monitoring: Real-time alerts for data quality issues

Late Arrival Handling

Configurable watermarks for out-of-order data
Automatic reprocessing of late events
Reconciliation reports for business users

SCD Type 2 Implementation

Automatic change detection and versioning
Customer segmentation history tracking
Anime schedule change management
Audit trail for all dimensional changes

📊 Analytics Capabilities
Star Schema Design

Facts: Sales transactions, cinema attendance, customer engagement
Dimensions: Customers, products, anime titles, calendar
Metrics: Revenue, attendance, cross-sell rates, customer lifetime value

Business Intelligence

Daily/weekly/monthly business summaries
Customer segmentation and behavior analysis
Product performance and inventory optimization
Cross-channel conversion tracking

Machine Learning Ready

Feature engineering pipelines
Customer churn prediction datasets
Recommendation system inputs
Demand forecasting data marts

🔍 Monitoring & Observability
Data Pipeline Monitoring

Kafka lag and throughput metrics
Spark job execution times and success rates
Data freshness and completeness tracking
Storage utilization and performance

Business Monitoring

Real-time transaction volumes
Data quality scorecards
SLA compliance tracking
Cost and resource optimization

🧪 Testing & Validation
Data Quality Tests
# Run comprehensive data quality checks
docker exec animeverse-spark-master spark-submit \
  --class silver_quality_check \
  /opt/bitnami/spark/work-dir/silver_quality_check.py

  🚀 Production Deployment
Scaling Considerations

Kafka: Increase partitions for higher throughput
Spark: Add more worker nodes for processing capacity
Storage: Configure object storage replication
Monitoring: Implement comprehensive alerting

Security Best Practices

Change default passwords in production
Enable TLS/SSL for all communications
Implement proper network segmentation
Use secrets management for credentials

📚 Documentation

Architecture Guide
Data Model Documentation
Setup Guide
Troubleshooting Guide

🤝 Contributing

Fork the repository
Create a feature branch
Make your changes
Add tests and documentation
Submit a pull request

📄 License
This project is licensed under the MIT License - see the LICENSE file for details.
🙏 Acknowledgments

Apache Foundation for the amazing big data tools
Anime community for the inspiration
Open source contributors worldwide