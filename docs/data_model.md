# AnimeVerse Data Model Documentation

## Data Modeling Approach

The AnimeVerse data pipeline follows the medallion architecture with three distinct layers:

### Bronze Layer (Raw Data)
Raw data ingested from various sources with minimal transformation.

### Silver Layer (Cleaned Data)  
Cleaned, validated, and enriched data ready for business logic.

### Gold Layer (Analytics Data)
Star schema design optimized for analytics and reporting.

## Bronze Layer Tables

### bronze_pos_transactions
**Purpose**: Raw point-of-sale transaction data
**Source**: POS systems via Kafka
**Update Frequency**: Real-time

| Column | Type | Description |
|--------|------|-------------|
| transaction_id | string | Unique transaction identifier |
| customer_id | string | Customer identifier |
| product_id | string | Product identifier |
| quantity | int | Number of items |
| unit_price | double | Price per unit |
| timestamp | string | Transaction timestamp |
| channel | string | Sales channel (online/store/mobile) |
| event_type | string | Type of transaction |
| store_location | string | Physical store location |
| ingestion_timestamp | timestamp | Data ingestion time |

### bronze_cinema_sales
**Purpose**: Raw cinema ticket and concession sales
**Source**: Cinema systems via Kafka
**Update Frequency**: Real-time

| Column | Type | Description |
|--------|------|-------------|
| booking_id | string | Unique booking identifier |
| customer_id | string | Customer identifier |
| anime_title | string | Movie title |
| screening_time | string | Show time |
| ticket_type | string | Ticket category |
| seats_booked | int | Number of seats |
| ticket_price | double | Price per ticket |
| concession_amount | double | Concession sales |
| theater_id | string | Theater identifier |
| payment_method | string | Payment type |

### bronze_inventory_updates
**Purpose**: Raw inventory movements and updates
**Source**: Supplier systems via Kafka
**Update Frequency**: Batch (with late arrivals)

| Column | Type | Description |
|--------|------|-------------|
| update_id | string | Unique update identifier |
| product_id | string | Product identifier |
| supplier_id | string | Supplier identifier |
| quantity_received | int | Quantity received |
| unit_cost | double | Cost per unit |
| shipment_date | string | Shipment date |
| expected_delivery | string | Expected delivery |
| warehouse_location | string | Storage location |
| quality_status | string | Quality check status |

### bronze_customer_reviews
**Purpose**: Raw customer review data
**Source**: Review systems via Kafka
**Update Frequency**: Real-time

| Column | Type | Description |
|--------|------|-------------|
| review_id | string | Unique review identifier |
| customer_id | string | Customer identifier |
| product_id | string | Product identifier |
| anime_title | string | Related anime |
| rating | int | Rating (1-5) |
| review_text | string | Review content |
| review_date | string | Review date |
| verified_purchase | boolean | Purchase verification |
| helpful_votes | int | Helpful vote count |

## Silver Layer Tables

### silver_transactions
**Purpose**: Cleaned and enriched transaction data
**Transformations**: Data quality checks, calculated fields, date dimensions

| Column | Type | Description |
|--------|------|-------------|
| transaction_id | string | Unique identifier |
| customer_id | string | Customer identifier |
| product_id | string | Product identifier |
| quantity | int | Quantity purchased |
| unit_price | double | Unit price |
| total_amount | double | Calculated total |
| discount_amount | double | Applied discounts |
| final_amount | double | Final amount paid |
| sale_ts | timestamp | Sale timestamp |
| date_key | int | Date dimension key |
| channel | string | Sales channel |
| store_location | string | Store location |
| processed_at | timestamp | Processing timestamp |

### silver_cinema_screenings
**Purpose**: Cleaned cinema and concession data
**Transformations**: Data validation, business rules, calculated metrics

### silver_inventory
**Purpose**: Cleaned inventory data with late arrival handling
**Transformations**: Late arrival flags, cost calculations, quality filters

### silver_reviews
**Purpose**: Cleaned review data with sentiment analysis
**Transformations**: Sentiment scoring, text cleaning, validation

## Gold Layer Tables (Star Schema)

### Dimension Tables

#### dim_customers (SCD Type 2)
**Purpose**: Customer master data with historical tracking

| Column | Type | Description |
|--------|------|-------------|
| customer_id | string | Natural key |
| name | string | Customer name |
| email | string | Email address |
| signup_date | date | Registration date |
| segment | string | Customer segment |
| loyalty_tier | string | Loyalty level |
| preferred_genres | string | Genre preferences |
| effective_start | timestamp | SCD start date |
| effective_end | timestamp | SCD end date |
| is_current | boolean | Current record flag |

#### dim_products
**Purpose**: Product catalog information

#### dim_anime_titles (SCD Type 2)
**Purpose**: Anime information with schedule changes

#### dim_calendar
**Purpose**: Date dimension for time-based analysis

### Fact Tables

#### fact_sales
**Purpose**: Sales transaction facts
**Grain**: One row per transaction line item

| Column | Type | Description |
|--------|------|-------------|
| sales_key | string | Surrogate key |
| customer_id | string | Customer dimension FK |
| product_id | string | Product dimension FK |
| anime_title_id | string | Anime dimension FK |
| date_key | int | Calendar dimension FK |
| quantity | int | Quantity sold |
| unit_price | double | Unit price |
| total_amount | double | Total amount |
| discount_amount | double | Discount applied |
| final_amount | double | Final amount |
| channel | string | Sales channel |
| store_location | string | Store location |

#### fact_cinema_attendance
**Purpose**: Cinema ticket and concession facts
**Grain**: One row per booking

#### fact_customer_engagement
**Purpose**: Cross-channel customer engagement
**Grain**: One row per customer per day

#### fact_daily_summary
**Purpose**: Daily business KPIs
**Grain**: One row per day

## Data Quality Rules

### Bronze Layer
- Schema validation
- Duplicate detection
- Missing value identification

### Silver Layer
- Business rule validation
- Range checks (prices, quantities)
- Referential integrity

### Gold Layer
- Dimensional integrity
- Fact table validation
- SCD processing validation

## Performance Considerations

### Partitioning Strategy
- **Bronze**: Partitioned by ingestion date
- **Silver**: Partitioned by business date  
- **Gold**: Partitioned by date_key

### Indexing
- Iceberg supports column-level statistics
- Z-ordering for frequently queried columns
- Bloom filters for high-cardinality columns

### Retention Policies
- **Bronze**: 2 years (compliance)
- **Silver**: 5 years (business analysis)
- **Gold**: 10 years (historical reporting)