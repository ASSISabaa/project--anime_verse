## `docs/mermaid_diagrams/gold_layer.md`
```markdown
# Gold Layer Data Model (Star Schema)

The Gold layer implements a star schema optimized for analytics, business intelligence, and machine learning workloads.

## Star Schema Overview

```mermaid
erDiagram
    fact_sales {
        string sales_key PK
        string customer_id FK
        string product_id FK
        string anime_title_id FK
        int date_key FK
        int quantity
        double unit_price
        double total_amount
        double discount_amount
        double final_amount
        string channel
        string store_location
        timestamp sale_ts
    }
    
    fact_cinema_attendance {
        string attendance_key PK
        string customer_id FK
        string anime_title_id FK
        int date_key FK
        string theater_id
        string ticket_type
        int seats_booked
        double ticket_price
        double total_ticket_amount
        double concession_amount
        double total_amount
        string payment_method
        timestamp screening_ts
    }
    
    fact_customer_engagement {
        string engagement_key PK
        string customer_id FK
        string anime_title_id FK
        int date_key FK
        string engagement_type
        int purchase_count
        double total_spent
        int screening_count
        double total_cinema_spent
        int review_count
        double avg_rating
        int engagement_score
    }
    
    dim_customers {
        string customer_id PK
        string name
        string email
        date signup_date
        string segment
        string loyalty_tier
        string preferred_genres
        timestamp effective_start
        timestamp effective_end
        boolean is_current
    }
    
    dim_products {
        string product_id PK
        string product_name
        string category
        string format
        string anime_title_id
        double base_price
        string supplier_id
        date created_date
    }
    
    dim_anime_titles {
        string anime_title_id PK
        string title_name
        string genre
        string season
        string studio
        date release_date
        int popularity_score
        string status
        date broadcast_start_date
        date broadcast_end_date
        timestamp effective_start
        timestamp effective_end
        boolean is_current
    }
    
    dim_calendar {
        int date_key PK
        date full_date
        int year
        int month
        int day
        int quarter
        int week_of_year
        int day_of_week
        string day_name
        string month_name
        boolean is_weekend
        boolean is_holiday
        string holiday_name
    }
    
    fact_sales }|--|| dim_customers : customer_id
    fact_sales }|--|| dim_products : product_id
    fact_sales }|--|| dim_anime_titles : anime_title_id
    fact_sales }|--|| dim_calendar : date_key
    
    fact_cinema_attendance }|--|| dim_customers : customer_id
    fact_cinema_attendance }|--|| dim_anime_titles : anime_title_id
    fact_cinema_attendance }|--|| dim_calendar : date_key
    
    fact_customer_engagement }|--|| dim_customers : customer_id
    fact_customer_engagement }|--|| dim_anime_titles : anime_title_id
    fact_customer_engagement }|--|| dim_calendar : date_key

SCD Type 2 Implementation
Customer Dimension Changes

flowchart TD
    A[New Customer Data] --> B{Compare with Current}
    B -->|No Change| C[No Action]
    B -->|Attribute Changed| D[Close Current Record]
    D --> E[Set effective_end = now]
    D --> F[Set is_current = false]
    E --> G[Insert New Record]
    F --> G
    G --> H[Set effective_start = now]
    G --> I[Set is_current = true]

Anime Schedule Changes
flowchart TD
    A[Schedule Update] --> B{Broadcast Dates Changed?}
    B -->|Yes| C[Create New Version]
    B -->|No| D[Update Current Record]
    C --> E[Close Previous Version]
    C --> F[Insert New Record with SCD]

Business Metrics and KPIs
Daily Summary Fact Table
erDiagram
    fact_daily_summary {
        int date_key PK
        date full_date
        string day_name
        boolean is_weekend
        double total_sales_amount
        int total_transactions
        int unique_customers
        double avg_order_value
        string top_selling_product
        int total_tickets_sold
        double total_concessions
        double total_cinema_revenue
        int unique_cinema_customers
        string top_anime_title
        int cross_channel_customers
        double store_to_cinema_conversion_rate
        int inventory_received
        double inventory_value
        int total_reviews
        double avg_rating
        double total_revenue
        int total_unique_customers
    }

Analytics Use Cases
Customer Analytics

Customer lifetime value calculation
Segmentation analysis (VIP, Premium, Regular, Casual)
Cross-channel behavior tracking
Churn prediction features

Product Analytics

Product performance by category and anime title
Inventory turnover analysis
Price optimization insights
Seasonal trend analysis

Cinema Analytics

Show performance and attendance patterns
Concession sales correlation with movie types
Optimal scheduling analysis
Revenue per seat metrics

Cross-Channel Analytics

Store-to-cinema conversion rates
Customer journey mapping
Multi-touch attribution
Unified customer view

Performance Optimizations
Partitioning Strategy

Facts: Partitioned by date_key for time-based queries
Dimensions: Partitioned by effective_date for SCD queries
Daily Summary: Partitioned by month for reporting efficiency

Indexing and Clustering

Clustered by customer_id for customer analytics
Z-ordered by anime_title_id for product analysis
Bloom filters on high-cardinality dimensions