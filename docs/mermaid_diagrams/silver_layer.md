## `docs/mermaid_diagrams/silver_layer.md`
```markdown
# Silver Layer Data Model

The Silver layer contains cleaned, validated, and business-rule-applied data. This layer serves as the foundation for analytics and machine learning.

## Tables Overview

```mermaid
erDiagram
    silver_transactions {
        string transaction_id PK
        string customer_id FK
        string product_id FK
        int quantity
        double unit_price
        double total_amount
        double discount_amount
        double final_amount
        timestamp sale_ts
        int date_key
        string channel
        string store_location
        timestamp processed_at
    }
    
    silver_cinema_screenings {
        string booking_id PK
        string customer_id FK
        string anime_title
        timestamp screening_ts
        int date_key
        string ticket_type
        int seats_booked
        double ticket_price
        double total_ticket_amount
        double concession_amount
        double total_amount
        string theater_id
        string payment_method
        timestamp processed_at
    }
    
    silver_inventory {
        string update_id PK
        string product_id FK
        string supplier_id
        int quantity_received
        double unit_cost
        double total_cost
        timestamp shipment_ts
        timestamp delivery_ts
        int date_key
        string warehouse_location
        boolean is_late_arrival
        timestamp processed_at
    }
    
    silver_reviews {
        string review_id PK
        string customer_id FK
        string product_id FK
        string anime_title
        int rating
        string review_text
        string sentiment_score
        timestamp review_ts
        int date_key
        boolean verified_purchase
        int helpful_votes
        timestamp processed_at
    }

Data Transformations
Common Transformations Applied
Data Quality

Null value handling and defaulting
Duplicate record removal
Range validation (prices, quantities, ratings)
Format standardization

Business Logic

Calculated fields (total_amount = quantity × unit_price)
Date dimension keys for time-based analysis
Late arrival flagging for inventory data
Sentiment scoring for review text

Data Enrichment

Timestamp parsing and timezone normalization
Category derivation and standardization
Business date vs. system date handling

Late Arrival Processing
flowchart TD
    A[Bronze Data] --> B{Check Timestamp}
    B -->|Within 2 hours| C[Normal Processing]
    B -->|2-48 hours old| D[Late Arrival Flag]
    B -->|> 48 hours| E[Exception Handling]
    
    C --> F[Silver Layer]
    D --> G[Silver Layer + Late Flag]
    E --> H[Quarantine Table]
    
    G --> I[Reconciliation Process]
    H --> J[Manual Review]
Quality Assurance
Data Validation Rules

Transactions: Positive quantities and prices
Cinema: Valid seat counts and show times
Inventory: Approved quality status only
Reviews: Rating range 1-5, valid customer references

Monitoring Metrics

Record count consistency (Bronze → Silver)
Data freshness (time since last update)
Quality score percentage
Late arrival rates and patterns