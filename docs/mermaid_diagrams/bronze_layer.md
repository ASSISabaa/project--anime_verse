## `docs/mermaid_diagrams/bronze_layer.md`
```markdown
# Bronze Layer Data Model

The Bronze layer contains raw, unprocessed data as it arrives from source systems. This layer preserves the original data structure with minimal transformation.

## Tables Overview

```mermaid
erDiagram
    bronze_pos_transactions {
        string transaction_id PK
        string customer_id
        string product_id
        int quantity
        double unit_price
        string timestamp
        string channel
        string event_type
        string store_location
        timestamp ingestion_timestamp
    }
    
    bronze_cinema_sales {
        string booking_id PK
        string customer_id
        string anime_title
        string screening_time
        string ticket_type
        int seats_booked
        double ticket_price
        double concession_amount
        string theater_id
        string payment_method
        timestamp ingestion_timestamp
    }
    
    bronze_inventory_updates {
        string update_id PK
        string product_id
        string supplier_id
        int quantity_received
        double unit_cost
        string shipment_date
        string expected_delivery
        string warehouse_location
        string quality_status
        timestamp ingestion_timestamp
    }
    
    bronze_customer_reviews {
        string review_id PK
        string customer_id
        string product_id
        string anime_title
        int rating
        string review_text
        string review_date
        boolean verified_purchase
        int helpful_votes
        timestamp ingestion_timestamp
    }

Data Sources and Characteristics
POS Transactions

Source: Point-of-sale systems via Kafka
Frequency: Real-time (2-second intervals)
Volume: ~1,000 transactions/hour
Partitioning: By ingestion date

Cinema Sales

Source: Cinema booking systems via Kafka
Frequency: Real-time (3-second intervals)
Volume: ~200 bookings/hour
Partitioning: By ingestion date

Inventory Updates

Source: Supplier systems via Kafka
Frequency: Batch with late arrivals (5-second intervals)
Volume: ~500 updates/hour
Late Arrival: Up to 48 hours
Partitioning: By ingestion date

Customer Reviews

Source: Review systems via Kafka
Frequency: Real-time (4-second intervals)
Volume: ~100 reviews/hour
Partitioning: By ingestion date

Quality Characteristics
Data Preservation

All original field names and types maintained
No business logic applied
Complete audit trail with ingestion timestamps
Source system metadata preserved

Schema Evolution

Automatic schema detection
Backward compatibility maintained
New fields added without breaking changes
Version tracking for schema changes