-- poc1-aurora-postgres-cluster


CREATE TABLE ecommerce_performance_summary (
    category VARCHAR(100) PRIMARY KEY,
    total_clicks BIGINT,
    total_sales BIGINT,
    gross_revenue NUMERIC(15, 2),
    conversion_rate NUMERIC(5, 2),
    data_sync_timestamp TIMESTAMP WITHOUT TIME ZONE
);


select * from ecommerce_performance_summary;

