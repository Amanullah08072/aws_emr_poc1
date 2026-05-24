-- Analytical Production Queries
-- 1. E-Commerce Conversion Rates by Category
-- Calculates how effectively traffic translates into transaction revenue across partitioned product segments:

-- SQL
SELECT 
    category,
    COUNT(*) AS total_clicks_tracked,
    SUM(CASE WHEN action = 'checkout_complete' THEN 1 ELSE 0 END) AS complete_sales,
    ROUND(
        (SUM(CASE WHEN action = 'checkout_complete' THEN 1.0 ELSE 0.0 END) / COUNT(*)) * 100, 
        2
    ) AS conversion_rate_percentage,
    ROUND(SUM(CAST(revenue AS DOUBLE)), 2) AS total_gross_sales
FROM "poc1_db1"."clickstream_analytics"
GROUP BY category
ORDER BY total_gross_sales DESC;

-- 2. Digital Platform Traffic Performance Profile
-- Isolates transaction volume anomalies and tracks metrics to discover high-value checkout funnels based on client device footprints:

-- SQL
SELECT 
    device,
    COUNT(*) AS total_interactions,
    SUM(CASE WHEN action = 'checkout_complete' THEN 1 ELSE 0 END) AS successful_purchases,
    ROUND(AVG(CAST(revenue AS DOUBLE)), 2) AS average_ticket_size
FROM "poc1_db1"."clickstream_analytics"
WHERE action = 'checkout_complete'
GROUP BY device
ORDER BY total_interactions DESC;
