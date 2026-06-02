-- Metabase Analytics Queries
-- Query definitions for Metabase dashboard

-- Sales Dashboard
SELECT 
    DATE(order_date) as date,
    COUNT(*) as total_orders,
    SUM(total_amount) as revenue
FROM orders
GROUP BY DATE(order_date)
ORDER BY date DESC;
