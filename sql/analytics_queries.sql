-- Analytics Queries
-- Ad-hoc analytics and reporting queries

-- Customer Lifetime Value
SELECT 
    customer_id,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(total_amount) as lifetime_value
FROM orders
GROUP BY customer_id
ORDER BY lifetime_value DESC;
