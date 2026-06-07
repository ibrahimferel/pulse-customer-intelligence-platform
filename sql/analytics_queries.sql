-- 1. Customer Performance Overview
-- 1.a. KPI Revenue: total revenue, AOV, total customers
SELECT
    SUM(monetary)                       AS total_revenue,
    AVG(avg_order_value)                 AS avg_order_value,
    COUNT(DISTINCT customer_unique_id)  AS total_customers
FROM analytics.customer_features;

-- 1.b. Revenue per state - bar chart
SELECT
    g.customer_state,
    COUNT(DISTINCT g.customer_unique_id)  AS total_customers,
    ROUND(SUM(c.monetary), 2)              AS total_revenue,
    ROUND(AVG(c.avg_order_value), 2)       AS avg_order_value
FROM analytics.geo_features g
LEFT JOIN analytics.customer_features c
    ON g.customer_unique_id = c.customer_unique_id
WHERE g.customer_state IS NOT NULL
GROUP BY g.customer_state
ORDER BY total_revenue DESC;

-- 1.c. Revenue trend bulanan - line chart
SELECT
    purchase_month,
    COUNT(DISTINCT order_id)   AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM analytics.delivery_features
WHERE purchase_month IS NOT NULL
GROUP BY purchase_month
ORDER BY purchase_month ASC;

--------------------------------------------------------------------------------------
-- 2.  Customer Segmentation (FRM)
-- 2.a. Frequency distribution
SELECT
    frequency,
    COUNT(DISTINCT customer_unique_id) AS total_customers
FROM analytics.customer_features
GROUP BY frequency
ORDER BY frequency ASC;

-- 2.b. Recency distribution
SELECT
    CASE
        WHEN recency_days <= 30   THEN '0-30 hari'
        WHEN recency_days <= 90   THEN '31-90 hari'
        WHEN recency_days <= 180  THEN '91-180 hari'
        WHEN recency_days <= 365  THEN '181-365 hari'
        ELSE '> 365 hari'
    END                                AS recency_bucket,
    COUNT(DISTINCT customer_unique_id) AS total_customers
FROM analytics.customer_features
GROUP BY recency_bucket
ORDER BY MIN(recency_days) ASC;

-- 2.c. Monetary distribution
SELECT
    CASE
        WHEN monetary < 100   THEN '< 100'
        WHEN monetary < 500   THEN '100-499'
        WHEN monetary < 1000  THEN '500-999'
        WHEN monetary < 5000  THEN '1K-4.9K'
        ELSE '5K+'
    END                                AS monetary_bucket,
    COUNT(DISTINCT customer_unique_id) AS total_customers,
    ROUND(AVG(monetary), 2)            AS avg_monetary
FROM analytics.customer_features
GROUP BY monetary_bucket
ORDER BY MIN(monetary) ASC;

-- 2.d. Retention Segmentation: active, returning, churned
SELECT
    CASE
        WHEN repeat_customer = 1
             AND customer_tenure_days >= 180 THEN 'Loyal'
        WHEN repeat_customer = 1
             AND customer_tenure_days <  180 THEN 'Returning'
        WHEN repeat_customer = 0
             AND recency_days <= 90          THEN 'New Active'
        ELSE                                      'Churned'
    END                                AS retention_segment,
    COUNT(DISTINCT customer_unique_id) AS total_customers,
    ROUND(AVG(monetary), 2)            AS avg_monetary,
    ROUND(AVG(frequency), 2)           AS avg_frequency
FROM analytics.customer_features
GROUP BY retention_segment
ORDER BY total_customers DESC;

--------------------------------------------------------------------------------------
-- 3.  Customer Experience Analytics
-- 3.a. CX category summary
SELECT
    cx_category,
    COUNT(DISTINCT customer_unique_id)      AS total_customers,
    ROUND(AVG(cx_score), 2)                  AS avg_cx_score,
    ROUND(AVG(avg_review_score), 2)           AS avg_review,
    ROUND(AVG(complaint_rate) * 100, 2)       AS complaint_rate_pct,
    ROUND(AVG(late_delivery_rate) * 100, 2)   AS late_delivery_pct
FROM analytics.cx_features
GROUP BY cx_category
ORDER BY avg_cx_score DESC;

-- 3.b. distribusi review bintang 1–5 
SELECT
    ROUND(toFloat64(review_score), 0)  AS star_rating,
    COUNT()                             AS total_reviews
FROM analytics.nlp_features
WHERE review_score IS NOT NULL
GROUP BY star_rating
ORDER BY star_rating ASC;

-- 3.c. Complaint topic: distribusi 6 kategori 
SELECT
    complaint_topic,
    COUNT()                               AS total_reviews,
    ROUND(AVG(toFloat64(review_score)), 2) AS avg_score
FROM analytics.nlp_features
WHERE complaint_topic IS NOT NULL
  AND has_text = 1
GROUP BY complaint_topic
ORDER BY total_reviews DESC;

-- 3.d. CX tier proporsi 
SELECT
    cx_category,
    COUNT()                  AS total_customers,
    ROUND(AVG(cx_score), 2)  AS avg_cx_score
FROM analytics.cx_features
GROUP BY cx_category
ORDER BY avg_cx_score DESC;

--------------------------------------------------------------------------------------
-- 4.  Customer Behaviour Drivers
-- 4.a. Delay vs repeat customer
SELECT
    CASE
        WHEN toFloat64(delivery_delay_days) <= 0 THEN 'On Time'
        WHEN toFloat64(delivery_delay_days) <= 3 THEN 'Delay 1–3 hari'
        WHEN toFloat64(delivery_delay_days) <= 7 THEN 'Delay 4–7 hari'
        ELSE 'Delay > 7 hari'
    END                                     AS delay_bucket,
    COUNT()                                 AS total_orders,
    ROUND(AVG(toFloat64(review_score)), 2)  AS avg_review,
    ROUND(SUM(CASE WHEN repeat_customer=1
        THEN 1 ELSE 0 END) * 100.0 / COUNT(), 2) AS repeat_rate_pct
FROM analytics.delivery_features
WHERE delivery_delay_days IS NOT NULL
GROUP BY delay_bucket
ORDER BY MIN(toFloat64(delivery_delay_days)) ASC;

-- 4.b. Payment type vs loyalty
SELECT
    p.payment_type,
    p.installment_group,
    COUNT(DISTINCT p.order_id)       AS total_orders,
    ROUND(AVG(c.frequency), 2)        AS avg_frequency,
    ROUND(AVG(c.monetary), 2)         AS avg_monetary
FROM analytics.payment_features p
LEFT JOIN analytics.delivery_features d ON p.order_id = d.order_id
LEFT JOIN analytics.customer_features c ON d.customer_id = c.customer_unique_id
WHERE p.payment_type IS NOT NULL
GROUP BY p.payment_type, p.installment_group
ORDER BY avg_monetary DESC;

-- 4.c. Customer lifecycle: active, at risk, churned
SELECT
    CASE
        WHEN recency_days <= 90
             AND customer_tenure_days >= 180 THEN 'Active Loyal'
        WHEN recency_days <= 90
             AND customer_tenure_days <  180 THEN 'Active New'
        WHEN recency_days BETWEEN 91 AND 365  THEN 'At Risk'
        ELSE                                       'Churned'
    END                                AS lifecycle_segment,
    COUNT(DISTINCT customer_unique_id) AS total_customers,
    ROUND(AVG(monetary), 2)            AS avg_monetary,
    ROUND(AVG(frequency), 2)           AS avg_frequency
FROM analytics.customer_features
GROUP BY lifecycle_segment
ORDER BY total_customers DESC;

--------------------------------------------------------------------------------------
-- 5.  Geo & Seller Analytics
-- 5.a. Distribusi pelanggan per provinsi
SELECT
    customer_state,
    COUNT(DISTINCT customer_unique_id) AS total_customers,
    ROUND(AVG(geolocation_lat), 4)       AS avg_lat,
    ROUND(AVG(geolocation_lng), 4)       AS avg_lng
FROM analytics.geo_features
WHERE customer_state IS NOT NULL
  AND geolocation_lat IS NOT NULL
GROUP BY customer_state
ORDER BY total_customers DESC;

-- 5.b. Revenue per provinsi (top 20)
SELECT
    g.customer_state,
    COUNT(DISTINCT g.customer_unique_id) AS total_customers,
    ROUND(SUM(c.monetary), 2)             AS total_revenue,
    ROUND(AVG(c.avg_order_value), 2)      AS avg_order_value
FROM analytics.geo_features g
LEFT JOIN analytics.customer_features c
    ON g.customer_unique_id = c.customer_unique_id
WHERE g.customer_state IS NOT NULL
GROUP BY g.customer_state
ORDER BY total_revenue DESC
LIMIT 20;
