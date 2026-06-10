-- 1. CUSTOMER PERFORMANCE OVERVIEW 

-- 1.1 KPI Revenue (Bobot KPI 0.4)
SELECT
    ROUND(SUM(monetary), 2)                              AS total_revenue,
    ROUND(AVG(avg_order_value), 2)                       AS avg_order_value,
    ROUND(SUM(monetary) / COUNT(DISTINCT customer_unique_id), 2)
                                                         AS revenue_per_customer,
    COUNT(DISTINCT customer_unique_id)                   AS total_customers,
    ROUND(SUM(monetary) / 99441, 2)                      AS avg_revenue_per_order
FROM analytics.customer_features;

-- 1.2 Customer KPI & Repeat Orders
SELECT
    COUNT(DISTINCT customer_unique_id)                   AS total_customers,
    SUM(repeat_customer)                                 AS total_repeat_customers,
    COUNT(DISTINCT customer_unique_id)
        - SUM(repeat_customer)                           AS one_time_customers,
    ROUND(SUM(repeat_customer) * 100.0
        / COUNT(DISTINCT customer_unique_id), 2)         AS repeat_rate_pct,
    ROUND(AVG(frequency), 2)                             AS avg_frequency,
    ROUND(AVG(monetary), 2)                              AS avg_monetary,
    MAX(frequency)                                       AS max_frequency
FROM analytics.customer_features;

-- 1.3  Revenue Trend Dashboard 
SELECT
    d.purchase_month,
    COUNT(DISTINCT d.order_id)                           AS total_orders,
    COUNT(DISTINCT d.customer_id)                        AS unique_customers,
    ROUND(SUM(c.monetary / c.frequency), 2)              AS estimated_revenue,
    ROUND(AVG(toFloat64(d.review_score)), 2)             AS avg_review_score,
    SUM(CASE WHEN d.cancellation_flag = 1 THEN 1 ELSE 0 END)
                                                         AS cancelled_orders,
    ROUND(SUM(CASE WHEN d.cancellation_flag = 1 THEN 1 ELSE 0 END)
        * 100.0 / COUNT(DISTINCT d.order_id), 2)         AS cancellation_rate_pct
FROM analytics.delivery_features d
LEFT JOIN analytics.customer_features c
    ON d.customer_id = c.customer_unique_id
WHERE d.purchase_month IS NOT NULL
GROUP BY d.purchase_month
ORDER BY d.purchase_month ASC;


-- 2. CUSTOMER SEGMENTATION (FRM) 

-- 2.1 Frequency Analysis 
SELECT
    frequency,
    COUNT(DISTINCT customer_unique_id)                   AS total_customers,
    ROUND(COUNT(DISTINCT customer_unique_id) * 100.0
        / SUM(COUNT(DISTINCT customer_unique_id)) OVER (), 2)
                                                         AS pct_customers,
    ROUND(AVG(monetary), 2)                              AS avg_monetary,
    ROUND(AVG(recency_days), 0)                          AS avg_recency_days
FROM analytics.customer_features
GROUP BY frequency
ORDER BY frequency ASC;

-- 2.2 Monetary Analysis 
SELECT
    CASE
        WHEN monetary = 0           THEN '0 — gratis/cancel'
        WHEN monetary < 50          THEN '< 50'
        WHEN monetary < 200         THEN '50-199'
        WHEN monetary < 500         THEN '200-499'
        WHEN monetary < 1000        THEN '500-999'
        WHEN monetary < 3000        THEN '1K-2.9K'
        ELSE                             '3K+'
    END                                                  AS monetary_bucket,
    COUNT(DISTINCT customer_unique_id)                   AS total_customers,
    ROUND(AVG(monetary), 2)                              AS avg_monetary,
    ROUND(AVG(avg_order_value), 2)                       AS avg_order_value,
    ROUND(AVG(frequency), 2)                             AS avg_frequency
FROM analytics.customer_features
GROUP BY monetary_bucket
ORDER BY MIN(monetary) ASC;

-- 2.3 Recency Analysis 
SELECT
    CASE
        WHEN recency_days <= 30    THEN '≤ 30 hari (Hot)'
        WHEN recency_days <= 90    THEN '31-90 hari (Warm)'
        WHEN recency_days <= 180   THEN '91-180 hari (Cooling)'
        WHEN recency_days <= 365   THEN '181-365 hari (Cold)'
        ELSE                            '> 365 hari (Dormant)'
    END                                                  AS recency_segment,
    COUNT(DISTINCT customer_unique_id)                   AS total_customers,
    ROUND(AVG(monetary), 2)                              AS avg_monetary,
    ROUND(AVG(frequency), 2)                             AS avg_frequency,
    SUM(repeat_customer)                                 AS total_repeat_customers
FROM analytics.customer_features
GROUP BY recency_segment
ORDER BY MIN(recency_days) ASC;

-- 2.4 High Value Customer Segment 
SELECT
    customer_unique_id,
    frequency,
    ROUND(monetary, 2)                                   AS monetary,
    ROUND(avg_order_value, 2)                            AS avg_order_value,
    recency_days,
    customer_tenure_days,
    preferred_payment_type,
    CASE
        WHEN monetary >= (SELECT quantile(0.95)(monetary)
                          FROM analytics.customer_features)
             THEN 'Top 5%'
        WHEN monetary >= (SELECT quantile(0.90)(monetary)
                          FROM analytics.customer_features)
             THEN 'Top 10%'
        ELSE 'Top 25%'
    END                                                  AS value_tier
FROM analytics.customer_features
WHERE
    repeat_customer = 1
    AND monetary >= (
        SELECT quantile(0.75)(monetary)
        FROM analytics.customer_features
    )
ORDER BY monetary DESC
LIMIT 200;

-- 3. CUSTOMER EXPERIENCE ANALYTICS

-- 3.1 CX Score Generated
SELECT
    cx_category,
    COUNT(DISTINCT customer_unique_id)                   AS total_customers,
    ROUND(AVG(cx_score), 2)                              AS avg_cx_score,
    ROUND(MIN(cx_score), 2)                              AS min_cx_score,
    ROUND(MAX(cx_score), 2)                              AS max_cx_score,
    ROUND(AVG(review_component) * 100, 2)                AS avg_review_component_pct,
    ROUND(AVG(delivery_component) * 100, 2)              AS avg_delivery_component_pct,
    ROUND(AVG(complaint_component) * 100, 2)             AS avg_complaint_component_pct,
    ROUND(AVG(loyalty_component) * 100, 2)               AS avg_loyalty_component_pct
FROM analytics.cx_features
GROUP BY cx_category
ORDER BY avg_cx_score DESC;

-- 3.2 Review Analysis 
SELECT
    ROUND(toFloat64(review_score), 0)                    AS star_rating,
    COUNT()                                              AS total_reviews,
    ROUND(COUNT() * 100.0 / SUM(COUNT()) OVER (), 2)     AS pct_of_total,
    ROUND(AVG(CASE WHEN has_text = 1 THEN 1 ELSE 0 END)
          * 100, 2)                                      AS pct_with_text_review,
    countIf(complaint_topic = 'delivery')                AS delivery_complaints,
    countIf(complaint_topic = 'damaged_product')         AS damaged_complaints,
    countIf(complaint_topic = 'refund')                  AS refund_complaints
FROM analytics.nlp_features
WHERE review_score IS NOT NULL
GROUP BY star_rating
ORDER BY star_rating ASC;

-- 3.3 Complaint Analysis 
SELECT
    cx.cx_category,
    COUNT(DISTINCT cx.customer_unique_id)                AS total_customers,
    ROUND(AVG(cx.complaint_rate) * 100, 2)               AS avg_complaint_rate_pct,
    ROUND(AVG(cx.avg_review_score), 2)                   AS avg_review_score,
    ROUND(AVG(cx.late_delivery_rate) * 100, 2)           AS avg_late_delivery_pct,
    ROUND(AVG(cx.cx_score), 2)                           AS avg_cx_score,
    ROUND(corr(cx.complaint_rate, cx.cx_score), 4)       AS corr_complaint_vs_cx
FROM analytics.cx_features cx
GROUP BY cx.cx_category
ORDER BY avg_complaint_rate_pct DESC;

-- 3.4 Complaint Topic Classification 
SELECT
    complaint_topic,
    COUNT()                                              AS total_reviews,
    ROUND(COUNT() * 100.0 / SUM(COUNT()) OVER (), 2)     AS pct_of_all_reviews,
    ROUND(AVG(toFloat64(review_score)), 2)               AS avg_star_rating,
    countIf(toFloat64(review_score) <= 2)                AS low_rating_count,
    ROUND(countIf(toFloat64(review_score) <= 2)
          * 100.0 / COUNT(), 2)                          AS low_rating_pct
FROM analytics.nlp_features
WHERE complaint_topic IS NOT NULL
  AND has_text = 1
GROUP BY complaint_topic
ORDER BY total_reviews DESC;

-- 3.5 CX Category Segmentation
SELECT
    cx_category,
    COUNT(DISTINCT customer_unique_id)                   AS total_customers,
    ROUND(COUNT(DISTINCT customer_unique_id) * 100.0
          / SUM(COUNT(DISTINCT customer_unique_id)) OVER (), 2)
                                                         AS pct_customers,
    ROUND(AVG(cx_score), 2)                              AS avg_cx_score,
    ROUND(AVG(avg_review_score), 2)                      AS avg_review_score,
    ROUND(AVG(complaint_rate) * 100, 2)                  AS avg_complaint_rate_pct,
    ROUND(AVG(late_delivery_rate) * 100, 2)              AS avg_late_delivery_pct,
    ROUND(AVG(monetary), 2)                              AS avg_monetary
FROM analytics.cx_features cx
LEFT JOIN analytics.customer_features c USING (customer_unique_id)
GROUP BY cx_category
ORDER BY avg_cx_score DESC;

-- 4. CUSTOMER BEHAVIOR DRIVERS 

-- 4.1 Delay vs Repeat Analysis 
SELECT
    CASE
        WHEN toFloat64(delivery_delay_days) < 0   THEN 'Early (sebelum estimasi)'
        WHEN toFloat64(delivery_delay_days) = 0   THEN 'On Time (tepat estimasi)'
        WHEN toFloat64(delivery_delay_days) <= 3  THEN 'Late 1-3 hari'
        WHEN toFloat64(delivery_delay_days) <= 7  THEN 'Late 4-7 hari'
        WHEN toFloat64(delivery_delay_days) <= 14 THEN 'Late 8-14 hari'
        ELSE                                           'Late > 14 hari'
    END                                                  AS delay_bucket,
    COUNT(DISTINCT order_id)                             AS total_orders,
    ROUND(AVG(toFloat64(review_score)), 2)               AS avg_review_score,
    ROUND(SUM(repeat_customer) * 100.0
          / COUNT(DISTINCT order_id), 2)                 AS repeat_rate_pct,
    SUM(cancellation_flag)                               AS total_cancelled,
    ROUND(SUM(cancellation_flag) * 100.0
          / COUNT(DISTINCT order_id), 2)                 AS cancellation_rate_pct
FROM analytics.delivery_features
WHERE delivery_delay_days IS NOT NULL
GROUP BY delay_bucket
ORDER BY MIN(toFloat64(delivery_delay_days)) ASC;

-- 4.2 Payment vs Loyalty Analysis 
SELECT
    p.payment_type,
    p.installment_group,
    COUNT(DISTINCT p.order_id)                           AS total_orders,
    COUNT(DISTINCT d.customer_id)                        AS total_customers,
    ROUND(AVG(p.payment_value), 2)                       AS avg_payment_value,
    ROUND(AVG(c.frequency), 2)                           AS avg_frequency,
    ROUND(AVG(c.monetary), 2)                            AS avg_monetary,
    ROUND(AVG(c.avg_order_value), 2)                     AS avg_order_value,
    ROUND(SUM(c.repeat_customer) * 100.0
          / COUNT(DISTINCT d.customer_id), 2)            AS repeat_rate_pct
FROM analytics.payment_features p
LEFT JOIN analytics.delivery_features d ON p.order_id = d.order_id
LEFT JOIN analytics.customer_features c ON d.customer_unique_id = c.customer_unique_id
WHERE p.payment_type IS NOT NULL
GROUP BY p.payment_type, p.installment_group
ORDER BY avg_monetary DESC;

-- 4.3 Customer Lifecycle Analysis  
SELECT
    CASE
        WHEN recency_days <= 90
             AND customer_tenure_days >= 365  THEN 'Active Loyal (≥1 tahun)'
        WHEN recency_days <= 90
             AND customer_tenure_days >= 180  THEN 'Active Regular (≥6 bln)'
        WHEN recency_days <= 90
             AND customer_tenure_days < 180   THEN 'Active New (< 6 bln)'
        WHEN recency_days BETWEEN 91 AND 180  THEN 'At Risk (91-180 hari)'
        WHEN recency_days BETWEEN 181 AND 365 THEN 'Lapsing (181-365 hari)'
        ELSE                                       'Churned (> 365 hari)'
    END                                                  AS lifecycle_segment,
    COUNT(DISTINCT customer_unique_id)                   AS total_customers,
    ROUND(AVG(monetary), 2)                              AS avg_monetary,
    ROUND(SUM(monetary), 2)                              AS total_revenue_at_risk,
    ROUND(AVG(frequency), 2)                             AS avg_frequency,
    ROUND(AVG(recency_days), 0)                          AS avg_recency_days,
    ROUND(AVG(customer_tenure_days), 0)                  AS avg_tenure_days
FROM analytics.customer_features
GROUP BY lifecycle_segment
ORDER BY MIN(recency_days) ASC;

-- 5. GEO & SELLER ANALYTICS

-- 5.1 Customer Distribution Map
SELECT
    g.customer_state,
    COUNT(DISTINCT g.customer_unique_id)                 AS total_customers,
    ROUND(AVG(c.avg_order_value), 2)                     AS avg_order_value,
    ROUND(AVG(c.frequency), 2)                           AS avg_frequency,
    ROUND(SUM(c.repeat_customer) * 100.0
          / COUNT(DISTINCT g.customer_unique_id), 2)     AS repeat_rate_pct
FROM analytics.geo_features g
LEFT JOIN analytics.customer_features c
    ON g.customer_unique_id = c.customer_unique_id
WHERE g.customer_state IS NOT NULL
  AND g.geolocation_lat IS NOT NULL
GROUP BY g.customer_state
ORDER BY ROUND(SUM(c.monetary), 2) DESC;

-- 5.2 Revenue by State Analysis
SELECT
    g.customer_state,
    ROUND(SUM(c.monetary), 2)                            AS total_revenue,
    ROUND(SUM(c.monetary) * 100.0
          / SUM(SUM(c.monetary)) OVER (), 2)             AS pct_of_total_revenue,
    COUNT(DISTINCT s.order_id)                           AS total_orders_with_seller,
    ROUND(AVG(toFloat64(s.same_state)) * 100, 2)         AS intrastate_order_pct
FROM analytics.geo_features g
LEFT JOIN analytics.customer_features c
    ON g.customer_unique_id = c.customer_unique_id
LEFT JOIN analytics.delivery_features d
    ON d.customer_unique_id = g.customer_unique_id
LEFT JOIN analytics.seller_features s
    ON s.order_id = d.order_id
WHERE g.customer_state IS NOT NULL
GROUP BY g.customer_state
ORDER BY total_revenue DESC
LIMIT 20;