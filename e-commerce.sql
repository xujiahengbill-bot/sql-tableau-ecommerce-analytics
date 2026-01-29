use chicago_crime;

select * from ecommerce_dataset_10000;

DROP TABLE IF EXISTS fact_orders;

CREATE TABLE fact_orders AS
SELECT
  customer_id,
  product_id,
  product_name,
  category,
  gender,
  age_group,
  country,
  CAST(signup_date AS DATE) AS signup_date,
  order_id,
  CAST(order_date AS DATETIME) AS order_date,
  order_status,
  payment_method,
  CAST(quantity AS UNSIGNED) AS quantity,
  CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
  CAST(quantity AS UNSIGNED) * CAST(unit_price AS DECIMAL(10,2)) AS order_value,
  CAST(rating AS UNSIGNED) AS rating,
  review_id,
  CAST(review_date AS DATE) AS review_date
FROM `ecommerce_dataset_10000`;

DROP TABLE IF EXISTS kpi_orders_daily;

CREATE TABLE kpi_orders_daily AS
SELECT
  DATE(order_date) AS order_dt,
  country,
  category,
  age_group,
  COUNT(DISTINCT order_id) AS orders,
  COUNT(DISTINCT customer_id) AS active_customers,
  SUM(order_value) AS revenue,
  SUM(order_value) / COUNT(DISTINCT order_id) AS avg_order_value,
  AVG(rating) AS avg_rating
FROM fact_orders
WHERE order_status = 'Delivered'
GROUP BY
  DATE(order_date), country, category, age_group;

select * from kpi_orders_daily;
DROP TABLE IF EXISTS kpi_category_monthly;

CREATE TABLE kpi_category_monthly AS
SELECT
  DATE_FORMAT(order_date, '%Y-%m-01') AS month_dt,
  category,
  COUNT(DISTINCT order_id) AS orders,
  SUM(order_value) AS revenue,
  SUM(order_value) / COUNT(DISTINCT order_id) AS aov,
  AVG(rating) AS avg_rating
FROM fact_orders
WHERE order_status = 'Delivered'
GROUP BY DATE_FORMAT(order_date, '%Y-%m-01'), category;

DROP TABLE IF EXISTS kpi_cohort_retention;

CREATE TABLE kpi_cohort_retention AS
WITH cohort_size AS (
  SELECT
    cohort_month,
    MAX(active_customers) AS cohort_users
  FROM cohort_activity
  WHERE cohort_month = activity_month
  GROUP BY cohort_month
)
SELECT
  a.cohort_month,
  a.activity_month,
  a.active_customers,
  s.cohort_users,
  a.active_customers * 1.0 / s.cohort_users AS retention_rate
FROM cohort_activity a
JOIN cohort_size s
  ON a.cohort_month = s.cohort_month
ORDER BY a.cohort_month, a.activity_month;

DROP TABLE IF EXISTS kpi_returns_monthly;

CREATE TABLE kpi_returns_monthly AS
SELECT
  DATE_FORMAT(order_date, '%Y-%m-01') AS month_dt,
  category,
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT CASE WHEN order_status = 'Returned' THEN order_id END) AS returned_orders,
  COUNT(DISTINCT CASE WHEN order_status = 'Returned' THEN order_id END) * 1.0
    / COUNT(DISTINCT order_id) AS return_rate
FROM fact_orders
GROUP BY DATE_FORMAT(order_date, '%Y-%m-01'), category;

DROP TABLE IF EXISTS funnel_customer_milestones;

CREATE TABLE funnel_customer_milestones AS
SELECT
  customer_id,
  MIN(signup_date) AS signup_date,
  MIN(order_date) AS first_order_date,
  MIN(CASE WHEN order_status = 'Delivered' THEN order_date END) AS first_delivered_date
FROM fact_orders
GROUP BY customer_id;

DROP TABLE IF EXISTS kpi_funnel_signup_month;

CREATE TABLE kpi_funnel_signup_month AS
SELECT
  DATE_FORMAT(signup_date, '%Y-%m-01') AS signup_month,
  COUNT(*) AS signed_up_customers,
  COUNT(CASE WHEN first_order_date IS NOT NULL THEN 1 END) AS customers_with_orders,
  COUNT(CASE WHEN first_delivered_date IS NOT NULL THEN 1 END) AS customers_with_deliveries,
  COUNT(CASE WHEN first_order_date IS NOT NULL THEN 1 END) * 1.0 / COUNT(*) AS signup_to_order_rate,
  COUNT(CASE WHEN first_delivered_date IS NOT NULL THEN 1 END) * 1.0
    / NULLIF(COUNT(CASE WHEN first_order_date IS NOT NULL THEN 1 END),0) AS order_to_delivery_rate
FROM funnel_customer_milestones
GROUP BY DATE_FORMAT(signup_date, '%Y-%m-01');

DROP TABLE IF EXISTS cohort_activity;

CREATE TABLE cohort_activity AS
WITH first_delivered AS (
  SELECT
    customer_id,
    DATE_FORMAT(MIN(order_date), '%Y-%m-01') AS cohort_month
  FROM fact_orders
  WHERE order_status = 'Delivered'
  GROUP BY customer_id
),
activity AS (
  SELECT
    f.customer_id,
    f.cohort_month,
    DATE_FORMAT(o.order_date, '%Y-%m-01') AS activity_month
  FROM first_delivered f
  JOIN fact_orders o
    ON f.customer_id = o.customer_id
  WHERE o.order_status = 'Delivered'
)
SELECT
  cohort_month,
  activity_month,
  COUNT(DISTINCT customer_id) AS active_customers
FROM activity
GROUP BY cohort_month, activity_month;


DROP TABLE IF EXISTS kpi_segment_country_age;

CREATE TABLE kpi_segment_country_age AS
SELECT
  country,
  age_group,
  COUNT(DISTINCT customer_id) AS customers,
  SUM(CASE WHEN order_status='Delivered' THEN order_value ELSE 0 END) AS delivered_revenue,
  AVG(CASE WHEN order_status='Delivered' THEN rating END) AS avg_rating
FROM fact_orders
GROUP BY country, age_group;



