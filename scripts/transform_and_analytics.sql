/* =====================================================
   E-COMMERCE ANALYTICS TRANSFORMATIONS
   Raw  -> Staging -> Dimensions -> Fact
   ===================================================== */

-- =========================
-- STAGING LAYER
-- =========================
-- Clean and standardize raw data

DROP TABLE IF EXISTS stg_orders;

CREATE TABLE stg_orders AS
SELECT
    order_id,
    user_id,
    product_id,
    category,
    price,
    qty,
    total_price,
    DATE(order_date) AS order_date,
    country,
    customer_segment
FROM raw_orders;


-- =========================
-- DIMENSION: CUSTOMER
-- =========================

DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer AS
SELECT DISTINCT
    user_id,
    country,
    customer_segment
FROM stg_orders;


-- =========================
-- DIMENSION: PRODUCT
-- =========================

DROP TABLE IF EXISTS dim_product;

CREATE TABLE dim_product AS
SELECT DISTINCT
    product_id,
    category,
    price
FROM stg_orders;


-- =========================
-- FACT TABLE: ORDERS
-- =========================

DROP TABLE IF EXISTS fact_orders;

CREATE TABLE fact_orders AS
SELECT
    o.order_id,
    o.order_date,
    o.user_id,
    o.product_id,
    o.qty,
    o.total_price
FROM stg_orders o;
