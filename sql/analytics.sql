TRUNCATE TABLE fact_orders;
TRUNCATE TABLE dim_customer;
TRUNCATE TABLE dim_product;

INSERT INTO dim_customer
SELECT
    user_id,
    MAX(country),
    MAX(customer_segment)
FROM stg_orders
GROUP BY user_id;

INSERT INTO dim_product
SELECT
    product_id,
    MAX(category)
FROM stg_orders
GROUP BY product_id;

INSERT INTO fact_orders
SELECT
    order_id,
    user_id,
    product_id,
    order_timestamp,
    qty,
    price,
    total_price
FROM stg_orders;
