TRUNCATE TABLE stg_orders;

INSERT INTO stg_orders (
    order_id,
    user_id,
    product_id,
    category,
    price,
    qty,
    total_price,
    order_timestamp,
    country,
    customer_segment
)
SELECT DISTINCT
    CAST(order_id AS UNSIGNED),
    CAST(user_id AS UNSIGNED),
    CAST(product_id AS UNSIGNED),
    category,
    CAST(price AS DECIMAL(10,2)),
    CAST(qty AS UNSIGNED),
    CAST(total_price AS DECIMAL(10,2)),
    STR_TO_DATE(order_date, '%Y-%m-%d %H:%i:%s'),
    country,
    customer_segment
FROM raw_orders
WHERE order_id IS NOT NULL
  AND product_id IS NOT NULL;
