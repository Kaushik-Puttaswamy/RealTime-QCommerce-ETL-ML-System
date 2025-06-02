CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    timestamp TIMESTAMP,
    items TEXT[],
    total_price FLOAT,
    location TEXT
);