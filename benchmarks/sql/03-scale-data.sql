-- Scale up data for benchmarks: 1000 users, 1000 products, 10000 orders

-- Generate 995 more users (already have 5)
INSERT INTO users (email, name, password_hash)
SELECT
    'user' || generate_series || '@example.com',
    'User ' || generate_series,
    'hashed_password'
FROM generate_series(6, 1000);

-- Generate 990 more products (already have 10)
INSERT INTO products (sku, name, description, price, cost, stock_quantity, category, is_active)
SELECT
    'SKU-' || LPAD(generate_series::text, 6, '0'),
    'Product ' || generate_series,
    'Description for product ' || generate_series,
    (random() * 500 + 10)::numeric(10,2),
    (random() * 200 + 5)::numeric(10,2),
    (random() * 500)::int,
    (ARRAY['Electronics', 'Accessories', 'Audio', 'Office', 'Gaming'])[1 + (random() * 4)::int],
    random() > 0.1
FROM generate_series(11, 1000);

-- Generate 9992 more orders (already have 8)
INSERT INTO orders (order_number, user_id, status, total_amount, shipping_address)
SELECT
    'ORD-BENCH-' || LPAD(generate_series::text, 8, '0'),
    1 + (random() * 999)::int,
    (ARRAY['pending', 'confirmed', 'processing', 'shipped', 'delivered'])[1 + (random() * 4)::int],
    (random() * 2000 + 50)::numeric(10,2),
    generate_series || ' Benchmark St, City ' || (generate_series % 100) || ', ST ' || (10000 + generate_series % 90000)
FROM generate_series(9, 10000);

-- Generate order items (1-3 items per order)
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT
    o.id,
    1 + (random() * 999)::int,
    1 + (random() * 3)::int,
    (random() * 500 + 10)::numeric(10,2)
FROM orders o
CROSS JOIN generate_series(1, 1 + (random() * 2)::int)
WHERE o.id > 8;

-- Analyze tables for query planner
ANALYZE users;
ANALYZE products;
ANALYZE orders;
ANALYZE order_items;
