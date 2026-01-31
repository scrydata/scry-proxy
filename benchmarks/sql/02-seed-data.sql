-- Seed data for Scry Platform integration testing
--
-- Provides realistic test data for:
-- - Query capture through scry-proxy
-- - CDC replication through scry-backfill
-- - Replay comparison in scry-platform

-- Users (5 sample users)
INSERT INTO users (email, name, password_hash) VALUES
    ('alice@example.com', 'Alice Johnson', 'hashed_alice_password'),
    ('bob@example.com', 'Bob Smith', 'hashed_bob_password'),
    ('carol@example.com', 'Carol Williams', 'hashed_carol_password'),
    ('david@example.com', 'David Brown', 'hashed_david_password'),
    ('eve@example.com', 'Eve Davis', 'hashed_eve_password');

-- Products (10 sample products across categories)
INSERT INTO products (sku, name, description, price, cost, stock_quantity, category) VALUES
    ('LAPTOP-001', 'Pro Laptop 15"', 'High-performance laptop with 16GB RAM and 512GB SSD', 1299.99, 899.99, 50, 'Electronics'),
    ('LAPTOP-002', 'Budget Laptop 14"', 'Affordable laptop for everyday use', 549.99, 349.99, 100, 'Electronics'),
    ('MOUSE-001', 'Wireless Mouse', 'Ergonomic wireless mouse with 6 buttons', 49.99, 19.99, 200, 'Accessories'),
    ('MOUSE-002', 'Gaming Mouse', 'High-DPI gaming mouse with RGB lighting', 79.99, 34.99, 150, 'Accessories'),
    ('KEYBOARD-001', 'Mechanical Keyboard', 'Cherry MX Blue switches with RGB backlight', 129.99, 69.99, 100, 'Accessories'),
    ('KEYBOARD-002', 'Wireless Keyboard', 'Slim wireless keyboard with numeric pad', 69.99, 29.99, 175, 'Accessories'),
    ('MONITOR-001', '27" 4K Monitor', 'Ultra HD IPS display with HDR support', 599.99, 399.99, 30, 'Electronics'),
    ('MONITOR-002', '24" FHD Monitor', 'Full HD monitor for office use', 249.99, 149.99, 75, 'Electronics'),
    ('HEADSET-001', 'Noise Canceling Headset', 'Professional audio headset with ANC', 199.99, 89.99, 75, 'Audio'),
    ('HEADSET-002', 'Budget Headphones', 'Comfortable over-ear headphones', 39.99, 14.99, 250, 'Audio');

-- Orders (8 sample orders with various statuses)
INSERT INTO orders (order_number, user_id, status, total_amount, shipping_address, notes) VALUES
    ('ORD-2024-0001', 1, 'delivered', 1349.98, '123 Main St, Seattle, WA 98101', 'Leave at door'),
    ('ORD-2024-0002', 2, 'delivered', 649.98, '456 Oak Ave, Portland, OR 97201', NULL),
    ('ORD-2024-0003', 3, 'shipped', 329.98, '789 Pine Rd, San Francisco, CA 94102', 'Fragile'),
    ('ORD-2024-0004', 1, 'processing', 199.99, '123 Main St, Seattle, WA 98101', NULL),
    ('ORD-2024-0005', 4, 'confirmed', 1429.98, '321 Elm St, Los Angeles, CA 90001', 'Gift wrap please'),
    ('ORD-2024-0006', 5, 'pending', 89.98, '654 Maple Dr, Denver, CO 80201', NULL),
    ('ORD-2024-0007', 2, 'cancelled', 549.99, '456 Oak Ave, Portland, OR 97201', 'Customer cancelled'),
    ('ORD-2024-0008', 3, 'pending', 879.97, '789 Pine Rd, San Francisco, CA 94102', 'Rush delivery');

-- Order items (items for each order)
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    -- Order 1: Laptop + Mouse
    (1, 1, 1, 1299.99),
    (1, 3, 1, 49.99),
    -- Order 2: Monitor + Mouse
    (2, 7, 1, 599.99),
    (2, 3, 1, 49.99),
    -- Order 3: Keyboard + Headset
    (3, 5, 1, 129.99),
    (3, 1, 1, 199.99),
    -- Order 4: Headset
    (4, 9, 1, 199.99),
    -- Order 5: Laptop + Keyboard
    (5, 1, 1, 1299.99),
    (5, 5, 1, 129.99),
    -- Order 6: Mouse + Headphones
    (6, 3, 1, 49.99),
    (6, 10, 1, 39.99),
    -- Order 7: Budget Laptop (cancelled)
    (7, 2, 1, 549.99),
    -- Order 8: Monitor + Keyboard + Mouse
    (8, 7, 1, 599.99),
    (8, 6, 1, 69.99),
    (8, 4, 1, 79.99),
    (8, 5, 1, 129.99);

-- Sample audit log entries
INSERT INTO audit_log (table_name, record_id, action, new_data, user_id) VALUES
    ('users', 1, 'INSERT', '{"email": "alice@example.com", "name": "Alice Johnson"}', NULL),
    ('users', 2, 'INSERT', '{"email": "bob@example.com", "name": "Bob Smith"}', NULL),
    ('orders', 1, 'INSERT', '{"order_number": "ORD-2024-0001", "status": "pending"}', 1),
    ('orders', 1, 'UPDATE', '{"status": "delivered"}', 1),
    ('products', 1, 'UPDATE', '{"stock_quantity": 49}', NULL);

-- Add some comments explaining the data
COMMENT ON TABLE users IS 'Sample users for demo - passwords are placeholders';
COMMENT ON TABLE products IS 'Sample products across multiple categories';
COMMENT ON TABLE orders IS 'Sample orders in various lifecycle states';
COMMENT ON TABLE audit_log IS 'Sample audit entries showing change tracking';
