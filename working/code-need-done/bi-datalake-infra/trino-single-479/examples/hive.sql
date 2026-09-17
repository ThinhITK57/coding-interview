CREATE TABLE hive.default.orders (
    order_id BIGINT,
    customer_id BIGINT,
    status VARCHAR,
    total_price DECIMAL(10, 2),
    order_date DATE
)
WITH (
    format = 'ORC', -- Định dạng phổ biến và tối ưu cho Hive
    external_location = 's3a://hive-tables/examples/orders/'
);

-- Chèn dữ liệu thủ công
INSERT INTO hive.default.orders VALUES 
(1, 101, 'COMPLETED', 150.50, DATE '2026-03-29');

-- Chèn dữ liệu từ bảng khác (ETL)
INSERT INTO hive.default.orders
SELECT id, cust_id, status, price, cast(created_at as DATE)
FROM staging.raw_orders;


INSERT INTO hive.default.orders (order_id, customer_id, status, total_price, order_date) VALUES
(1, 101, 'PENDING', 150.50, DATE '2026-03-01'),
(2, 102, 'COMPLETED', 299.99, DATE '2026-03-02'),
(3, 103, 'CANCELLED', 75.00, DATE '2026-03-03'),
(4, 101, 'COMPLETED', 500.00, DATE '2026-03-05'),
(5, 104, 'PENDING', 1200.75, DATE '2026-03-07');