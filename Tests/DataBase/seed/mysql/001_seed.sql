DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS sync_audit;
DROP TABLE IF EXISTS datatype_samples;

CREATE TABLE customers (
    customer_id INT NOT NULL PRIMARY KEY,
    full_name VARCHAR(120) NOT NULL,
    email VARCHAR(160) NOT NULL,
    country VARCHAR(40) NOT NULL,
    signup_ts DATETIME NOT NULL,
    credit_score INT NULL,
    is_active BOOLEAN NOT NULL
);

CREATE TABLE products (
    product_id INT NOT NULL PRIMARY KEY,
    product_name VARCHAR(120) NOT NULL,
    category VARCHAR(40) NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL,
    is_available BOOLEAN NOT NULL,
    created_at DATETIME NOT NULL
);

CREATE TABLE orders (
    order_id BIGINT NOT NULL PRIMARY KEY,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    order_ts DATETIME NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    updated_at DATETIME NOT NULL
);

CREATE TABLE payments (
    payment_id BIGINT NOT NULL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    paid_at DATETIME NULL,
    amount DECIMAL(14, 2) NOT NULL,
    payment_method VARCHAR(30) NOT NULL,
    is_success BOOLEAN NOT NULL
);

CREATE TABLE sync_audit (
    audit_id INT NOT NULL PRIMARY KEY,
    table_name VARCHAR(80) NOT NULL,
    run_ts DATETIME NOT NULL,
    row_count BIGINT NOT NULL,
    checksum_value VARCHAR(64) NULL
);

CREATE TABLE datatype_samples (
    sample_id INT NOT NULL PRIMARY KEY,
    tiny_value TINYINT NOT NULL,
    tiny_unsigned TINYINT UNSIGNED NOT NULL,
    small_unsigned SMALLINT UNSIGNED NOT NULL,
    medium_value MEDIUMINT NOT NULL,
    int_unsigned INT UNSIGNED NOT NULL,
    big_unsigned BIGINT UNSIGNED NOT NULL,
    decimal_value DECIMAL(38, 10) NOT NULL,
    float_value FLOAT NOT NULL,
    double_value DOUBLE NOT NULL,
    date_value DATE NOT NULL,
    time_value TIME(3) NOT NULL,
    datetime_value DATETIME(6) NOT NULL,
    timestamp_value TIMESTAMP(6) NULL,
    year_value YEAR NOT NULL,
    fixed_char CHAR(10) NOT NULL,
    varchar_value VARCHAR(255) NOT NULL,
    text_value TEXT NOT NULL,
    binary_value BINARY(16) NOT NULL,
    varbinary_value VARBINARY(64) NOT NULL,
    blob_value BLOB NOT NULL,
    json_value JSON NOT NULL,
    enum_value ENUM('created', 'paid', 'failed', 'closed') NOT NULL,
    set_value SET('sync', 'api', 'bulk', 'staging') NOT NULL,
    bit_value BIT(8) NOT NULL,
    nullable_text VARCHAR(100) NULL
);

CREATE TEMPORARY TABLE seq_1000000 (id INT PRIMARY KEY);

INSERT INTO seq_1000000 (id)
SELECT ones.n + tens.n * 10 + hundreds.n * 100 + thousands.n * 1000 + ten_thousands.n * 10000 + hundred_thousands.n * 100000 + 1 AS id
FROM
    (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) ones
    CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) tens
    CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) hundreds
    CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) thousands
    CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) ten_thousands
    CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) hundred_thousands
WHERE ones.n + tens.n * 10 + hundreds.n * 100 + thousands.n * 1000 + ten_thousands.n * 10000 + hundred_thousands.n * 100000 + 1 <= 1000000;

INSERT INTO customers (customer_id, full_name, email, country, signup_ts, credit_score, is_active)
SELECT
    id,
    CONCAT('Customer ', id),
    CONCAT('customer', id, '@example.test'),
    CASE id % 6
        WHEN 0 THEN 'Georgia'
        WHEN 1 THEN 'United States'
        WHEN 2 THEN 'Germany'
        WHEN 3 THEN 'United Kingdom'
        WHEN 4 THEN 'France'
        ELSE 'Netherlands'
    END,
    DATE_ADD('2023-01-01 00:00:00', INTERVAL id MINUTE),
    CASE WHEN id % 13 = 0 THEN NULL ELSE 300 + (id % 551) END,
    id % 10 <> 0
FROM seq_1000000
WHERE id <= 250000;

INSERT INTO products (product_id, product_name, category, unit_price, is_available, created_at)
SELECT
    id,
    CONCAT('Product ', id),
    CASE id % 5
        WHEN 0 THEN 'Cards'
        WHEN 1 THEN 'Loans'
        WHEN 2 THEN 'Deposits'
        WHEN 3 THEN 'Insurance'
        ELSE 'Digital'
    END,
    CAST(5.00 + (id % 300) * 1.17 AS DECIMAL(12, 2)),
    id % 17 <> 0,
    DATE_ADD('2022-01-01 00:00:00', INTERVAL (id % 365) DAY)
FROM seq_1000000
WHERE id <= 2500;

INSERT INTO orders (order_id, customer_id, product_id, order_ts, quantity, unit_price, status, updated_at)
SELECT
    id,
    ((id - 1) % 250000) + 1,
    ((id - 1) % 2500) + 1,
    DATE_ADD('2024-01-01 00:00:00', INTERVAL (id % 525600) MINUTE),
    (id % 7) + 1,
    CAST(5.00 + (id % 300) * 1.17 AS DECIMAL(12, 2)),
    CASE id % 5
        WHEN 0 THEN 'created'
        WHEN 1 THEN 'paid'
        WHEN 2 THEN 'shipped'
        WHEN 3 THEN 'cancelled'
        ELSE 'closed'
    END,
    DATE_ADD('2024-01-01 00:00:00', INTERVAL ((id % 525600) + 30) MINUTE)
FROM seq_1000000;

INSERT INTO payments (payment_id, order_id, paid_at, amount, payment_method, is_success)
SELECT
    order_id,
    order_id,
    CASE WHEN order_id % 5 = 3 THEN NULL ELSE DATE_ADD(order_ts, INTERVAL 10 MINUTE) END,
    CAST(quantity * unit_price AS DECIMAL(14, 2)),
    CASE order_id % 4
        WHEN 0 THEN 'card'
        WHEN 1 THEN 'transfer'
        WHEN 2 THEN 'cash'
        ELSE 'wallet'
    END,
    order_id % 11 <> 0
FROM orders;

INSERT INTO sync_audit (audit_id, table_name, run_ts, row_count, checksum_value)
SELECT
    id,
    CASE id % 4
        WHEN 0 THEN 'customers'
        WHEN 1 THEN 'products'
        WHEN 2 THEN 'orders'
        ELSE 'payments'
    END,
    DATE_ADD('2025-01-01 00:00:00', INTERVAL id HOUR),
    CASE id % 4
        WHEN 0 THEN 250000
        WHEN 1 THEN 2500
        ELSE 1000000
    END,
    MD5(CONCAT('syncdb-', id))
FROM seq_1000000
WHERE id <= 500;

INSERT INTO datatype_samples (
    sample_id,
    tiny_value,
    tiny_unsigned,
    small_unsigned,
    medium_value,
    int_unsigned,
    big_unsigned,
    decimal_value,
    float_value,
    double_value,
    date_value,
    time_value,
    datetime_value,
    timestamp_value,
    year_value,
    fixed_char,
    varchar_value,
    text_value,
    binary_value,
    varbinary_value,
    blob_value,
    json_value,
    enum_value,
    set_value,
    bit_value,
    nullable_text
)
SELECT
    id,
    CAST(id % 127 AS SIGNED),
    id % 255,
    id * 3,
    id * 100,
    id * 1000,
    id * 1000000,
    CAST(id * 12345.678901 AS DECIMAL(38, 10)),
    id * 1.5,
    id * 1.2345,
    DATE_ADD('2024-01-01', INTERVAL id DAY),
    TIME(DATE_ADD('00:00:00', INTERVAL id * 17 SECOND)),
    DATE_ADD('2024-01-01 00:00:00.000000', INTERVAL id MICROSECOND),
    DATE_ADD('2024-01-01 00:00:00.000000', INTERVAL id MINUTE),
    2020 + (id % 6),
    LEFT(CONCAT('char-', id, '          '), 10),
    CONCAT('varchar sample ', id),
    CONCAT('text sample ', id, ' for mysql'),
    UNHEX(MD5(CONCAT('binary-', id))),
    UNHEX(SHA2(CONCAT('varbinary-', id), 256)),
    UNHEX(SHA2(CONCAT('blob-', id), 512)),
    JSON_OBJECT('sample_id', id, 'engine', 'mysql', 'active', id % 2 = 0),
    CASE id % 4
        WHEN 0 THEN 'created'
        WHEN 1 THEN 'paid'
        WHEN 2 THEN 'failed'
        ELSE 'closed'
    END,
    CASE id % 4
        WHEN 0 THEN 'sync,api'
        WHEN 1 THEN 'bulk'
        WHEN 2 THEN 'api,staging'
        ELSE 'sync,bulk,staging'
    END,
    id % 256,
    CASE WHEN id % 5 = 0 THEN NULL ELSE CONCAT('nullable sample ', id) END
FROM seq_1000000
WHERE id <= 25;

CREATE INDEX ix_orders_customer_id ON orders(customer_id);
CREATE INDEX ix_orders_product_id ON orders(product_id);
CREATE INDEX ix_orders_order_ts ON orders(order_ts);
CREATE INDEX ix_payments_order_id ON payments(order_id);
