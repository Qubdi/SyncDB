IF DB_ID(N'syncdb_test') IS NULL
BEGIN
    CREATE DATABASE syncdb_test;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.sql_logins WHERE name = N'admin')
BEGIN
    CREATE LOGIN admin WITH PASSWORD = N'admin', CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF;
END
GO

USE syncdb_test;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'admin')
BEGIN
    CREATE USER admin FOR LOGIN admin;
END
GO

ALTER ROLE db_owner ADD MEMBER admin;
GO

IF OBJECT_ID(N'dbo.payments', N'U') IS NOT NULL DROP TABLE dbo.payments;
IF OBJECT_ID(N'dbo.orders', N'U') IS NOT NULL DROP TABLE dbo.orders;
IF OBJECT_ID(N'dbo.products', N'U') IS NOT NULL DROP TABLE dbo.products;
IF OBJECT_ID(N'dbo.customers', N'U') IS NOT NULL DROP TABLE dbo.customers;
IF OBJECT_ID(N'dbo.sync_audit', N'U') IS NOT NULL DROP TABLE dbo.sync_audit;
IF OBJECT_ID(N'dbo.datatype_samples', N'U') IS NOT NULL DROP TABLE dbo.datatype_samples;
GO

CREATE TABLE dbo.customers (
    customer_id INT NOT NULL PRIMARY KEY,
    full_name NVARCHAR(120) NOT NULL,
    email NVARCHAR(160) NOT NULL,
    country NVARCHAR(40) NOT NULL,
    signup_ts DATETIME2 NOT NULL,
    credit_score INT NULL,
    is_active BIT NOT NULL
);

CREATE TABLE dbo.products (
    product_id INT NOT NULL PRIMARY KEY,
    product_name NVARCHAR(120) NOT NULL,
    category NVARCHAR(40) NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL,
    is_available BIT NOT NULL,
    created_at DATETIME2 NOT NULL
);

CREATE TABLE dbo.orders (
    order_id BIGINT NOT NULL PRIMARY KEY,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    order_ts DATETIME2 NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL,
    status NVARCHAR(20) NOT NULL,
    updated_at DATETIME2 NOT NULL
);

CREATE TABLE dbo.payments (
    payment_id BIGINT NOT NULL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    paid_at DATETIME2 NULL,
    amount DECIMAL(14, 2) NOT NULL,
    payment_method NVARCHAR(30) NOT NULL,
    is_success BIT NOT NULL
);

CREATE TABLE dbo.sync_audit (
    audit_id INT NOT NULL PRIMARY KEY,
    table_name NVARCHAR(80) NOT NULL,
    run_ts DATETIME2 NOT NULL,
    row_count BIGINT NOT NULL,
    checksum_value NVARCHAR(64) NULL
);

CREATE TABLE dbo.datatype_samples (
    sample_id INT NOT NULL PRIMARY KEY,
    guid_value UNIQUEIDENTIFIER NOT NULL,
    tiny_value TINYINT NOT NULL,
    small_value SMALLINT NOT NULL,
    money_value MONEY NOT NULL,
    smallmoney_value SMALLMONEY NOT NULL,
    decimal_value DECIMAL(38, 10) NOT NULL,
    float_value FLOAT NOT NULL,
    real_value REAL NOT NULL,
    date_value DATE NOT NULL,
    time_value TIME(3) NOT NULL,
    datetime_value DATETIME NOT NULL,
    datetime2_value DATETIME2(7) NOT NULL,
    datetimeoffset_value DATETIMEOFFSET(7) NOT NULL,
    fixed_char CHAR(10) NOT NULL,
    fixed_nchar NCHAR(10) NOT NULL,
    ascii_text VARCHAR(255) NOT NULL,
    long_text NVARCHAR(MAX) NOT NULL,
    binary_value BINARY(16) NOT NULL,
    varbinary_value VARBINARY(MAX) NOT NULL,
    xml_value XML NOT NULL,
    json_value NVARCHAR(MAX) NOT NULL,
    row_version ROWVERSION NOT NULL,
    nullable_text NVARCHAR(100) NULL,
    CONSTRAINT ck_datatype_samples_json CHECK (ISJSON(json_value) = 1)
);
GO

WITH n AS (
    SELECT TOP (250000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO dbo.customers (customer_id, full_name, email, country, signup_ts, credit_score, is_active)
SELECT
    id,
    CONCAT(N'Customer ', id),
    CONCAT(N'customer', id, N'@example.test'),
    CASE id % 6
        WHEN 0 THEN N'Georgia'
        WHEN 1 THEN N'United States'
        WHEN 2 THEN N'Germany'
        WHEN 3 THEN N'United Kingdom'
        WHEN 4 THEN N'France'
        ELSE N'Netherlands'
    END,
    DATEADD(MINUTE, id, CAST('2023-01-01T00:00:00' AS DATETIME2)),
    CASE WHEN id % 13 = 0 THEN NULL ELSE 300 + (id % 551) END,
    CASE WHEN id % 10 = 0 THEN 0 ELSE 1 END
FROM n;

WITH n AS (
    SELECT TOP (2500) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO dbo.products (product_id, product_name, category, unit_price, is_available, created_at)
SELECT
    id,
    CONCAT(N'Product ', id),
    CASE id % 5
        WHEN 0 THEN N'Cards'
        WHEN 1 THEN N'Loans'
        WHEN 2 THEN N'Deposits'
        WHEN 3 THEN N'Insurance'
        ELSE N'Digital'
    END,
    CAST(5.00 + (id % 300) * 1.17 AS DECIMAL(12, 2)),
    CASE WHEN id % 17 = 0 THEN 0 ELSE 1 END,
    DATEADD(DAY, id % 365, CAST('2022-01-01T00:00:00' AS DATETIME2))
FROM n;

WITH n AS (
    SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id
    FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c
)
INSERT INTO dbo.orders (order_id, customer_id, product_id, order_ts, quantity, unit_price, status, updated_at)
SELECT
    id,
    ((id - 1) % 250000) + 1,
    ((id - 1) % 2500) + 1,
    DATEADD(MINUTE, id % 525600, CAST('2024-01-01T00:00:00' AS DATETIME2)),
    (id % 7) + 1,
    CAST(5.00 + (id % 300) * 1.17 AS DECIMAL(12, 2)),
    CASE id % 5
        WHEN 0 THEN N'created'
        WHEN 1 THEN N'paid'
        WHEN 2 THEN N'shipped'
        WHEN 3 THEN N'cancelled'
        ELSE N'closed'
    END,
    DATEADD(MINUTE, (id % 525600) + 30, CAST('2024-01-01T00:00:00' AS DATETIME2))
FROM n;

INSERT INTO dbo.payments (payment_id, order_id, paid_at, amount, payment_method, is_success)
SELECT
    order_id,
    order_id,
    CASE WHEN order_id % 5 = 3 THEN NULL ELSE DATEADD(MINUTE, 10, order_ts) END,
    CAST(quantity * unit_price AS DECIMAL(14, 2)),
    CASE order_id % 4
        WHEN 0 THEN N'card'
        WHEN 1 THEN N'transfer'
        WHEN 2 THEN N'cash'
        ELSE N'wallet'
    END,
    CASE WHEN order_id % 11 = 0 THEN 0 ELSE 1 END
FROM dbo.orders;

WITH n AS (
    SELECT TOP (500) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO dbo.sync_audit (audit_id, table_name, run_ts, row_count, checksum_value)
SELECT
    id,
    CASE id % 4
        WHEN 0 THEN N'customers'
        WHEN 1 THEN N'products'
        WHEN 2 THEN N'orders'
        ELSE N'payments'
    END,
    DATEADD(HOUR, id, CAST('2025-01-01T00:00:00' AS DATETIME2)),
    CASE id % 4
        WHEN 0 THEN 250000
        WHEN 1 THEN 2500
        ELSE 1000000
    END,
    CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', CONCAT(N'syncdb-', id)), 2)
FROM n;

WITH n AS (
    SELECT TOP (25) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id
    FROM sys.all_objects
)
INSERT INTO dbo.datatype_samples (
    sample_id,
    guid_value,
    tiny_value,
    small_value,
    money_value,
    smallmoney_value,
    decimal_value,
    float_value,
    real_value,
    date_value,
    time_value,
    datetime_value,
    datetime2_value,
    datetimeoffset_value,
    fixed_char,
    fixed_nchar,
    ascii_text,
    long_text,
    binary_value,
    varbinary_value,
    xml_value,
    json_value,
    nullable_text
)
SELECT
    id,
    CONVERT(UNIQUEIDENTIFIER, CONCAT(RIGHT(CONCAT('00000000', CONVERT(VARCHAR(8), id)), 8), '-0000-0000-0000-000000000000')),
    CONVERT(TINYINT, id % 255),
    CONVERT(SMALLINT, id * 3),
    CONVERT(MONEY, id * 100.25),
    CONVERT(SMALLMONEY, id * 10.75),
    CONVERT(DECIMAL(38, 10), id * 12345.678901),
    CONVERT(FLOAT, id * 1.2345),
    CONVERT(REAL, id * 1.5),
    DATEADD(DAY, id, CONVERT(DATE, '2024-01-01')),
    CONVERT(TIME(3), DATEADD(SECOND, id * 17, CONVERT(TIME, '00:00:00'))),
    DATEADD(MINUTE, id, CONVERT(DATETIME, '2024-01-01T00:00:00')),
    DATEADD(MICROSECOND, id, CONVERT(DATETIME2(7), '2024-01-01T00:00:00')),
    TODATETIMEOFFSET(DATEADD(MINUTE, id, CONVERT(DATETIME2(7), '2024-01-01T00:00:00')), '+04:00'),
    LEFT(CONCAT('char-', id, '          '), 10),
    LEFT(CONCAT(N'nchar-', id, N'          '), 10),
    CONCAT('varchar sample ', id),
    CONCAT(N'nvarchar max sample ', id, N' with unicode text'),
    CONVERT(BINARY(16), HASHBYTES('MD5', CONCAT('binary-', id))),
    HASHBYTES('SHA2_256', CONCAT('varbinary-', id)),
    CONVERT(XML, CONCAT('<sample id="', id, '"><name>Sample ', id, '</name></sample>')),
    CONCAT(N'{"sample_id":', id, N',"engine":"mssql","active":', CASE WHEN id % 2 = 0 THEN N'true' ELSE N'false' END, N'}'),
    CASE WHEN id % 5 = 0 THEN NULL ELSE CONCAT(N'nullable sample ', id) END
FROM n;
GO

CREATE INDEX ix_orders_customer_id ON dbo.orders(customer_id);
CREATE INDEX ix_orders_product_id ON dbo.orders(product_id);
CREATE INDEX ix_orders_order_ts ON dbo.orders(order_ts);
CREATE INDEX ix_payments_order_id ON dbo.payments(order_id);
GO
