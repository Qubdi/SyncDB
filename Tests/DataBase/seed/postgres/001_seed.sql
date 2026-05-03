-- ── Schema ────────────────────────────────────────────────────────────────────
-- Drop in reverse FK dependency order so re-runs are safe.
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS sync_audit;
DROP TABLE IF EXISTS datatype_samples;

CREATE TABLE customers (
    customer_id integer PRIMARY KEY,
    full_name varchar(120) NOT NULL,
    email varchar(160) NOT NULL,
    country varchar(40) NOT NULL,
    signup_ts timestamp NOT NULL,
    credit_score integer NULL,
    is_active boolean NOT NULL
);

CREATE TABLE products (
    product_id integer PRIMARY KEY,
    product_name varchar(120) NOT NULL,
    category varchar(40) NOT NULL,
    unit_price numeric(12, 2) NOT NULL,
    is_available boolean NOT NULL,
    created_at timestamp NOT NULL
);

CREATE TABLE orders (
    order_id bigint PRIMARY KEY,
    customer_id integer NOT NULL,
    product_id integer NOT NULL,
    order_ts timestamp NOT NULL,
    quantity integer NOT NULL,
    unit_price numeric(12, 2) NOT NULL,
    status varchar(20) NOT NULL,
    updated_at timestamp NOT NULL
);

CREATE TABLE payments (
    payment_id bigint PRIMARY KEY,
    order_id bigint NOT NULL,
    paid_at timestamp NULL,
    amount numeric(14, 2) NOT NULL,
    payment_method varchar(30) NOT NULL,
    is_success boolean NOT NULL
);

CREATE TABLE sync_audit (
    audit_id integer PRIMARY KEY,
    table_name varchar(80) NOT NULL,
    run_ts timestamp NOT NULL,
    row_count bigint NOT NULL,
    checksum_value varchar(64) NULL
);

CREATE TABLE datatype_samples (
    sample_id integer PRIMARY KEY,
    uuid_value uuid NOT NULL,
    small_value smallint NOT NULL,
    integer_value integer NOT NULL,
    bigint_value bigint NOT NULL,
    numeric_value numeric(38, 10) NOT NULL,
    double_value double precision NOT NULL,
    real_value real NOT NULL,
    money_value money NOT NULL,
    date_value date NOT NULL,
    time_value time(3) NOT NULL,
    timestamp_value timestamp(6) NOT NULL,
    timestamptz_value timestamptz NOT NULL,
    interval_value interval NOT NULL,
    fixed_char char(10) NOT NULL,
    varchar_value varchar(255) NOT NULL,
    text_value text NOT NULL,
    bytea_value bytea NOT NULL,
    json_value json NOT NULL,
    jsonb_value jsonb NOT NULL,
    inet_value inet NOT NULL,
    cidr_value cidr NOT NULL,
    macaddr_value macaddr NOT NULL,
    bit_value bit(8) NOT NULL,
    varbit_value bit varying(32) NOT NULL,
    text_array text[] NOT NULL,
    integer_array integer[] NOT NULL,
    point_value point NOT NULL,
    nullable_text text NULL
);

-- ── Seed Data ─────────────────────────────────────────────────────────────────
INSERT INTO customers (customer_id, full_name, email, country, signup_ts, credit_score, is_active)
SELECT
    id,
    'Customer ' || id,
    'customer' || id || '@example.test',
    CASE id % 6
        WHEN 0 THEN 'Georgia'
        WHEN 1 THEN 'United States'
        WHEN 2 THEN 'Germany'
        WHEN 3 THEN 'United Kingdom'
        WHEN 4 THEN 'France'
        ELSE 'Netherlands'
    END,
    timestamp '2023-01-01 00:00:00' + (id || ' minutes')::interval,
    CASE WHEN id % 13 = 0 THEN NULL ELSE 300 + (id % 551) END,
    id % 10 <> 0
FROM generate_series(1, 250000) AS id;

INSERT INTO products (product_id, product_name, category, unit_price, is_available, created_at)
SELECT
    id,
    'Product ' || id,
    CASE id % 5
        WHEN 0 THEN 'Cards'
        WHEN 1 THEN 'Loans'
        WHEN 2 THEN 'Deposits'
        WHEN 3 THEN 'Insurance'
        ELSE 'Digital'
    END,
    (5.00 + (id % 300) * 1.17)::numeric(12, 2),
    id % 17 <> 0,
    timestamp '2022-01-01 00:00:00' + ((id % 365) || ' days')::interval
FROM generate_series(1, 2500) AS id;

INSERT INTO orders (order_id, customer_id, product_id, order_ts, quantity, unit_price, status, updated_at)
SELECT
    id,
    ((id - 1) % 250000) + 1,
    ((id - 1) % 2500) + 1,
    timestamp '2024-01-01 00:00:00' + ((id % 525600) || ' minutes')::interval,
    (id % 7) + 1,
    (5.00 + (id % 300) * 1.17)::numeric(12, 2),
    CASE id % 5
        WHEN 0 THEN 'created'
        WHEN 1 THEN 'paid'
        WHEN 2 THEN 'shipped'
        WHEN 3 THEN 'cancelled'
        ELSE 'closed'
    END,
    timestamp '2024-01-01 00:00:00' + (((id % 525600) + 30) || ' minutes')::interval
FROM generate_series(1, 1000000) AS id;

INSERT INTO payments (payment_id, order_id, paid_at, amount, payment_method, is_success)
SELECT
    order_id,
    order_id,
    CASE WHEN order_id % 5 = 3 THEN NULL ELSE order_ts + interval '10 minutes' END,
    (quantity * unit_price)::numeric(14, 2),
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
    timestamp '2025-01-01 00:00:00' + (id || ' hours')::interval,
    CASE id % 4
        WHEN 0 THEN 250000
        WHEN 1 THEN 2500
        ELSE 1000000
    END,
    md5('syncdb-' || id)
FROM generate_series(1, 500) AS id;

INSERT INTO datatype_samples (
    sample_id,
    uuid_value,
    small_value,
    integer_value,
    bigint_value,
    numeric_value,
    double_value,
    real_value,
    money_value,
    date_value,
    time_value,
    timestamp_value,
    timestamptz_value,
    interval_value,
    fixed_char,
    varchar_value,
    text_value,
    bytea_value,
    json_value,
    jsonb_value,
    inet_value,
    cidr_value,
    macaddr_value,
    bit_value,
    varbit_value,
    text_array,
    integer_array,
    point_value,
    nullable_text
)
SELECT
    id,
    ('00000000-0000-0000-0000-' || lpad(id::text, 12, '0'))::uuid,
    (id * 3)::smallint,
    id * 100,
    id::bigint * 1000000,
    (id * 12345.678901)::numeric(38, 10),
    id * 1.2345,
    (id * 1.5)::real,
    (id * 100.25)::money,
    date '2024-01-01' + id,
    time '00:00:00' + ((id * 17) || ' seconds')::interval,
    timestamp '2024-01-01 00:00:00' + (id || ' microseconds')::interval,
    timestamptz '2024-01-01 00:00:00+04' + (id || ' minutes')::interval,
    (id || ' days')::interval + ((id * 3) || ' seconds')::interval,
    left('char-' || id || '          ', 10),
    'varchar sample ' || id,
    'text sample ' || id || ' for postgres',
    decode(md5('bytea-' || id), 'hex'),
    json_build_object('sample_id', id, 'engine', 'postgres', 'active', id % 2 = 0),
    jsonb_build_object('sample_id', id, 'engine', 'postgres', 'tags', ARRAY['sync', 'type', id::text]),
    ('192.168.10.' || ((id % 250) + 1))::inet,
    ('10.' || (id % 250) || '.0.0/16')::cidr,
    ('08:00:2b:01:02:' || lpad(to_hex(id % 255), 2, '0'))::macaddr,
    (id % 256)::bit(8),
    ((id * 1024 + id)::bit(32))::varbit,
    ARRAY['alpha', 'beta', 'sample-' || id],
    ARRAY[id, id * 2, id * 3],
    point(id, id * 2),
    CASE WHEN id % 5 = 0 THEN NULL ELSE 'nullable sample ' || id END
FROM generate_series(1, 25) AS id;

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX ix_orders_customer_id ON orders(customer_id);
CREATE INDEX ix_orders_product_id ON orders(product_id);
CREATE INDEX ix_orders_order_ts ON orders(order_ts);
CREATE INDEX ix_payments_order_id ON payments(order_id);
