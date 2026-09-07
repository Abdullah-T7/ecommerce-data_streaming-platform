CREATE TABLE IF NOT EXISTS orders (
    event_id        VARCHAR(100) PRIMARY KEY,
    event_type      VARCHAR(50) NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    order_id        VARCHAR(50) NOT NULL,
    customer_id     VARCHAR(50) NOT NULL,
    customer_name   VARCHAR(255),
    product         VARCHAR(100),
    quantity        INTEGER,
    amount          NUMERIC(12, 2),
    country         VARCHAR(100),
    created_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payments (
    event_id        VARCHAR(100) PRIMARY KEY,
    event_type      VARCHAR(50) NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    order_id        VARCHAR(50) NOT NULL,
    customer_id     VARCHAR(50) NOT NULL,
    amount          NUMERIC(12, 2),
    payment_method  VARCHAR(50),
    payment_status  VARCHAR(50),
    created_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS shipments (
    event_id         VARCHAR(100) PRIMARY KEY,
    event_type       VARCHAR(50) NOT NULL,
    event_time       TIMESTAMPTZ NOT NULL,
    order_id         VARCHAR(50) NOT NULL,
    customer_id      VARCHAR(50) NOT NULL,
    carrier          VARCHAR(50),
    tracking_number  VARCHAR(100),
    shipment_status  VARCHAR(50),
    created_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orders_event_time
    ON orders(event_time);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON orders(customer_id);

CREATE INDEX IF NOT EXISTS idx_orders_order_id
    ON orders(order_id);
