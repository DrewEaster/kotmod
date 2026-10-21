CREATE TABLE customers (id text PRIMARY KEY, name text NOT NULL);
CREATE TABLE orders (
    id text PRIMARY KEY,
    customer_id text NOT NULL,
    status text NOT NULL,
    reason text,
    placed_at timestamptz NOT NULL
);
