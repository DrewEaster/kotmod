CREATE TABLE customers (
    id            text PRIMARY KEY,
    name          text        NOT NULL,
    email         text        NOT NULL,
    registered_at timestamptz NOT NULL
);

CREATE TABLE orders (
    id          text PRIMARY KEY,
    customer_id text        NOT NULL REFERENCES customers,
    status      text        NOT NULL,
    item        text        NOT NULL,
    reason      text,
    placed_at   timestamptz NOT NULL
);

CREATE TABLE order_status (
    id       text PRIMARY KEY,
    status   text   NOT NULL,
    sequence bigint NOT NULL
);
