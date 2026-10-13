create table orders (
    id          uuid        primary key,
    status      text        not null check (status in ('NEW', 'IN_PROGRESS', 'COMPLETED', 'FAILED')),
    created_at  timestamptz not null default now(),
    updated_at  timestamptz not null default now()
);

create index orders_created_at on orders (created_at desc);

-- One pick job per order line.
create table pick_jobs (
    id          uuid        primary key,
    order_id    uuid        not null references orders (id),
    sku         text        not null,
    qty         int         not null check (qty > 0),
    pick_x      int         not null,
    pick_y      int         not null,
    status      text        not null check (status in ('PENDING', 'ASSIGNED', 'PICKED', 'COMPLETED', 'FAILED')),
    created_at  timestamptz not null default now(),
    updated_at  timestamptz not null default now()
);

create index pick_jobs_order_id on pick_jobs (order_id);

-- Transactional outbox: rows are written in the same transaction as the business change and
-- published afterwards by OutboxRelay, so a message is sent if and only if the change committed.
create table outbox (
    id            bigserial   primary key,
    destination   text        not null check (destination in ('KAFKA', 'RABBIT')),
    topic         text        not null,   -- Kafka topic, or RabbitMQ exchange
    message_key   text        not null,   -- Kafka record key, or RabbitMQ routing key
    message_id    uuid        not null unique,
    payload       text        not null,   -- JSON, sent byte for byte
    created_at    timestamptz not null default now(),
    published_at  timestamptz
);

create index outbox_unpublished on outbox (id) where published_at is null;
