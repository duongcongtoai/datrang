CREATE TABLE if not exists event (
    id BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL
);

CREATE PUBLICATION app_slot_pub FOR TABLE event;