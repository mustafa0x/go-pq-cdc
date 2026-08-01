CREATE TABLE public.users (
    id serial PRIMARY KEY,
    name text NOT NULL,
    created_on timestamptz
);

CREATE TABLE public.test_heartbeat_table (
    id integer PRIMARY KEY DEFAULT 1,
    last_heartbeat timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT test_heartbeat_table_single_row CHECK (id = 1)
);
