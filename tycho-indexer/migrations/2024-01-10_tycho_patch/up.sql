CREATE TYPE protocol_system_name AS ENUM(
    'ambient'
);

CREATE TABLE protocol_system_type (
    "id" BIGSERIAL PRIMARY KEY,
    "protocol_enum" protocol_system_name UNIQUE NOT NULL
);