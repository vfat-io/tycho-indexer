CREATE TYPE protocol_system_type_enum AS ENUM(
    'ambient'
);

CREATE TABLE protocol_system_type (
    "id" BIGSERIAL PRIMARY KEY,
    "protocol_enum" protocol_system_type_enum UNIQUE NOT NULL
);