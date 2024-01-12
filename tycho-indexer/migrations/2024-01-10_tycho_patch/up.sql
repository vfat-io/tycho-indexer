CREATE TYPE protocol_system_type AS ENUM(
    'ambient'
);

ALTER TABLE protocol_system
ALTER COLUMN "name" TYPE protocol_system_type USING "name"::protocol_system_type;