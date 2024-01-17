-- Make the "name" column of the protocol type unique
ALTER TABLE protocol_type
    ADD CONSTRAINT unique_name_constraint UNIQUE (name);

-- Change the name of the financial and implementation type enums
CREATE TYPE financial_type AS ENUM(
    'swap',
    'psm',
    'debt',
    'leverage'
);

ALTER TABLE protocol_type
ALTER COLUMN financial_type TYPE financial_type
USING protocol_type::text::financial_type;

DROP TYPE financial_protocol_type;

CREATE TYPE implementation_type AS ENUM(
    'custom',
    'vm'
);

ALTER TABLE protocol_type
ALTER COLUMN "implementation" TYPE implementation_type
USING protocol_type::text::implementation_type;

DROP TYPE protocol_implementation_type;