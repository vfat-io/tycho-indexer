CREATE TYPE protocol_implementation_type AS ENUM(
    'custom',
    'vm'
);

ALTER TABLE protocol_type
ALTER COLUMN "implementation" TYPE protocol_implementation_type
USING implementation_type::text::protocol_implementation_type;

DROP TYPE implementation_type;

CREATE TYPE financial_protocol_type AS ENUM(
    'swap',
    'psm',
    'debt',
    'leverage'
);

ALTER TABLE protocol_type
ALTER COLUMN protocol_type TYPE financial_protocol_type
USING protocol_type::text::financial_protocol_type;

DROP TYPE financial_type;

ALTER TABLE protocol_type
DROP CONSTRAINT unique_name_constraint;

-- dropping tables will drop any triggers or indices along with it
ALTER TABLE protocol_system
ALTER COLUMN "name" TYPE varchar(255);

DROP TYPE protocol_system_type;


