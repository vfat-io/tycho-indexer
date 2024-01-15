-- dropping tables will drop any triggers or indices along with it
ALTER TABLE protocol_system
ALTER COLUMN "name" TYPE varchar(255);

-- custom types
DROP TYPE protocol_system_type;

ALTER TABLE protocol_type
DROP CONSTRAINT unique_name_constraint;

CREATE TYPE financial_protocol_type AS ENUM(
    'swap',
    'psm',
    'debt',
    'leverage'
);

ALTER TABLE your_table
ALTER COLUMN your_column TYPE financial_protocol_type
USING your_column::text::financial_protocol_type;

DROP TYPE financial_type;


CREATE TYPE protocol_implementation_type AS ENUM(
    'custom',
    'vm'
);

ALTER TABLE protocol_type
ALTER COLUMN implementation TYPE protocol_implementation_type
USING implementation_type::text::protocol_implementation_type;

DROP TYPE implementation_type;


