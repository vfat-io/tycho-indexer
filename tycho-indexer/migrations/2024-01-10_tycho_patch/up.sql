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

ALTER TABLE protocol_state
DROP COLUMN state,
DROP COLUMN tvl,
DROP COLUMN inertias,
ADD COLUMN attribute_name VARCHAR NULL,
ADD COLUMN attribute_value BYTEA NULL;

-- Make sure either both attribute_name and attribute_value are given or neither are given
ALTER TABLE protocol_state
ADD CONSTRAINT check_attribute_fields
CHECK (
    (attribute_name IS NULL AND attribute_value IS NULL) OR
    (attribute_name IS NOT NULL AND attribute_value IS NOT NULL)
);

DROP TRIGGER invalidate_previous_protocol_state ON protocol_state;
DROP FUNCTION invalidate_previous_entry_protocol_state();


ALTER TABLE protocol_system
ADD CONSTRAINT name_unique UNIQUE (name);


-- Make the "account_id" column of the token unique
ALTER TABLE token
    ADD CONSTRAINT unique_account_id_constraint UNIQUE (account_id);