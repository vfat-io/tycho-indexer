CREATE TYPE protocol_implementation_type AS ENUM(
    'custom',
    'vm'
);

ALTER TABLE protocol_type
ALTER COLUMN "implementation" TYPE protocol_implementation_type
USING protocol_type::text::protocol_implementation_type;

DROP TYPE implementation_type;

CREATE TYPE financial_protocol_type AS ENUM(
    'swap',
    'psm',
    'debt',
    'leverage'
);

ALTER TABLE protocol_type
ALTER COLUMN financial_type TYPE financial_protocol_type
USING protocol_type::text::financial_protocol_type;

DROP TYPE financial_type;

ALTER TABLE protocol_type
DROP CONSTRAINT unique_name_constraint;

-- dropping tables will drop any triggers or indices along with it
ALTER TABLE protocol_system
ALTER COLUMN "name" TYPE varchar(255);

DROP TYPE protocol_system_type;

CREATE OR REPLACE FUNCTION invalidate_previous_entry_protocol_state()
    RETURNS TRIGGER
    AS $$
BEGIN
    -- Update the 'valid_to' field of the last valid entry when a new one is inserted.
    UPDATE
        protocol_state
    SET
        valid_to = NEW.valid_from
    WHERE
        valid_to IS NULL
        AND protocol_component_id = NEW.protocol_component_id;
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

ALTER TABLE protocol_state 
DROP CONSTRAINT check_attribute_fields;

ALTER TABLE protocol_state 
DROP COLUMN attribute_name,
DROP COLUMN attribute_value,
ADD COLUMN state jsonb NULL,
ADD COLUMN tvl bigint NULL,
ADD COLUMN inertias bigint[] NULL;

CREATE TRIGGER invalidate_previous_protocol_state
    BEFORE INSERT ON protocol_state
    FOR EACH ROW
    EXECUTE PROCEDURE invalidate_previous_entry_protocol_state();
