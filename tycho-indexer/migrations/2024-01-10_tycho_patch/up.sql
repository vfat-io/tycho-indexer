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
    ADD COLUMN attribute_name VARCHAR NOT NULL,
    ADD COLUMN attribute_value BYTEA NOT NULL,
    ADD COLUMN previous_value BYTEA NULL;

ALTER TABLE protocol_system
    ADD CONSTRAINT name_unique UNIQUE (name);

-- Make the "account_id" column of the token unique
ALTER TABLE token
    ADD CONSTRAINT unique_account_id_constraint UNIQUE (account_id);

DROP TRIGGER invalidate_previous_protocol_state ON protocol_state;

DROP FUNCTION invalidate_previous_entry_protocol_state();

DROP TRIGGER invalidate_previous_contract_storage ON contract_storage;

DROP FUNCTION invalidate_previous_entry_contract_storage();

DROP TRIGGER invalidate_previous_account_balance ON account_balance;

DROP FUNCTION invalidate_previous_entry_account_balance();

DROP TRIGGER invalidate_previous_contract_code ON contract_code;

DROP FUNCTION invalidate_previous_entry_contract_code();

CREATE TABLE IF NOT EXISTS protocol_component_holds_contract(
    "protocol_component_id" bigint REFERENCES protocol_component(id) ON DELETE CASCADE NOT NULL,
    -- we don't allow a contract to be deleted unless the protocol component was removed
    "contract_code_id" bigint REFERENCES "contract_code"(id) NOT NULL,
    -- Timestamp this entry was inserted into this table.
    "inserted_ts" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Timestamp this entry was inserted into this table.
    "modified_ts" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("protocol_component_id", "contract_code_id")
);

CREATE INDEX IF NOT EXISTS idx_protocol_component_holds_contract_protocol_component_id ON protocol_component_holds_contract(protocol_component_id);

CREATE INDEX IF NOT EXISTS idx_protocol_component_holds_contract_contract_code_id ON protocol_component_holds_contract(contract_code_id);

--  Saves the component balance of a protocol component.
CREATE TABLE IF NOT EXISTS component_balance(
    "id" bigserial PRIMARY KEY,
    -- id of the token whose tvl changed
    "token_id" bigint REFERENCES "token"(id) NOT NULL,
    -- new balance of the token for this component
    "new_balance" bytea NOT NULL,
    -- new balance of the token for this component
    "previous_value" bytea NOT NULL,
    -- new balance as a floating point number
    "balance_float" float NOT NULL,
    -- the transaction that modified the tvl of this component
    "modify_tx" bigint REFERENCES "transaction"(id) ON DELETE CASCADE NOT NULL,
    -- Reference to static attributes of the protocol.
    "protocol_component_id" bigint REFERENCES protocol_component(id) NOT NULL,
    -- Timestamp this entry was inserted into this table.
    "inserted_ts" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- The ts at which this state became valid at.
    "valid_from" timestamptz NOT NULL,
    -- The ts at which this state stopped being valid at. Null if this
    --	state is the currently valid entry.
    "valid_to" timestamptz
);

-- Rename the protocol_holds_token table to protocol_component_holds_token
DROP TRIGGER update_modtime_protocol_holds_token ON protocol_holds_token;

DROP TRIGGER audit_table_protocol_holds_token ON protocol_holds_token;

ALTER TABLE protocol_holds_token RENAME TO protocol_component_holds_token;

CREATE TRIGGER update_modtime_protocol_component_holds_token
    BEFORE UPDATE ON protocol_component_holds_token
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_column();

CREATE TRIGGER audit_table_protocol_component_holds_token
    BEFORE UPDATE ON protocol_component_holds_token
    FOR EACH ROW
    EXECUTE PROCEDURE audit_trigger();

