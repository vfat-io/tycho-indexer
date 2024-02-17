--- Special composite indices for in-app versioning queries
CREATE INDEX IF NOT EXISTS idx_component_balance_component_id_token_id_valid_to ON component_balance(protocol_component_id, token_id, valid_to);

CREATE INDEX IF NOT EXISTS idx_protocol_state_component_id_attribute_name_valid_to ON protocol_state(protocol_component_id, attribute_name, valid_to);

CREATE INDEX IF NOT EXISTS idx_contract_storage_account_id_slot_valid_to ON contract_storage(account_id, slot, valid_to);

-- index to speed up token lookups
CREATE INDEX IF NOT EXISTS idx_account_address ON account(address);
