--- Drop special composite indices for in-app versioning queries

DROP INDEX IF EXISTS idx_contract_storage_account_id_slot_valid_to;
DROP INDEX IF EXISTS idx_protocol_state_component_id_attribute_name_valid_to;
DROP INDEX IF EXISTS idx_component_balance_component_id_token_id_valid_to;
