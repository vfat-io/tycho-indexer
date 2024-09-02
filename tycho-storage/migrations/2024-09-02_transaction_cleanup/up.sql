CREATE OR REPLACE FUNCTION clean_transaction_table() RETURNS void AS $$
BEGIN
    DELETE FROM "transaction" t 
    WHERE NOT EXISTS (
        SELECT 1 FROM protocol_state ps WHERE ps.modify_tx = t.id 
    )
    AND NOT EXISTS (
        SELECT 1 FROM protocol_component pc WHERE pc.creation_tx = t.id OR pc.deletion_tx = t.id
    )
    AND NOT EXISTS (
        SELECT 1 FROM account a WHERE a.creation_tx = t.id OR a.deletion_tx = t.id
    )
    AND NOT EXISTS (
        SELECT 1 FROM contract_storage cs WHERE cs.modify_tx = t.id 
    )
    AND NOT EXISTS (
        SELECT 1 FROM component_balance cb WHERE cb.modify_tx = t.id 
    )
    AND NOT EXISTS (
        SELECT 1 FROM contract_code cc WHERE cc.modify_tx = t.id 
    )
    AND NOT EXISTS (
        SELECT 1 FROM account_balance ab WHERE ab.modify_tx = t.id 
    );

END;
$$ LANGUAGE plpgsql;

-- Schedule the cleanup function to run every day at midnight
SELECT cron.schedule('clean_transaction_table', '0 0 * * *', 'SELECT clean_transaction_table();');
