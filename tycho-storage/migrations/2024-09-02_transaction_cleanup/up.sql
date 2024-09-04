CREATE OR REPLACE FUNCTION clean_transaction_table() RETURNS void AS $$
DECLARE
    batch_size INT := 500;
    rows_deleted INT;
BEGIN
    -- Create a temporary table to hold the transaction IDs for transactions to delete
    CREATE TEMP TABLE temp_transaction_ids (
        id BIGINT
    );

    -- Insert the transaction IDs to delete into the temp table
    INSERT INTO temp_transaction_ids (id)
    SELECT t2.id
    FROM "transaction" t2
    LEFT JOIN contract_code cc ON cc.modify_tx = t2.id
    LEFT JOIN protocol_component pc ON pc.creation_tx = t2.id OR pc.deletion_tx = t2.id
    LEFT JOIN account a ON a.creation_tx = t2.id OR a.deletion_tx = t2.id
    LEFT JOIN account_balance ab ON ab.modify_tx = t2.id
    LEFT JOIN component_balance cb ON cb.modify_tx = t2.id
    LEFT JOIN protocol_state ps ON ps.modify_tx = t2.id
    LEFT JOIN contract_storage cs ON cs.modify_tx = t2.id
    WHERE cc.modify_tx IS NULL
      AND pc.creation_tx IS NULL
      AND pc.deletion_tx IS NULL
      AND a.creation_tx IS NULL
      AND a.deletion_tx IS NULL
      AND ab.modify_tx IS NULL
      AND cb.modify_tx IS NULL
      AND ps.modify_tx IS NULL
      AND cs.modify_tx IS NULL;

    -- Delete in batches
    LOOP
        DELETE FROM "transaction"
        WHERE id IN (
            SELECT id
            FROM temp_transaction_ids
            LIMIT batch_size
        );

        GET DIAGNOSTICS rows_deleted = ROW_COUNT;

        -- Commit after each batch to release locks and reduce blocking
        COMMIT;

        -- Exit the loop if no more transactions are left to delete
        IF rows_deleted = 0 THEN
            EXIT;
        END IF;

        -- Remove the deleted rows from the temporary table
        DELETE FROM temp_transaction_ids
        WHERE id IN (
            SELECT id
            FROM temp_transaction_ids
            LIMIT batch_size
        );

        -- Commit the cleanup of the temporary table
        COMMIT;
    END LOOP;

    -- Drop the temporary table when done
    DROP TABLE temp_transaction_ids;

END;
$$ LANGUAGE plpgsql;

-- Schedule the cleanup function to run every Sunday at midnight
SELECT cron.schedule('clean_transaction_table', '0 0 * * 0', 'SELECT clean_transaction_table();');
