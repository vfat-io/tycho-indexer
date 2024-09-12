CREATE OR REPLACE FUNCTION clean_transaction_table() RETURNS void AS $$
DECLARE
    batch_size INT := 1000;
    rows_deleted INT;
    offset_val INT := 0;
    orphaned_count INT;  -- Variable to store count of orphaned transactions
BEGIN
    -- Create the outer temporary table to hold all transaction IDs that are safe to delete
    CREATE TEMP TABLE temp_transaction_ids (
        id BIGINT PRIMARY KEY
    );

    -- Create temp_batch_transaction_ids once and truncate it after each batch instead of dropping and creating.
    CREATE TEMP TABLE temp_batch_transaction_ids (
        id BIGINT PRIMARY KEY
    );

    LOOP
        TRUNCATE temp_batch_transaction_ids;

        RAISE NOTICE 'Processing batch with OFFSET %', offset_val;

        INSERT INTO temp_batch_transaction_ids (id)
        SELECT t2.id
        FROM "transaction" t2
        ORDER BY t2.id
        LIMIT batch_size OFFSET offset_val;

        -- Check if there are no more transactions to process
        IF NOT FOUND THEN
            -- Calculate the number of orphaned transactions
            SELECT COUNT(*) INTO orphaned_count FROM temp_transaction_ids;
            RAISE NOTICE 'SEARCH COMPLETE. Found % total orphaned transactions', orphaned_count;
            EXIT;
        END IF;

        offset_val := offset_val + batch_size;

        DELETE FROM temp_batch_transaction_ids
        WHERE id IN (
            SELECT t2.id
            FROM temp_batch_transaction_ids t2
            JOIN contract_code cc ON cc.modify_tx = t2.id
        );

        DELETE FROM temp_batch_transaction_ids
        WHERE id IN (
            SELECT t2.id
            FROM temp_batch_transaction_ids t2
            JOIN protocol_component pc ON pc.creation_tx = t2.id OR pc.deletion_tx = t2.id
        );

        DELETE FROM temp_batch_transaction_ids
        WHERE id IN (
            SELECT t2.id
            FROM temp_batch_transaction_ids t2
            JOIN account a ON a.creation_tx = t2.id OR a.deletion_tx = t2.id
        );

        DELETE FROM temp_batch_transaction_ids
        WHERE id IN (
            SELECT t2.id
            FROM temp_batch_transaction_ids t2
            JOIN account_balance ab ON ab.modify_tx = t2.id
        );

        DELETE FROM temp_batch_transaction_ids
        WHERE id IN (
            SELECT t2.id
            FROM temp_batch_transaction_ids t2
            JOIN component_balance cb ON cb.modify_tx = t2.id
        );

        DELETE FROM temp_batch_transaction_ids
        WHERE id IN (
            SELECT t2.id
            FROM temp_batch_transaction_ids t2
            JOIN protocol_state ps ON ps.modify_tx = t2.id
        );

        DELETE FROM temp_batch_transaction_ids
        WHERE id IN (
            SELECT t2.id
            FROM temp_batch_transaction_ids t2
            JOIN contract_storage cs ON cs.modify_tx = t2.id
        );

        SELECT COUNT(*) INTO orphaned_count FROM temp_batch_transaction_ids;
        RAISE NOTICE 'Found % orphaned transactions', orphaned_count;

        INSERT INTO temp_transaction_ids (id)
        SELECT id
        FROM temp_batch_transaction_ids;

        -- Pause between batches to reduce load
        PERFORM pg_sleep(1);

    END LOOP;

    -- Phase 2: Delete the collected transaction IDs in batches
    LOOP
        RAISE NOTICE 'Deleting next transaction batch. [batch_size = %]', batch_size;
        RAISE NOTICE 'Remaining transactions: %', orphaned_count;

        -- Delete the rows in batches from the transaction table
        DELETE FROM "transaction"
        WHERE id IN (
            SELECT id FROM temp_transaction_ids
            LIMIT batch_size
        );

        DELETE FROM temp_transaction_ids
        WHERE id IN (
            SELECT id FROM temp_transaction_ids
            LIMIT batch_size
        );

        GET DIAGNOSTICS rows_deleted = ROW_COUNT;

        orphaned_count := orphaned_count - rows_deleted;

        -- Exit the loop if no more transactions are left to delete
        IF rows_deleted < batch_size THEN
            RAISE NOTICE 'Transaction table cleanup complete.';
            EXIT;
        END IF;

        -- Pause between delete batches to reduce load
        PERFORM pg_sleep(1);
    END LOOP;

    -- Drop the temporary tables when done
    DROP TABLE IF EXISTS temp_transaction_ids;
    DROP TABLE IF EXISTS temp_batch_transaction_ids;

END;
$$ LANGUAGE plpgsql;

-- Schedule the cleanup function to run every Sunday at midnight
SELECT cron.schedule('clean_transaction_table', '0 0 * * 0', 'SELECT clean_transaction_table();');
