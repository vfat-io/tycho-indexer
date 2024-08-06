--- Removes a protocol system and all related entries from the database. 
--- This includes all protocol components, protocol states, accounts and their histories.
--- Note - linked blocks and transactions will not be removed.
--- TO USE: run the following cli command:
--- `psql -d tycho_indexer_{env} -c "\set protocol_system_name '{protol_system_to_delete}'" -f remove_protocol_system.sql`
--- where tycho_indexer_{env} is the name of your database and protol_system_to_delete is the protocol system to be removed.

BEGIN;

-- Input variable of protocol system to be deleted
\echo 'Will delete all db entries related to' :protocol_system_name

--- Find and remove all linked accounts (accounts are not cascade deleted). Note, this will cascade delete the linked contract
--- code entries too
DELETE FROM account
WHERE id IN (
    SELECT cc.account_id
    FROM contract_code cc
    JOIN protocol_component_holds_contract pchc ON pchc.contract_code_id = cc.id
    JOIN protocol_component pc ON pchc.protocol_component_id = pc.id
    JOIN protocol_system ps ON pc.protocol_system_id = ps.id
    WHERE ps.name = :'protocol_system_name'
);

-- Cascade delete protocol system and all related entries
DELETE FROM protocol_system
WHERE name = :'protocol_system_name';

COMMIT;