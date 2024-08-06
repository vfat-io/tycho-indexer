#!/bin/bash

#  WARNING!!! Do not use this script for protocols that share accounts/contracts with other systems. The
#  shared accounts/contracts will be removed from the database.

#  Removes a protocol system and all related entries from the database.
#  This includes all protocol components, protocol states, accounts and their histories.
#  Note - linked blocks and transactions will not be removed.
#  TO USE: run the following cli command: './remove_protocol_system.sh <database_name> <protocol_system_to_delete>'

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <database_name> <protocol_system_to_delete>"
    exit 1
fi

# Set the database and protocol system to delete
db_name=$1
protocol_system_to_delete=$2

# Warning message
echo 'WARNING: This script will delete all db entries related to the specified protocol system. This includes any shared accounts/contracts.'
echo 'If this protocol shares accounts/contracts with other systems, do not proceed.'
read -p 'Are you sure you want to proceed? (y/n) ' -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Exiting..."
    exit 1
fi

# Execute the SQL commands using psql
psql -d "$db_name" <<EOF
\set protocol_system_name '$protocol_system_to_delete'

\echo 'Deleting all db entries related to :protocol_system_name'

BEGIN;

--- Find and remove all linked accounts (accounts are not cascade deleted). Note, this will cascade delete the linked contract
--- code entries too.
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
EOF

