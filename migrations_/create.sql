-- Enumeration of supported blockchains
create table "chain" (
	"id" bigint primary key,
	
	-- The name of the blockchain
	"name" varchar(255) unique not null
);

-- This table stores block at which relevant changes occured.
create table block (
	"id" bigint primary key,
	
	-- The unique hash of this block.
	"hash" char(32) unique not null,
	
	-- The ancestor hash of this block. Used to trace forked blocks.
	"parent_hash" char(32) unique not null,
	
	-- Whether this block is part of the canonical chain.
	"main" bool not null default true,
	
	-- The block number, table might contain forks so the number is not 
	--	necessarily unique - not even withing a single chains scope.
	"number" bigint not null,
	
	-- Timestamp this block was validated/mined/created.
	"ts" timestamptz not null,
	
	-- Timestamp this block was inserted
	"inserted_ts" timestamptz not null default current_timestamp,
	
	-- The chain this block belongs to.
	"chain_id" bigint references "chain"(id) not null,
	
	unique(chain_id, "hash"),
);

create index idx_block_number_identity on block("number", "chain_id");
create index idx_block_hash_identity on block("hash", "chain_id");


create table transaction (
	"id" bigint primary key,
	
	-- The unique hash of this transaction.
	"hash" char(32) unique not null,
	
	-- sender of the transaction.
	"from" bytea not null,
	
	-- receiver of the transaction,
	"to" bytea not null,
	
	-- sequential index of the transaction in the block.
	"index" bigint not null,
		
	-- transactions are block scoped and thus also chain scoped.
	"block_id" bigint references block(id) not null,
	
	unique("index", block_id),
	unique("hash", block_id)
);

create index idx_transaction_block_id on transaction(block_id);


-- ProtocolSystem table group functional components (protocols) that 
--	belong to the same logical system.
create table protocol_system (
	"id" bigint primary key,
	
	-- The name of the procotol system, e.g. uniswap-v2, ambient, etc.
	"name" VARCHAR(255) not null,
	
	-- The block at which this system was created, e.g. factory deployed.
	"created_at" bigint references block(id) not null
);

CREATE TYPE financial_protocol_type AS ENUM ('swap', 'psm', 'debt', 'leverage');

-- Table describing the different protocol types available in the system.
CREATE TABLE protocol_type (
    "id" BIGINT PRIMARY KEY,
    
    -- The name of the type e.g. uniswap-v2:pool
    "name" VARCHAR(255) not null,
    
    -- The actual type of the protocol.
    "type" financial_protocol_type not null,
    
    -- The jsonschema to evaluate the attribute json for pools of this type.
    "attribute_schema" JSONB
);

-- Describes the static attributes of a protocol (component). A protocol is usually
--	a single component of a protocol system. E.g. uniswap-v2 is the system
--	that creates and operates swap components (aka pools).
create table protocol (
    "id" bigint PRIMARY KEY,
    
    -- Protocols are scoped to a specific chain.
    "chain_id" bigint references "chain"(id) not null,
    
    -- External id to identify protocol within a chain and system scope. 
    --  We can't safely assume a protocol maps 1:1 to a contract address nor 
    --	vice versa.
    "external_id" VARCHAR(255) not null,
    
    -- All static attributes of the protocol, e.g. precision.
    "attributes" JSONB,
    
    -- The block at which this protocol was created. Somewhat redundant as
    --	it could be inferred from related contracts but it might not be clear
    --	in case the protocol relates or more than one contract.
    "created_at" INTEGER REFERENCES block(id) not null,
    
    -- The block at which this protocol ceased to exist.
    "deleted_at" INTEGER REFERENCES block(id),
    
    -- The financial type of this protocol.
    "protocol_type_id" BIGINT REFERENCES protocol_type(id) not null,
    
    -- The system that this protocol belongs to e.g. uniswap-v2.
    "protocol_system_id" BIGINT REFERENCES protocol_system(id) not null,
    
    unique("chain_id", "protocol_system_id", "external_id")
);

create index idx_protocol_identity on protocol(external_id, protocol_system_id, chain_id);

-- Describes the mutable state of a component. Versioned by blocks.
create table protocol_state (
	"id" bigint primary key,
	
	-- The total value locked within the protocol. Might not always apply.
	"tvl" bigint,
	
	-- the inertias per token of the protocol (in increasing order sorted 
	--	by token contract address). Might not always apply. 
	"inertias" bigint[],
	
	-- The actual state of the protocols attributes. This is only relevant 
	--	for fully implemented protocols. For protocols using vm simulation 
	--	use the contract tables instead. 
	"state" JSONB,
	
	-- The tx at which this state became valid at.
	"valid_from" bigint REFERENCES "transaction"(id) not null,
	
	-- The tx at which this state stopped being valid at. Null if this 
	--	state is the currently valid entry.
    "valid_to" bigint REFERENCES "transaction"(id) default null,
    
    -- Reference to static attributes of the protocol.
    "protocol_id" BIGINT REFERENCES Protocol(id) not null,
    
    unique(protocol_id, valid_from),
    unique(protocol_id, valid_to)
);

create index idx_protocol_state_valid_to on protocol_state(valid_to);
create index idx_protocol_state_valid_protocol_id on protocol_state(protocol_id);


-- Describes a single contract.
create table contract (
    "id" bigint primary key,
    
    -- Contracts are scoped to a single chain.
    "chain_id" bigint references "chain"(id) not null,
    
    -- Succinct title of this contract e.g. "maker psm"
    "title" varchar(255) not null,
    
    -- The address of this contract.
	"address" bytea not null,
	
	-- The tx this contract was created.
	"created_at" bigint REFERENCES "transaction"(id) not null,
	
	-- The tx this contract was destroyed. Null in case it is active.
    "deleted_at" bigint REFERENCES "transaction"(id) default null,
    
    -- The address is required to be unique per chain.
    UNIQUE ("chain_id", "address")
);

create index idx_contract_chain_id on contract(chain_id)

-- Describes tokens e.g. ERC20 on evm chains.
create table "token" (
	"id" bigint primary key,
	
	-- The contract that implements this token.
	"contract_id" bigint references contract(id) not null,
	
	-- The symbol/ticker for this token.
	"symbol" VARCHAR(255) not null,
	
	-- Decimal precision the token stores balances with.
	"decimals" int not null,
	
	-- The tax this token charges on transfer.
	"tax" bigint,
	
	-- The estimated amount of gas used per transfer.
	"gas" bigint[] 
);

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX idx_token_symbol ON "token" USING gin("symbol" gin_trgm_ops);
CREATE INDEX idx_token_contract_id ON token(contract_id);



-- M2M relationship between tokens and protocol 
CREATE TABLE protocol_holds_token (
    "protocol_id" BIGINT REFERENCES protocol(id) not null,
    "token_id" BIGINT REFERENCES "token"(id) not null,
    PRIMARY KEY("protocol_id", "token_id")
);

-- Versioned contract balance.
create table contract_balance(
	"id" bigint primary key,
	
	-- The balance of the contract.
	"balance" char(32),
	
	-- the contract this entry refers to.
	"contract_id" bigint references contract(id) not null,
	
		-- The tx at which this state became valid at.
	"valid_from" bigint REFERENCES "transaction"(id) not null,
	
	-- The tx at which this state stopped being valid at. Null if this 
	--	state is the currently valid entry.
    "valid_to" bigint REFERENCES "transaction"(id) default null,
    unique(contract_id, valid_from),
    unique(contract_id, valid_to)
);

CREATE INDEX contract_balance_contract_id_idx ON contract_balance (contract_id);
CREATE INDEX contract_balance_valid_to_idx ON contract_balance (valid_to);

-- Versioned contract code.
create table contract_code(
	"id" bigint primary key,
	
	-- The code of this contract optimised for the system using it, e.g. revm.
	"code" bytea not null,
	
	-- The hash of the code, allows to easily detect changes.
	"hash" bytea not null,
	
	-- the contract this entry refers to.
	"contract_id" bigint references contracct(id) not null,
	
	-- The tx at which this state became valid at.
	"valid_from" bigint REFERENCES "transaction"(id) not null,
	
	-- The tx at which this state stopped being valid at. Null if this 
	--	state is the currently valid entry.
    "valid_to" bigint REFERENCES "transaction"(id) default null,
    unique(contract_id, valid_from),
    unique(contract_id, valid_to)
);

CREATE INDEX contract_code_contract_id_idx ON contract_code (contract_id);
CREATE INDEX contract_code_valid_to_idx ON contract_code (valid_to);

-- Versioned contract storage.
create table contract_storage (
	"id" bigint primary key,
	
	-- the preimage/slot for this entry. 
	"slot" char(32) not null,
	
	-- the value of the storage slot.
	"value" char(32) not null,
	
	-- the contract this entry refers to.
	"contract_id" bigint references contracct(id) not null,
	
	-- The tx at which this state became valid at.
	"valid_from" bigint REFERENCES "transaction"(id) not null,
	
	-- The tx at which this state stopped being valid at. Null if this 
	--	state is the currently valid entry.
    "valid_to" bigint REFERENCES "transaction"(id) default null,
    
    unique(contract_id, valid_from),
    unique(contract_id, valid_to)
);

CREATE INDEX contract_storage_contract_id_idx ON contract_storage (contract_id);
CREATE INDEX contract_storage_valid_to_idx ON contract_storage (valid_to);

-- Relationship between protocols and contract(s).
CREATE TABLE protocol_calls_contract (
	"id" BIGSERIAL PRIMARY KEY,

    "protocol_id" BIGINT REFERENCES protocol(id) not null,
    
    "contract_id" BIGINT REFERENCES contract(id) not null,
    
    -- Tx this assocuation became valud, versioned association between contracts, 
    -- allows to track updates of e.g. price feeds. 
	"valid_from" bigint REFERENCES "transaction"(id) not null,
	
	-- The tx at which this association stopped being valid. Null if this 
	--	state is the currently valid entry.
    "valid_to" bigint REFERENCES "transaction"(id) default null,
    
    UNIQUE("protocol_id", "contract_id", "valid_from"),
    UNIQUE("protocol_id", "contract_id", "valid_to")
);

CREATE INDEX protocol_calls_contract_protocol_id_idx ON protocol_calls_contract(protocol_id);
CREATE INDEX protocol_calls_contract_contract_id_idx ON protocol_calls_contract(contract_id);
CREATE INDEX protocol_calls_contract_valid_to_idx ON protocol_calls_contract(valid_to);

/*
 * TRIGGERS
 * 
 * Below follows trigger logic for all versioned tables. The trigger will automatically 
 * identify the previous version of an entry, set the valid_to field to the 
 * valid_from of the new version.
 */

CREATE OR REPLACE FUNCTION invalidate_previous_entry(tbl_name text, identity_col text) RETURNS TRIGGER AS $$
BEGIN
    EXECUTE format('
	    UPDATE %I 
	    SET valid_to = NEW.valid_from 
	    WHERE id = (
	        SELECT id FROM %I 
	        WHERE valid_to IS NULL AND %I = NEW.%I 
	    );', tbl_name, tbl_name, identity_col, identity_col);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER invalidate_previous_protocol_state
BEFORE INSERT ON protocol_state 
FOR EACH ROW EXECUTE PROCEDURE invalidate_previous_entry('protocol_state', 'protocol_id');

CREATE TRIGGER invalidate_previous_contract_balance
BEFORE INSERT ON contract_balance 
FOR EACH ROW EXECUTE PROCEDURE invalidate_previous_entry('contract_balance', 'contract_id');

CREATE TRIGGER invalidate_previous_contract_code
BEFORE INSERT ON contract_code
FOR EACH ROW EXECUTE PROCEDURE invalidate_previous_entry('contract_code', 'contract_id');

CREATE TRIGGER invalidate_previous_contract_storage
BEFORE INSERT ON contract_storage
FOR EACH ROW EXECUTE PROCEDURE invalidate_previous_entry('contract_storage', 'contract_id');


CREATE OR REPLACE FUNCTION invalidate_previous_entry_protocol_calls_contract() RETURNS TRIGGER AS $$
BEGIN
-- Update the 'valid_to' field of the last valid entry when a new one is inserted.
    UPDATE protocol_calls_contract 
    SET valid_to = NEW.valid_from 
    WHERE valid_to IS NULL AND protocol_id = NEW.protocol_id and contract_id = NEW.contract_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Bind the function to 'protocol_state' table, to be called on each row insert event.
CREATE TRIGGER invalidate_previous_entry_protocol_calls_contract
BEFORE INSERT ON protocol_calls_contract 
FOR EACH ROW EXECUTE PROCEDURE invalidate_previous_entry_protocol_calls_contract();