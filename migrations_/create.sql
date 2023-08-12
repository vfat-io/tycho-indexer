create table Block (
	"id" bigint primary key,
	"hash" char(32) UNIQUE,
	"number" bigint,
	"ts" timestamptz
);

create table Protocol (
    "id" bigint PRIMARY KEY,
    "external_id" VARCHAR(255),
    "attributes" JSONB,
    "valid_from" INTEGER REFERENCES Block(id),
    "valid_to" INTEGER REFERENCES Block(id),
);

CREATE TYPE financial_protocol_type AS ENUM ('swap', 'debt', 'leverage');

CREATE TABLE ProtocolType (
    "id" BIGINT PRIMARY KEY,
    "name" VARCHAR(255),
    "type" financial_protocol_type,
    "attribute_schema" JSONB,
    "protocol_type_id" BIGINT REFERENCES ProtocolType(id),
    "protocol_system_id" BIGINT REFERENCES ProtocolSystem(id)
);

create table ProtocolSystem (
	"id" bigint primary key,
	"name" VARCHAR(255),
	"valid_from" bigint references Block(id),
);

create table ProtocolState (
	"id" bigint primary key,
	"tvl": bigint,
	"inertias": bigint[],
	"state" JSONB,
	"valid_from" INTEGER REFERENCES Block(id),
    "valid_to" INTEGER REFERENCES Block(id),
    "protocol_id" BIGINT REFERENCES Protocol(id),
);

create table Token (
	"id" bigint primary key,
	"symbol" VARCHAR(255),
	"decimals" int,
	"tax" bigint,
	"gas" bigint[]
);

CREATE TABLE ProtocolToken (
    "protocol_id" BIGINT REFERENCES Protocol(id),
    "token_id" BIGINT REFERENCES Token(id),
    PRIMARY KEY("protocol_id", "token_id")
);

create table Contract (
    "id" bigint primary key,
	"address" char(20) unique,
	"valid_from" INTEGER REFERENCES Block(id),
    "valid_to" INTEGER REFERENCES Block(id)
);

create table ContractBalance(
	"id" bigint primary key,
	"balance" char(32),
	"valid_from" INTEGER REFERENCES Block(id),
    "valid_to" INTEGER REFERENCES Block(id)
);

create table ContractCode(
	"id" bigint primary key,
	"code" bytea,
	"hash" char(32),
	"valid_from" INTEGER REFERENCES Block(id),
    "valid_to" INTEGER REFERENCES Block(id)
);

create table ContractStorageSlot (
	"key" char(32),
	"value" char(32),
	"valid_from" INTEGER REFERENCES Block(id),
    "valid_to" INTEGER REFERENCES Block(id)
);


