INSERT INTO "chain"("name") 
VALUES 
    ('ethereum'), 
    ('starknet'), 
    ('zksync');


INSERT INTO block ("hash", "parent_hash", "number", "ts", "chain_id") 
VALUES 
    -- ethereum blocks
    (E'\\x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6', E'\\xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3', 1, TIMESTAMP '2022-11-01 08:00:00', 1),
    (E'\\xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9', E'\\x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6', 2, TIMESTAMP '2022-11-01 09:00:00', 1),
    (E'\\x3d6122660cc824376f11ee842f83addc3525e2dd6756b9bcf0affa6aa88cf741', E'\\xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9', 3, TIMESTAMP '2022-11-01 10:00:00', 1), 
    (E'\\x23adf5a3be0f5235b36941bcb29b62504278ec5b9cdfa277b992ba4a7a3cd3a2', E'\\x3d6122660cc824376f11ee842f83addc3525e2dd6756b9bcf0affa6aa88cf741', 4, TIMESTAMP '2022-11-01 11:00:00', 1), 

    -- starknet blocks
    ('0x345pqr', '0x000002', 1, TIMESTAMP '2022-11-01 12:00:00', 2),
    ('0x678stu', '0x345pqr', 2, TIMESTAMP '2022-11-01 13:00:00', 2),
    ('0x901vwx', '0x678stu', 3, TIMESTAMP '2022-11-01 14:00:00', 2), 
    ('0x234yz_', '0x901vwx', 4, TIMESTAMP '2022-11-01 15:00:00', 2),

    -- zksync blocks
    ('0x567zzz', '0x000003', 1, TIMESTAMP '2022-11-01 16:00:00', 3),
    ('0x890aaa', '0x567zzz', 2, TIMESTAMP '2022-11-01 17:00:00', 3),
    ('0x123bbb', '0x890aaa', 3, TIMESTAMP '2022-11-01 18:00:00', 3), 
    ('0x456ccc', '0x123bbb', 4, TIMESTAMP '2022-11-01 19:00:00', 3);


INSERT INTO "transaction" ("hash", "from", "to", "index", "block_id")
VALUES
    -- ethereum
    ('0x001', '\x31', '\x32', 1, 1), 
    ('0x002', '\x33', '\x34', 2, 1), 
    ('0x003', '\x35', '\x36', 1, 2),
    ('0x004', '\x37', '\x38', 1, 3),
    ('0x005', '\x39', '\x40', 2, 3), 
    ('0x006', '\x41', '\x42', 1, 4),

    -- starknet
    ('0x007', '\x43', '\x44', 1, 5),
    ('0x008', '\x45', '\x46', 2, 5), 
    ('0x009', '\x47', '\x48', 1, 6),
    ('0x0010', '\x49', '\x50', 1, 7),
    ('0x0011', '\x51', '\x52', 1, 8),

    -- zksync
    ('0x0012', '\x53', '\x54', 1, 9),
    ('0x0013', '\x55', '\x56', 1, 10),
    ('0x0014', '\x57', '\x58', 1, 11),
    ('0x0015', '\x59', '\x60', 1, 12),
    ('0x0016', '\x59', '\x60', 2, 12);


INSERT INTO protocol_system ("name")
VALUES
    ('uniswap-v2'),
    ('ambient'),   
    ('sithswap'),  
    ('jediswap'),  
    ('syncswap'), 
    ('mute');


INSERT INTO protocol_type ("name", "type", "implementation", "attribute_schema")
VALUES 
    ('uniswap-v2:swap', 'swap', 'custom', '{"properties": {"fee": {"type": "integer"}}}'),  
    ('ambient:swap', 'swap', 'vm', '{"properties": {"fee": {"type": "integer"}}}'),
    ('sithswap:swap', 'swap', 'custom', '{"properties": {"fee": {"type": "integer"}}}'),
    ('jediswap:swap', 'swap', 'custom', '{"properties": {"fee": {"type": "integer"}}}'),  
    ('syncswap:swap', 'swap', 'custom', '{"properties": {"fee": {"type": "integer"}}}'),  
    ('mute:swap', 'swap', 'custom', '{"properties": {"fee": {"type": "integer"}}}');


INSERT INTO protocol_component(chain_id, external_id, attributes, created_at, protocol_type_id, protocol_system_id)
VALUES 
    (1, 'uniswap_1', '{"fee": 3000}', '2021-01-10 14:00:00', 1, 1),
    (1, 'uniswap_2', '{"fee": 3000}', '2021-01-10 14:00:00', 1, 1),
    (1, 'ambient_1', '{"fee": 3000}', '2021-01-10 14:00:00', 2, 2),
    (1, 'ambient_2', '{"fee": 3000}', '2021-01-10 14:00:00', 2, 2),

    (2, 'sithswap_1', '{"fee": 3000}', '2021-01-10 14:00:00', 3, 3),
    (2, 'sithswap_2', '{"fee": 3000}', '2021-01-10 14:00:00', 3, 3),
    (2, 'jediswap_1', '{"fee": 3000}', '2021-01-10 14:00:00', 4, 4),
    (2, 'jediswap_2', '{"fee": 3000}', '2021-01-10 14:00:00', 4, 4),

    (3, 'syncswap_1', '{"fee": 3000}', '2021-01-10 14:00:00', 5, 5),
    (3, 'syncswap_2', '{"fee": 3000}', '2021-01-10 14:00:00', 5, 5),
    (3, 'mute_1', '{"fee": 3000}', '2021-01-10 14:00:00', 6, 6),
    (3, 'mute_2', '{"fee": 3000}', '2021-01-10 14:00:00', 6, 6);


INSERT INTO protocol_state("tvl", "inertias", "state", "modify_tx", "valid_from", "protocol_component_id")
VALUES 
    (1000, ARRAY[25, 50], '{"reserve0": 500, "reserve1": 1000}', 1, '2021-01-10 14:00:00', 1),
    (1000, ARRAY[25, 50], '{"reserve0": 500, "reserve1": 1000}', 1, '2021-01-10 15:00:00', 2),

    (1000, ARRAY[25, 50], '{"reserve0": 500, "reserve1": 1000}', 1, '2021-01-10 14:00:00', 5),
    (1000, ARRAY[25, 50], '{"reserve0": 500, "reserve1": 1000}', 1, '2021-01-10 15:00:00', 6),

    (1000, ARRAY[25, 50], '{"reserve0": 500, "reserve1": 1000}', 1, '2021-01-10 14:00:00', 7),
    (1000, ARRAY[25, 50], '{"reserve0": 500, "reserve1": 1000}', 1, '2021-01-10 15:00:00', 8),

    (1000, ARRAY[25, 50], '{"reserve0": 500, "reserve1": 1000}', 1, '2021-01-10 14:00:00', 9),
    (1000, ARRAY[25, 50], '{"reserve0": 500, "reserve1": 1000}', 1, '2021-01-10 15:00:00', 10),

    (1000, ARRAY[25, 50], '{"reserve0": 500, "reserve1": 1000}', 1, '2021-01-10 14:00:00', 11),
    (1000, ARRAY[25, 50], '{"reserve0": 500, "reserve1": 1000}', 1, '2021-01-10 15:00:00', 12),
    -- updated state for entry with protocol_component_id=1
    (1000, ARRAY[15, 75], '{"reserve0": 250, "reserve1": 1250}', 1, '2021-01-10 15:00:00', 1);
    


INSERT INTO "contract"("chain_id", "title", "address", "creation_tx", "created_at", "deleted_at")
VALUES
    (1, 'ambient', E'\\xDEADBEEF69', 1, '2021-01-10 13:00:00', null),
    (1, 'wsteth token', E'\\xBADBABE420', 2, '2021-01-10 14:00:00', null),
    (1, 'DAI token', E'\\x6B175474E89094C44Da98b954EedeAC495271d0F', 2, '2021-01-10 14:00:00', null),
    (1, 'USDC token', E'\\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 2, '2021-01-10 14:00:00', null),
    (1, 'USDT token', E'\\xdAC17F958D2ee523a2206206994597C13D831ec7', 2, '2021-01-10 14:00:00', null);


INSERT INTO "token" 
("contract_id", "symbol", "decimals", "tax", "gas")
VALUES
(3, 'DAI', 18, null, ARRAY[21000, 21000, 21000, 60000]),
(4, 'USDC', 6, null, ARRAY[21000, 21000, 21000, 60000]),
(5, 'USDT', 6, null, ARRAY[21000, 21000, 21000, 60000]);


INSERT INTO protocol_holds_token(protocol_component_id, token_id)
VALUES 
    (1, 1),
    (1, 3),
    (2, 1),
    (2, 2),
    (3, 2),    
    (3, 3),
    (4, 1),
    (4, 2);

INSERT INTO contract_balance 
( "balance", "contract_id", "modify_tx", "valid_from", "valid_to")
VALUES
    (E'\\xde0b6b3a7640000', 1, 1, '2021-01-10 13:00:00', '2021-01-10 14:00:00'),
    (E'\\x0',               1, 1, '2021-01-10 14:00:00', null),
    (E'\\x0',               2, 2, '2021-01-10 13:00:00', '2021-01-10 13:00:00'),
    (E'\\xde0b6b3a7640000', 2, 2, '2021-01-10 13:00:00', null);

INSERT INTO contract_code ("code", "hash", "contract_id", "modify_tx", "valid_from", "valid_to") 
VALUES 
    (E'\\x00',E'\\x00',1,1,'2022-01-10 13:00:00+00',null),
    (E'\\x00',E'\\x00',2,2,'2022-01-10 13:00:00+00',null);

INSERT INTO contract_storage (slot, value, contract_id, modify_tx, valid_from, valid_to) 
VALUES 
    (E'\\x1234', E'\\x6789', 1, 1, '2022-01-10 13:00:00', '2022-01-10 14:00:00'),
    (E'\\x1234', E'\\xabcd', 1, 1, '2022-01-10 14:00:00', null),
    (E'\\x00',    E'\\x6789', 2, 2, '2022-01-10 13:00:00', '2022-01-10 14:00:00'),
    (E'\\x00',    E'\\xabcd', 2, 2, '2022-01-10 14:00:00', null);

INSERT INTO protocol_calls_contract(protocol_component_id, contract_id, valid_from) 
VALUES 
    (3, 1, '2021-01-10 13:00:00'),
    (4, 1, '2021-01-10 13:00:00');

