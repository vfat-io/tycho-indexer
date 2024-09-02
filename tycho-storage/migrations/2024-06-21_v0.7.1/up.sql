--- Add block_id to extraction_state (without NOT NULL constraint)
ALTER TABLE extraction_state
ADD COLUMN "block_id" bigint REFERENCES "block"(id);

-- populate existing extraction_state with oldest block_id
WITH oldest_block AS (
    SELECT id
    FROM "block"
    ORDER BY id ASC
    LIMIT 1
)
UPDATE extraction_state
SET block_id = (SELECT id FROM oldest_block);

-- add NOT NULL constraint
ALTER TABLE extraction_state ALTER COLUMN block_id SET NOT NULL;
