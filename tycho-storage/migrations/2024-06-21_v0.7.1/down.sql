-- This file should undo anything in `up.sql`
ALTER TABLE extraction_state
DROP COLUMN "block_id";