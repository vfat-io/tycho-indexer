ALTER TABLE extraction_state
ADD COLUMN "block_id" bigint REFERENCES "block"(id) NOT NULL;