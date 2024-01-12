-- This file should undo anything in `up.sql`
ALTER TABLE protocol_type
    DROP CONSTRAINT unique_name_constraint;