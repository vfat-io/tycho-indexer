-- Your SQL goes here

-- Make the "name" column of the protocol type unique
ALTER TABLE protocol_type
    ADD CONSTRAINT unique_name_constraint UNIQUE (name);