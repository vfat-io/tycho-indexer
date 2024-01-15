-- dropping tables will drop any triggers or indices along with it
ALTER TABLE protocol_system
ALTER COLUMN "name" TYPE varchar(255);

-- custom types
DROP TYPE protocol_system_type;


