SELECT cron.unschedule('clean_transaction_table');

DROP FUNCTION IF EXISTS clean_transaction_table();