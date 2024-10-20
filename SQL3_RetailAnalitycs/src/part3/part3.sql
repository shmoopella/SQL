CREATE ROLE administrator;
GRANT pg_signal_backend TO administrator;

CREATE ROLE visitor;
GRANT pg_read_all_data TO visitor;

-- SELECT rolname FROM pg_roles