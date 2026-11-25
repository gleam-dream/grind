--- migration:up

SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended('grind-migrate-v1:' || current_schema(), 0))) l;

INSERT INTO grind_schema_migrations (version) VALUES (15);

--- migration:down

DELETE FROM grind_schema_migrations WHERE version = 15;

--- migration:end
