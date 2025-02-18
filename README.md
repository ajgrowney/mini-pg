## Postgres Data Layout

### Relation Location

Table / index data is stored in its own file, which you can find using the `pg_relation_filepath` function

Relative to the data directory, it sits at `base/{schema_id}/{table_id}.tbl` (or `.idx`)
