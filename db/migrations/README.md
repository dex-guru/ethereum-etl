## Usage

1. Read Alembic tutorial: https://alembic.sqlalchemy.org/en/latest/tutorial.html.
2. To create a new migration script, run:

    ```bash
    alembic revision -m "Some meaningful description of the migration"
    ```

   This will create a new migration script in the `db/migrations/versions` directory.
   The script will contain DDL statement examples that you can use as a reference.
3. Write the migration script. See the recommendations below
   in [Writing migrations](#writing-migrations).
4. Update schema DDL in `db/migrations/schema.sql` to reflect the new database state after
   the migration. This file is used for a reference only, for example, to see how the migration
   changes the database schema. This is helpful on code review.

   <br>

   Run the following command with a URL to a test Clickhouse instance. This will not modify any
   existing database objects and will clean up after itself (hopefully).

   ```bash
   CLICKHOUSE_URL="clickhouse+native://default:@localhost" python db/migrations/dump_schema.py
   ```
5. Run pytest to test the migrations.

## Writing migrations

* Do not ever modify the migration scripts that have already been merged to master branch.
* Migrations should be backward compatible. This means that the migration should be compatible
  with the previous app version that is already deployed to production. For example, if you add a
  new column to a table, the app code should not break if it does not use the new column. But if
  you rename a column, which is used by the app, the app will break.
* Do not place database name in the DDL statements. The database name should be implicitly
  inferred from the connection URL.
* Do not use `IF EXISTS` or `IF NOT EXISTS` in the DDL statements. This can lead to unexpected
  database state.
* Do not use `USE` statement in the migration scripts.
* Avoid long-running operations in the migration scripts.
* If you need to perform some long-running data transformation, consider doing DDL-only via
  the migration script and deploy a special worker to perform the data transformation.

## Deploying migrations manually

* Show current revision in the database

    ```bash
    CLICKHOUSE_URL="clickhouse+native://user:password@host:9000/db" alembic current
    ```
* Deploy the latest migrations

    ```bash
    CLICKHOUSE_URL="clickhouse+native://user:password@host:9000/db" alembic upgrade head
    ````

* Rollback the latest migration

    ```bash
    CLICKHOUSE_URL="clickhouse+native://user:password@host:9000/db" alembic downgrade -1
    ```

## Troubleshooting

If you get an error during the migration, you'll have to manually fix the database state.
Use Alembic's option `--sql` (aka offline mode) to generate the SQL script that would be executed
by Alembic.

```bash
alembic upgrade --sql head  # show all migrations sql from the beginning
alembic upgrade --sql <from_revision>:<to_revision>  # shows migrations sql in the given revision range
```

### Hacks

* Use `CLICKHOUSE_REPLICATED=1` env var to assume a replicated clickhouse instance when
  generating the schema DDL in offline (`--sql`) mode. 
