# SQL input plugin for Fluentd event collector

## Overview

This sql input plugin reads records from a RDBMS periodically. Thus you can copy tables to other storages through Fluentd.

## How does it work?

This plugin runs following SQL periodically:

SELECT * FROM *table* WHERE *update\_column* > *last\_update\_column\_value* ORDER BY *update_column* ASC LIMIT 500

What you need to configure is *update\_column*. The column should be an incremental column (such as AUTO\_ INCREMENT primary key) so that this plugin reads newly INSERTed rows. Alternatively, you can use a column incremented every time when you update the row (such as `last_updated_at` column) so that this plugin reads the UPDATEd rows as well.
If you omit to set *update\_column* parameter, it uses primary key.

It stores last selected rows to a file (named *state\_file*) to not forget the last row when Fluentd restarts.

## Configuration

    <source>
      type sql

      host rdb_host
      database rdb_database
      adapter mysql2_or_postgresql_etc
      username myusername
      password mypassword

      tag_prefix my.rdb  # optional, but recommended

      select_interval 60s  # optional
      select_limit 500     # optional

      state_file /var/run/fluentd/sql_state

      <table>
        table table1
        tag table1  # optional
        update_column update_col1
        time_column time_col2  # optional
      </table>

      <table>
        table table2
        tag table2  # optional
        update_column updated_at
        time_column updated_at  # optional
      </table>

      # detects all tables instead of <table> sections
      #all_tables
    </source>

* **host** RDBMS host
* **port** RDBMS port
* **database** RDBMS database name
* **adapter** RDBMS driver name (mysql2 for MySQL, postgresql for PostgreSQL, etc.)
* **user** RDBMS login user name
* **password** RDBMS login password
* **tag_prefix** prefix of tags of events. actual tag will be this\_tag\_prefix.tables\_tag (optional)
* **select_interval** interval to run SQLs (optional)
* **select_limit** LIMIT of number of rows for each SQL (optional)
* **state_file** path to a file to store last rows
* **all_tables** reads all tables instead of configuring each tables in \<table\> sections

\<table\> sections:

* **tag** tag name of events (optional; default value is table name)
* **table** RDBM table name
* **update_column**: see above description
* **time_column** (optional): if this option is set, this plugin uses this column's value as the the event's time. Otherwise it uses current time.

## Limitation

You should make sure target tables have index (and/or partitions) on the *update\_column*. Otherwise SELECT causes full table scan and serious performance problem.

You can't replicate DELETEd rows.
