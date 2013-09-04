# SQL input plugin for Fluentd event collector

## Overview

This sql input plugin reads records from a RDBMS periodically. Thus you can copy tables to other storages through Fluentd.

## How does it work?

This plugin runs following SQL repeatedly every 60 seconds to *tail* a table like `tail` command of UNIX.

SELECT * FROM *table* WHERE *update\_column* > *last\_update\_column\_value* ORDER BY *update_column* ASC LIMIT 500

What you need to configure is *update\_column*. The column should be incremental column (such as AUTO\_ INCREMENT primary key) so that this plugin reads newly INSERTed rows. Alternatively, it should be updated every time when you update the row (such as `updated_at` column) so that this plugin reads the UPDATEd rows as well. If you omit to set *update\_column* parameter, it uses primary key.

It stores last selected rows to a file named state\_file to not forget the last row when fluentd restarted.

## Configuration

    <source>
      type sql

      host rdb_host
      database rdb_database
      adapter mysql2_or_postgresql_etc
      user myusername
      password mypassword

      tag_prefix my.rdb

      select_interval 60s
      select_limit 500

      state_file /var/run/fluentd/sql_state

      <table>
        tag table1
        table table1
        update_column update_col1
        time_column time_col2
      </table>

      <table>
        tag table2
        table table2
        update_column updated_at
        time_column updated_at
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
* **update_column**
* **time_column** (optional)

## Limitation

You should make sure target tables have index (and/or partitions) on the *update\_column*. Otherwise SELECT causes full table scan and serious performance problem.

You can't replicate DELETEd rows.

