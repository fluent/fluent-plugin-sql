# SQL input plugin for [Fluentd](http://fluentd.org) event collector

## Overview

This SQL plugin has two parts:

1. SQL **input** plugin reads records from RDBMSes periodically. An example use case would be getting "diffs" of a table (based on the "updated_at" field).
2. SQL **output** plugin that writes records into RDBMSes. An example use case would be aggregating server/app/sensor logs into RDBMS systems.

## Requirements

| fluent-plugin-sql | fluentd    | ruby   |
|-------------------|------------|--------|
| >= 1.0.0          | >= v0.14.4 | >= 2.1 |
| <  1.0.0          | <  v0.14.0 | >= 1.9 |

NOTE: fluent-plugin-sql v2's buffer format is different from v1. If you update the plugin to v2, don't reuse v1's buffer.

## Installation

    $ fluent-gem install fluent-plugin-sql --no-document
    $ fluent-gem install pg --no-document # for postgresql

You should install actual RDBMS driver gem together. `pg` gem for postgresql adapter or `mysql2` gem for `mysql2` adapter. Other adapters supported by [ActiveRecord](https://github.com/rails/rails/tree/master/activerecord) should work.

We recommend that mysql2 gem is higher than `0.3.12` and pg gem is higher than `0.16.0`.

If you use ruby 2.1, use pg gem 0.21.0 (< 1.0.0) because ActiveRecord 5.1.4 or earlier doesn't support Ruby 2.1.

## Input: How It Works

This plugin runs following SQL periodically:

SELECT * FROM *table* WHERE *update\_column* > *last\_update\_column\_value* ORDER BY *update_column* ASC LIMIT 500

What you need to configure is *update\_column*. The column should be an incremental column (such as AUTO\_ INCREMENT primary key) so that this plugin reads newly INSERTed rows. Alternatively, you can use a column incremented every time when you update the row (such as `last_updated_at` column) so that this plugin reads the UPDATEd rows as well.
If you omit to set *update\_column* parameter, it uses primary key.

It stores last selected rows to a file (named *state\_file*) to not forget the last row when Fluentd restarts.

## Input: Configuration

    <source>
      @type sql

      host rdb_host
	  port rdb_port
      database rdb_database
      adapter mysql2_or_postgresql_or_etc
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
        time_format %Y-%m-%d %H:%M:%S.%6N # optional
      </table>

      # detects all tables instead of <table> sections
      #all_tables
    </source>

* **host** RDBMS host
* **port** RDBMS port
* **database** RDBMS database name
* **adapter** RDBMS driver name. You should install corresponding gem before start (mysql2 gem for mysql2 adapter, pg gem for postgresql adapter, etc.)
* **username** RDBMS login user name
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
* **primary_key** (optional): if you want to get data from the table which doesn't have primary key like PostgreSQL's View, set this parameter.
* **time_format** (optional): if you want to specify the format of the date used in the query, useful when using alternative adapters which have restrictions on format

## Input: Limitation

You should make sure target tables have index (and/or partitions) on the *update\_column*. Otherwise SELECT causes full table scan and serious performance problem.

You can't replicate DELETEd rows.

## Output: How It Works

This plugin takes advantage of ActiveRecord underneath. For `host`, `port`, `database`, `adapter`, `username`, `password`, `socket` parameters, you can think of ActiveRecord's equivalent parameters.

## Output: Configuration

    <match my.rdb.*>
      @type sql
      host rdb_host
      port 3306
      database rdb_database
      adapter mysql2_or_postgresql_or_etc
      username myusername
      password mypassword
      socket path_to_socket
      remove_tag_prefix my.rdb # optional, dual of tag_prefix in in_sql

      <table>
        table table1
        column_mapping 'timestamp:created_at,fluentdata1:dbcol1,fluentdata2:dbcol2,fluentdata3:dbcol3'
        # This is the default table because it has no "pattern" argument in <table>
        # The logic is such that if all non-default <table> blocks
        # do not match, the default one is chosen.
        # The default table is required.
      </table>

      <table hello.*> # You can pass the same pattern you use in match statements.
        table table2
        # This is the non-default table. It is chosen if the tag matches the pattern
        # AFTER remove_tag_prefix is applied to the incoming event. For example, if
        # the message comes in with the tag my.rdb.hello.world, "remove_tag_prefix my.rdb"
        # makes it "hello.world", which gets matched here because of "pattern hello.*".
      </table>
      
      <table hello.world>
        table table3
        # This is the second non-default table. You can have as many non-default tables
        # as you wish. One caveat: non-default tables are matched top-to-bottom and
        # the events go into the first table it matches to. Hence, this particular table
        # never gets any data, since the above "hello.*" subsumes "hello.world".
      </table>
    </match>

* **host** RDBMS host
* **port** RDBMS port
* **database** RDBMS database name
* **adapter** RDBMS driver name. You should install corresponding gem before start (mysql2 gem for mysql2 adapter, pg gem for postgresql adapter, etc.)
* **username** RDBMS login user name
* **password** RDBMS login password
* **socket** RDBMS socket path
* **pool** A connection pool synchronizes thread access to a limited number of database connections
* **timeout** RDBMS connection timeout
* **remove_tag_prefix** remove the given prefix from the events. See "tag_prefix" in "Input: Configuration". (optional)

\<table\> sections:

* **table** RDBM table name
* **column_mapping**: [Required] Record to table schema mapping. The format is consists of `from:to` or `key` values are separated by `,`. For example, if set 'item_id:id,item_text:data,updated_at' to **column_mapping**, `item_id` field of record is stored into `id` column and `updated_at` field of record is stored into `updated_at` column.
* **\<table pattern\>**: the pattern to which the incoming event's tag (after it goes through `remove_tag_prefix`, if given). The patterns should follow the same syntax as [that of \<match\>](https://docs.fluentd.org/configuration/config-file#how-match-patterns-work). **Exactly one \<table\> element must NOT have this parameter so that it becomes the default table to store data**.
