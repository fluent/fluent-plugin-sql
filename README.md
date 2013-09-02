# SQL input plugin for Fluentd event collector

## Overview

This sql input plugin reads records from a RDBMS periodically. Thus you can replicate tables to other storages through Fluentd.

## Configuration

    <source>
      type sql

      host rdb_host
      database rdb_database
      adapter mysql2_or_postgresql_etc
      user myusername
      password mypassword

      tag_prefix my.rdb

      select_interval 10s
      select_limit 100

      state_file state.yml

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

## Architecture

It runs following SQL every 10 seconds:

    SELECT * FROM rdb_database.table1 WHERE update_col1 > ${last_record[:update_col1]} ORDER BY update_col1 ASC LIMIT 100

