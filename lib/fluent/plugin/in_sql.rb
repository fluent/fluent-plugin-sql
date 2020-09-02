#
# Fluent
#
# Copyright (C) 2013 FURUHASHI Sadayuki
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require "fluent/plugin/input"

module Fluent::Plugin

  require 'active_record'

  class SQLInput < Input
    Fluent::Plugin.register_input('sql', self)

    desc 'RDBMS host'
    config_param :host, :string
    desc 'RDBMS port'
    config_param :port, :integer, default: nil
    desc 'RDBMS driver name.'
    config_param :adapter, :string
    desc 'RDBMS database name'
    config_param :database, :string
    desc 'RDBMS login user name'
    config_param :username, :string, default: nil
    desc 'RDBMS login password'
    config_param :password, :string, default: nil, secret: true
    desc 'RDBMS socket path'
    config_param :socket, :string, default: nil
    desc 'PostgreSQL schema search path'
    config_param :schema_search_path, :string, default: nil

    desc 'path to a file to store last rows'
    config_param :state_file, :string, default: nil
    desc 'prefix of tags of events. actual tag will be this_tag_prefix.tables_tag (optional)'
    config_param :tag_prefix, :string, default: nil
    desc 'interval to run SQLs (optional)'
    config_param :select_interval, :time, default: 60
    desc 'limit of number of rows for each SQL(optional)'
    config_param :select_limit, :time, default: 500

    class TableElement
      include Fluent::Configurable

      config_param :table, :string
      config_param :tag, :string, default: nil
      config_param :update_column, :string, default: nil
      config_param :time_column, :string, default: nil
      config_param :primary_key, :string, default: nil

      attr_reader :log

      def configure(conf)
        super
      end

      def init(tag_prefix, base_model, router, log)
        @router = router
        @tag = "#{tag_prefix}.#{@tag}" if tag_prefix
        @log = log

        # creates a model for this table
        table_name = @table
        primary_key = @primary_key
        @model = Class.new(base_model) do
          self.table_name = table_name
          self.inheritance_column = '_never_use_'
          self.primary_key = primary_key if primary_key

          #self.include_root_in_json = false

          def read_attribute_for_serialization(n)
            v = send(n)
            if v.respond_to?(:to_msgpack)
              v
            elsif v.is_a? Time
              v.strftime('%Y-%m-%d %H:%M:%S.%6N%z')
            else
              v.to_s
            end
          end
        end

        # ActiveRecord requires model class to have a name.
        class_name = table_name.gsub(/\./, "_").singularize.camelize
        base_model.const_set(class_name, @model)

        # Sets model_name otherwise ActiveRecord causes errors
        model_name = ActiveModel::Name.new(@model, nil, class_name)
        @model.define_singleton_method(:model_name) { model_name }

        # if update_column is not set, here uses primary key
        unless @update_column
          pk = @model.columns_hash[@model.primary_key]
          unless pk
            raise "Composite primary key is not supported. Set update_column parameter to <table> section."
          end
          @update_column = pk.name
        end
      end

      # Make sure we always have a Fluent::EventTime object regardless of what comes in
      def normalized_time(tv, now)
        return Fluent::EventTime.from_time(tv) if tv.is_a?(Time)
        begin
          Fluent::EventTime.parse(tv.to_s)
        rescue
          log.warn "Message contains invalid timestamp, using current time instead (#{now.inspect})"
          now
        end
      end

      # emits next records and returns the last record of emitted records
      def emit_next_records(last_record, limit)
        relation = @model
        if last_record && last_update_value = last_record[@update_column]
          relation = relation.where("#{@update_column} > ?", last_update_value)
        end
        relation = relation.order("#{@update_column} ASC")
        relation = relation.limit(limit) if limit > 0

        now = Fluent::Engine.now

        me = Fluent::MultiEventStream.new
        relation.each do |obj|
          record = obj.serializable_hash rescue nil
          if record
            time =
              if @time_column && (tv = obj.read_attribute(@time_column))
                normalized_time(tv, now)
              else
                now
              end

            me.add(time, record)
            last_record = record
          end
        end

        last_record = last_record.dup if last_record  # some plugin rewrites record :(
        @router.emit_stream(@tag, me)

        return last_record
      end
    end

    def configure(conf)
      super

      unless @state_file
        $log.warn "'state_file PATH' parameter is not set to a 'sql' source."
        $log.warn "this parameter is highly recommended to save the last rows to resume tailing."
      end

      @tables = conf.elements.select {|e|
        e.name == 'table'
      }.map {|e|
        te = TableElement.new
        te.configure(e)
        te
      }

      if config['all_tables']
        @all_tables = true
      end
    end

    SKIP_TABLE_REGEXP = /\Aschema_migrations\Z/i

    def start
      @state_store = @state_file.nil? ? MemoryStateStore.new : StateStore.new(@state_file)

      config = {
        adapter: @adapter,
        host: @host,
        port: @port,
        database: @database,
        username: @username,
        password: @password,
        socket: @socket,
        schema_search_path: @schema_search_path,
      }

      # creates subclass of ActiveRecord::Base so that it can have different
      # database configuration from ActiveRecord::Base.
      @base_model = Class.new(ActiveRecord::Base) do
        # base model doesn't have corresponding phisical table
        self.abstract_class = true
      end

      # ActiveRecord requires the base_model to have a name. Here sets name
      # of an anonymous class by assigning it to a constant. In Ruby, class has
      # a name of a constant assigned first
      SQLInput.const_set("BaseModel_#{rand(1 << 31)}", @base_model)

      # Now base_model can have independent configuration from ActiveRecord::Base
      @base_model.establish_connection(config)

      if @all_tables
        # get list of tables from the database
        @tables = @base_model.connection.tables.map do |table_name|
          if table_name.match(SKIP_TABLE_REGEXP)
            # some tables such as "schema_migrations" should be ignored
            nil
          else
            te = TableElement.new
            te.configure({
              'table' => table_name,
              'tag' => table_name,
              'update_column' => nil,
            })
            te
          end
        end.compact
      end

      # ignore tables if TableElement#init failed
      @tables.reject! do |te|
        begin
          te.init(@tag_prefix, @base_model, router, log)
          log.info "Selecting '#{te.table}' table"
          false
        rescue => e
          log.warn "Can't handle '#{te.table}' table. Ignoring.", error: e
          log.warn_backtrace e.backtrace
          true
        end
      end

      @stop_flag = false
      @thread = Thread.new(&method(:thread_main))
    end

    def shutdown
      @stop_flag = true
      $log.debug "Waiting for thread to finish"
      @thread.join
    end

    def thread_main
      until @stop_flag
        sleep @select_interval

        begin
          conn = @base_model.connection
          conn.active? || conn.reconnect!
        rescue => e
          log.warn "can't connect to database. Reconnect at next try"
          next
        end

        @tables.each do |t|
          begin
            last_record = @state_store.last_records[t.table]
            @state_store.last_records[t.table] = t.emit_next_records(last_record, @select_limit)
            @state_store.update!
          rescue => e
            log.error "unexpected error", error: e
            log.error_backtrace e.backtrace
          end
        end
      end
    end

    class StateStore
      def initialize(path)
        require 'yaml'

        @path = path
        if File.exists?(@path)
          @data = YAML.load_file(@path)
          if @data == false || @data == []
            # this happens if an users created an empty file accidentally
            @data = {}
          elsif !@data.is_a?(Hash)
            raise "state_file on #{@path.inspect} is invalid"
          end
        else
          @data = {}
        end
      end

      def last_records
        @data['last_records'] ||= {}
      end

      def update!
        File.open(@path, 'w') {|f|
          f.write YAML.dump(@data)
        }
      end
    end

    class MemoryStateStore
      def initialize
        @data = {}
      end

      def last_records
        @data['last_records'] ||= {}
      end

      def update!
      end
    end
  end

end
