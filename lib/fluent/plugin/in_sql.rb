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
module Fluent

  require 'active_record'

  class SQLInput < Input
    Plugin.register_input('sql', self)

    config_param :host, :string
    config_param :port, :integer, :default => nil
    config_param :adapter, :string
    config_param :database, :string
    config_param :username, :string, :default => nil
    config_param :password, :string, :default => nil

    config_param :state_file, :string, :default => nil
    config_param :tag_prefix, :string, :default => nil
    config_param :select_interval, :time, :default => 60
    config_param :select_limit, :time, :default => 500

    class TableElement
      include Configurable

      config_param :table, :string
      config_param :tag, :string, :default => nil
      config_param :update_column, :string, :default => nil
      config_param :time_column, :string, :default => nil

      def configure(conf)
        super

        unless @state_file
          $log.warn "'state_file PATH' parameter is not set to a 'sql' source."
          $log.warn "this parameter is highly recommended to save the last rows to resume tailing."
        end
      end

      def init(tag_prefix, base_model)
        @tag = "#{tag_prefix}.#{@tag}" if tag_prefix

        # creates a model for this table
        table_name = @table
        @model = Class.new(base_model) do
          self.table_name = table_name
          self.inheritance_column = '_never_use_'
        end

        # ActiveRecord requires model class to have a name.
        class_name = table_name.singularize.camelize
        base_model.const_set(class_name, @model)

        # Sets model_name otherwise ActiveRecord causes errors
        model_name = ActiveModel::Name.new(@model, nil, class_name)
        @model.define_singleton_method(:model_name) { model_name }

        # if update_column is not set, here uses primary key
        unless @update_column
          columns = Hash[@model.columns.map {|c| [c.name, c] }]
          pk = columns[@model.primary_key]
          unless pk
            raise "Composite primary key is not supported. Set update_column parameter to <table> section."
          end
          @update_column = pk.name
        end
      end

      # emits next records and returns the last record of emitted records
      def emit_next_records(last_record, limit)
        relation = @model
        if last_record && last_update_value = last_record[@update_column]
          relation = relation.where("#{@update_column} > ?", last_update_value)
        end
        relation = relation.order("#{@update_column} ASC").limit(limit)

        now = Engine.now
        entry_name = @model.table_name.singularize

        me = MultiEventStream.new
        relation.each do |obj|
          record = obj.as_json[entry_name] rescue nil
          if record
            if tv = record[@time_column]
              time = Time.parse(tv.to_s) rescue now
            else
              time = now
            end
            me.add(time, record)
            last_record = record
          end
        end

        last_record = last_record.dup  # some plugin rewrites record :(
        Engine.emit_stream(@tag, me)

        return last_record
      end
    end

    def configure(conf)
      super

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
      @state_store = StateStore.new(@state_file)

      config = {
        :adapter => @adapter,
        :host => @host,
        :port => @port,
        :database => @database,
        :username => @username,
        :password => @password,
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
      SQLInput.const_set("BaseModel_#{rand(1<<31)}", @base_model)

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
          te.init(@tag_prefix, @base_model)
          $log.info "Selecting '#{te.table}' table"
          false
        rescue
          $log.warn "Can't handle '#{te.table}' table. Ignoring.", :error => $!
          $log.warn_backtrace $!.backtrace
          true
        end
      end

      @stop_flag = false
      @thread = Thread.new(&method(:thread_main))
    end

    def shutdown
      @stop_flag = true
    end

    def thread_main
      until @stop_flag
        sleep @select_interval

        @tables.each do |t|
          begin
            last_record = @state_store.last_records[t.table]
            @state_store.last_records[t.table] = t.emit_next_records(last_record, @select_limit)
            @state_store.update!
          rescue
            $log.error "unexpected error", :error=>$!.to_s
            $log.error_backtrace
          end
        end
      end
    end

    class StateStore
      def initialize(path)
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
  end

end
