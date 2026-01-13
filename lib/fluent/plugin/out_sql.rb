require "fluent/plugin/output"

require 'active_record'
require 'activerecord-import'

module Fluent::Plugin
  class SQLOutput < Output
    Fluent::Plugin.register_output('sql', self)

    helpers :inject, :compat_parameters, :event_emitter

    desc 'RDBMS host'
    config_param :host, :string
    desc 'RDBMS port'
    config_param :port, :integer, default: nil
    desc 'RDBMS driver name.'
    config_param :adapter, :string
    desc 'RDBMS login user name'
    config_param :username, :string, default: nil
    desc 'RDBMS login password'
    config_param :password, :string, default: nil, secret: true
    desc 'RDBMS database name'
    config_param :database, :string
    desc 'RDBMS socket path'
    config_param :socket, :string, default: nil
    desc 'PostgreSQL schema search path'
    config_param :schema_search_path, :string, default: nil
    desc 'remove the given prefix from the events'
    config_param :remove_tag_prefix, :string, default: nil
    desc 'enable fallback'
    config_param :enable_fallback, :bool, default: true
    desc "size of ActiveRecord's connection pool"
    config_param :pool, :integer, default: 5
    desc "specifies the timeout to establish a new connection to the database before failing"
    config_param :timeout, :integer, default: 5000

    config_section :buffer do
      config_set_default :chunk_keys, ["tag"]
    end

    attr_accessor :tables

    # TODO: Merge SQLInput's TableElement
    class TableElement
      include Fluent::Configurable

      config_param :table, :string
      config_param :column_mapping, :string
      config_param :num_retries, :integer, default: 5

      attr_reader :model
      attr_reader :pattern

      def initialize(pattern, log, enable_fallback)
        super()
        @pattern = Fluent::MatchPattern.create(pattern)
        @log = log
        @enable_fallback = enable_fallback
      end

      def configure(conf)
        super

        @mapping = parse_column_mapping(@column_mapping)
        @format_proc = Proc.new { |record|
          new_record = {}
          @mapping.each { |k, c|
            new_record[c] = record[k]
          }
          new_record
        }
      end

      def init(base_model)
        # See SQLInput for more details of following code
        table_name = @table
        @model = Class.new(base_model) do
          self.table_name = table_name
          self.inheritance_column = '_never_use_output_'
        end

        class_name = table_name.singularize.camelize
        base_model.const_set(class_name, @model)
        model_name = ActiveModel::Name.new(@model, nil, class_name)
        @model.define_singleton_method(:model_name) { model_name }

        # TODO: check column_names and table schema
        # @model.column_names
      end

      def import(chunk, output)
        tag = chunk.metadata.tag
        records = []
        chunk.msgpack_each { |time, data|
          begin
            data = output.inject_values_to_record(tag, time, data)
            records << @model.new(@format_proc.call(data))
          rescue => e
            args = {error: e, table: @table, record: Yajl.dump(data)}
            @log.warn "Failed to create the model. Ignore a record:", args
          end
        }
        begin
          @model.import(records)
        rescue ActiveRecord::StatementInvalid, ActiveRecord::Import::MissingColumnError => e
          if @enable_fallback
            # ignore other exceptions to use Fluentd retry mechanizm
            @log.warn "Got deterministic error. Fallback to one-by-one import", error: e
            one_by_one_import(records)
          else
            @log.warn "Got deterministic error. Fallback is disabled", error: e
            raise e
          end
        end
      end

      def one_by_one_import(records)
        records.each { |record|
          retries = 0
          begin
            @model.import([record])
          rescue ActiveRecord::StatementInvalid, ActiveRecord::Import::MissingColumnError => e
            @log.error "Got deterministic error again. Dump a record", error: e, record: record
          rescue => e
            retries += 1
            if retries > @num_retries
              @log.error "Can't recover undeterministic error. Dump a record", error: e, record: record
              next
            end

            @log.warn "Failed to import a record: retry number = #{retries}", error: e
            sleep 0.5
            retry
          end
        }
      end

      private

      def parse_column_mapping(column_mapping_conf)
        mapping = {}
        column_mapping_conf.split(',').each { |column_map|
          key, column = column_map.strip.split(':', 2)
          column = key if column.nil?
          mapping[key] = column
        }
        mapping
      end
    end

    def initialize
      super
    end

    def configure(conf)
      compat_parameters_convert(conf, :inject, :buffer)

      super

      if remove_tag_prefix = conf['remove_tag_prefix']
        @remove_tag_prefix = Regexp.new('^' + Regexp.escape(remove_tag_prefix))
      end

      @tables = []
      @default_table = nil
      conf.elements.select { |e|
        e.name == 'table'
      }.each { |e|
        te = TableElement.new(e.arg, log, @enable_fallback)
        te.configure(e)
        if e.arg.empty?
          $log.warn "Detect duplicate default table definition" if @default_table
          @default_table = te
        else
          @tables << te
        end
      }

      if @pool < @buffer_config.flush_thread_count
        log.warn "connection pool size is smaller than buffer's flush_thread_count. Recommend to increase pool value", :pool => @pool, :flush_thread_count => @buffer_config.flush_thread_count
      end

      if @default_table.nil?
        raise Fluent::ConfigError, "There is no default table. <table> is required in sql output"
      end
    end

    def start
      super

      config = {
        adapter: @adapter,
        host: @host,
        port: @port,
        database: @database,
        username: @username,
        password: @password,
        socket: @socket,
        schema_search_path: @schema_search_path,
        pool: @pool,
        timeout: @timeout,
      }

      @base_model = Class.new(ActiveRecord::Base) do
        self.abstract_class = true
      end

      SQLOutput.const_set("BaseModel_#{rand(1 << 31)}", @base_model)
      @base_model.establish_connection(config)

      # ignore tables if TableElement#init failed
      @tables.reject! do |te|
        init_table(te, @base_model)
      end
      init_table(@default_table, @base_model)
    end

    def shutdown
      super
    end

    def formatted_to_msgpack_binary
      true
    end

    def write(chunk)
      @base_model.connection_pool.with_connection do

        @tables.each { |table|
          tag = format_tag(chunk.metadata.tag)
          if table.pattern.match(tag)
            return table.import(chunk, self)
          end
        }
        @default_table.import(chunk, self)
      end
    end

    private

    def init_table(te, base_model)
      begin
        te.init(base_model)
        log.info "Selecting '#{te.table}' table"
        false
      rescue => e
        log.warn "Can't handle '#{te.table}' table. Ignoring.", error: e
        log.warn_backtrace e.backtrace
        true
      end
    end

    def format_tag(tag)
      if tag && @remove_tag_prefix
        tag.gsub(@remove_tag_prefix, '')
      else
        tag
      end
    end
  end
end
