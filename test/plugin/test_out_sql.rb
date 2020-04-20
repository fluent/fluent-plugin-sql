require "helper"
require "fluent/test/driver/output"

class SqlOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def teardown
  end

  CONFIG = %[
    host localhost
    port 5432
    adapter postgresql

    database fluentd_test
    username fluentd
    password fluentd

    schema_search_path public

    remove_tag_prefix db

    <table>
      table logs
      column_mapping timestamp:created_at,host:host,ident:ident,pid:pid,message:message
    </table>
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::SQLOutput).configure(conf)
  end

  def test_configure
    d = create_driver
    expected = {
      host: "localhost",
      port: 5432,
      adapter: "postgresql",
      database: "fluentd_test",
      username: "fluentd",
      password: "fluentd",
      schema_search_path: 'public',
      remove_tag_suffix: /^db/,
      enable_fallback: true,
      pool: 5
    }
    actual = {
      host: d.instance.host,
      port: d.instance.port,
      adapter: d.instance.adapter,
      database: d.instance.database,
      username: d.instance.username,
      password: d.instance.password,
      schema_search_path: d.instance.schema_search_path,
      remove_tag_suffix: d.instance.remove_tag_prefix,
      enable_fallback: d.instance.enable_fallback,
      pool: d.instance.pool
    }
    assert_equal(expected, actual)
    assert_empty(d.instance.tables)
    default_table = d.instance.instance_variable_get(:@default_table)
    assert_equal("logs", default_table.table)
  end

  def test_emit
    d = create_driver
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.run(default_tag: 'test') do
      d.feed(time, {"message" => "message1"})
      d.feed(time, {"message" => "message2"})
    end

    default_table = d.instance.instance_variable_get(:@default_table)
    model = default_table.instance_variable_get(:@model)
    assert_equal(2, model.all.count)
    messages = model.pluck(:message).sort
    assert_equal(["message1", "message2"], messages)
  end

  class Fallback < self
    def test_simple
      d = create_driver
      time = Time.parse("2011-01-02 13:14:15 UTC").to_i

      d.run(default_tag: 'test') do
        d.feed(time, {"message" => "message1"})
        d.feed(time, {"message" => "message2"})

        default_table = d.instance.instance_variable_get(:@default_table)
        model = default_table.instance_variable_get(:@model)
        mock(model).import(anything).at_least(1) do
          raise ActiveRecord::Import::MissingColumnError.new("dummy_table", "dummy_column")
        end
        mock(default_table).one_by_one_import(anything)
      end
    end

    def test_limit
      d = create_driver
      time = Time.parse("2011-01-02 13:14:15 UTC").to_i

      d.run(default_tag: 'test') do
        d.feed(time, {"message" => "message1"})
        d.feed(time, {"message" => "message2"})

        default_table = d.instance.instance_variable_get(:@default_table)
        model = default_table.instance_variable_get(:@model)
        mock(model).import([anything, anything]).once do
          raise ActiveRecord::Import::MissingColumnError.new("dummy_table", "dummy_column")
        end
        mock(model).import([anything]).times(12) do
          raise StandardError
        end
        assert_equal(5, default_table.instance_variable_get(:@num_retries))
      end
    end
  end
end
