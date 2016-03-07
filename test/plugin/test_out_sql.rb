require "helper"

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

    database fluentd-test
    username fluentd
    password fluentd

    remove_tag_prefix db

    <table>
      table logs
      column_mapping timestamp:created_at,host:host,ident:ident,pid:pid,message:message
    </table>
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::SQLOutput).configure(conf)
  end

  def test_configure
    d = create_driver
    expected = {
      host: "localhost",
      port: 5432,
      adapter: "postgresql",
      database: "fluentd-test",
      username: "fluentd",
      password: "fluentd",
      remove_tag_suffix: /^db/
    }
    actual = {
      host: d.instance.host,
      port: d.instance.port,
      adapter: d.instance.adapter,
      database: d.instance.database,
      username: d.instance.username,
      password: d.instance.password,
      remove_tag_suffix: d.instance.remove_tag_prefix
    }
    assert_equal(expected, actual)
    assert_empty(d.instance.tables)
    default_table = d.instance.instance_variable_get(:@default_table)
    assert_equal("logs", default_table.table)
  end

  def test_emit
    d = create_driver
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.emit({"message" => "message1"}, time)
    d.emit({"message" => "message2"}, time)

    d.run

    default_table = d.instance.instance_variable_get(:@default_table)
    model = default_table.instance_variable_get(:@model)
    assert_equal(2, model.all.count)
    messages = model.pluck(:message).sort
    assert_equal(["message1", "message2"], messages)
  end
end
