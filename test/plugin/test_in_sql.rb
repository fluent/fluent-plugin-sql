require "helper"

class SqlInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def teardown
  end

  CONFIG = %[
    adapter postgresql
    host localhost
    port 5432
    database fluentd_test

    username fluentd
    password fluentd

    tag_prefix db

    <table>
      table messages
      tag logs
      update_column updated_at
      time_column updated_at
    </table>
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::SQLInput).configure(conf)
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
      tag_prefix: "db"
    }
    actual = {
      host: d.instance.host,
      port: d.instance.port,
      adapter: d.instance.adapter,
      database: d.instance.database,
      username: d.instance.username,
      password: d.instance.password,
      tag_prefix: d.instance.tag_prefix
    }
    assert_equal(expected, actual)
    tables = d.instance.instance_variable_get(:@tables)
    assert_equal(1, tables.size)
    messages = tables.first
    assert_equal("messages", messages.table)
    assert_equal("logs", messages.tag)
  end

  def test_message
    d = create_driver(CONFIG + "select_interval 1")
    Message.create!(message: "message 1")
    Message.create!(message: "message 2")
    Message.create!(message: "message 3")

    d.run do
    end
    assert_equal("db.logs", d.emits[0][0])
    expected = [
      [d.emits[0][1], "message 1"],
      [d.emits[1][1], "message 2"],
      [d.emits[2][1], "message 3"],
    ]
    actual = [
      [Time.parse(d.emits[0][2]["updated_at"]).to_i, d.emits[0][2]["message"]],
      [Time.parse(d.emits[1][2]["updated_at"]).to_i, d.emits[1][2]["message"]],
      [Time.parse(d.emits[2][2]["updated_at"]).to_i, d.emits[2][2]["message"]],
    ]
    assert_equal(expected, actual)
  end

  class Message < ActiveRecord::Base
  end
end
