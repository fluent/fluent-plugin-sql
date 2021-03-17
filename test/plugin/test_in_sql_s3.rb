require "helper"
require "fluent/test/driver/input"

class SqlInputTestS3 < Test::Unit::TestCase
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

    s3_bucket_name joustie
    s3_bucket_key test.test
    aws_region eu-west-1

    schema_search_path public

    tag_prefix db

    <table>
      table messages
      tag logs
      update_column updated_at
      time_column updated_at
    </table>
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::SQLInput).configure(conf)
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
      schema_search_path: "public",
      tag_prefix: "db"
    }
    actual = {
      host: d.instance.host,
      port: d.instance.port,
      adapter: d.instance.adapter,
      database: d.instance.database,
      username: d.instance.username,
      password: d.instance.password,
      schema_search_path: d.instance.schema_search_path,
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
    Message.create!(message: "message 4")
    Message.create!(message: "message 5")
    Message.create!(message: "message 6")

    d.end_if do
      d.record_count >= 3
    end
    d.run

    assert_equal("db.logs", d.events[0][0])
    expected = [
      [d.events[0][1], "message 4"],
      [d.events[1][1], "message 5"],
      [d.events[2][1], "message 6"],
    ]
    actual = [
      [Fluent::EventTime.parse(d.events[4][2]["updated_at"]), d.events[4][2]["message"]],
      [Fluent::EventTime.parse(d.events[5][2]["updated_at"]), d.events[5][2]["message"]],
      [Fluent::EventTime.parse(d.events[6][2]["updated_at"]), d.events[6][2]["message"]],
    ]
    assert_equal(expected, actual)
  end

  class Message < ActiveRecord::Base
  end
end
