require "helper"
require "fluent/test/driver/input"

class SqlInputTestS3 < Test::Unit::TestCase
  
  CONFIG = %[
    adapter postgresql
    host localhost
    port 5432
    database fluentd_test

    username fluentd
    password fluentd

    s3_bucket_name fluentd-test12345
    s3_bucket_key sql.state
    aws_region us-east-1

    schema_search_path public

    tag_prefix db

    <table>
      table messages
      tag logs
      update_column updated_at
      time_column updated_at
    </table>
  ]

  def setup
    Fluent::Test.setup
    
    @bucket_name = 'fluentd-test12345'
    @bucket_key  = 'sql.state'
    @aws_region  = 'us-east-1'

    # creating the object_key for the test
    s3_client = Aws::S3::Client.new(region: @aws_region)
    s3_response = s3_client.put_object(
          bucket: @bucket_name,
          key: @bucket_key,
          body: ''
        )
    if s3_response.etag
      return true
    else
      return false
    end
    rescue StandardError => e
      puts "Error creating object: #{e.message}"
    return false
  end

  def teardown
  end

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
      [d.events[3][1], "message 4"],
      [d.events[4][1], "message 5"],
      [d.events[5][1], "message 6"],
    ]
    actual = [
      [Fluent::EventTime.parse(d.events[3][2]["updated_at"]), d.events[3][2]["message"]],
      [Fluent::EventTime.parse(d.events[4][2]["updated_at"]), d.events[4][2]["message"]],
      [Fluent::EventTime.parse(d.events[5][2]["updated_at"]), d.events[5][2]["message"]],
    ]
    assert_equal(expected, actual)
    
    # Test if last updated recordid is saved in state file on S3
    s3_client = Aws::S3::Client.new(region: @aws_region)
    s3_response = s3_client.get_object(
      bucket: @bucket_name, 
      key: @bucket_key)
    if s3_response
      @data = YAML.load(s3_response.body.read)
    else
      @data = {}
    end

    # Reported id by plugin
    expected = d.events[5][2]["id"]
    # Id saved in S3
    actual = @data['last_records']['messages']['id']

    assert_equal(expected, actual)
  end

  class Message < ActiveRecord::Base
  end
end
