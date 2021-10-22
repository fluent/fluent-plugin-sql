require "test/unit"
require "test/unit/rr"
require "test/unit/notify" unless ENV['CI']
require "fluent/test"
require "fluent/plugin/out_sql"
require "fluent/plugin/in_sql"

load "fixtures/schema.rb"
