require "active_record"
ActiveRecord::Base.establish_connection(host: "localhost",
                                        port: 5432,
                                        username: "fluentd",
                                        password: "fluentd",
                                        adapter: "postgresql",
                                        database: "fluentd_test")
ActiveRecord::Schema.define(version: 20160225030107) do
  create_table "logs", force: :cascade do |t|
    t.string   "host"
    t.string   "ident"
    t.string   "pid"
    t.text     "message"
    t.datetime "created_at",  null: false
    t.datetime "updated_at",  null: false
  end

  create_table "messages", force: :cascade do |t|
    t.string "message"
    t.datetime "created_at",  null: false
    t.datetime "updated_at",  null: false
  end

  create_table "messages_custom_time", force: :cascade do |t|
    t.string "message"
    t.datetime "created_at",  null: false
    t.datetime "updated_at",  null: false
    t.string "custom_time"
  end
end

