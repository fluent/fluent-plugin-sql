#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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

require File.expand_path('../format_string', File.dirname(__FILE__))


class SQLOutput < ObjectBufferedOutput
  Plugin.register_output('sql', self)

  class SQLOutputFormatContext < FormatContext
    include TimeFormatContextMixin
    include HostnameContextMixin

    def initialize(params)
      @record = params[:record] || {}
      @tag = params[:tag] || nil
      super
    end

    def record(column)
      @record[column.to_s]
    end

    def json
      @record.to_json
    end

    def time
      @time.to_i  # TimeFormatContextMixin
    end

    def tag
      @tag
    end
  end

  def initialize
    require 'sequel'
    super
  end

  config_param :url, :string
  config_param :begin, :string, :default => nil
  config_param :insert, :string
  config_param :insert_params, :string, :default => nil
  config_param :commit, :string, :default => nil
  config_param :rollback, :string, :default => nil

  def configure(conf)
    super

    @insert_params_formats = []
    if @insert_params
      @insert_params.split(/\s*,\s*/).each {|s|
        @insert_params_formats << FormatString.new(s)
      }
    end

    @insert_format = FormatString.new(@insert)
  end

  def write_objects(tag, chunk)
    execute do |db|
      db.run @begin if @begin
      begin
        chunk.each {|time,record|
          context = SQLOutputFormatContext.new(:tag=>tag, :time=>Time.at(time), :record=>record)
          query = @insert_format.format(context)
          params = @insert_params_formats.map {|f|
            f.format(context)
          }

          db["insert #{query}", *params].insert
        }
        db.run @commit if @commit
      rescue
        $log.warn $!
        $log.warn_backtrace
        db.run @rollback if @rollback
      end
    end
  end

  def execute(&block)
    db = Sequel.connect(@url, :max_connections=>1)
    begin
      block.call(db)
    rescue
      $log.warn "in_sql: #{$!}"
      $log.warn_backtrace
    ensure
      db.disconnect
    end
  end
end


end
