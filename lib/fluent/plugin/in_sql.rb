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


class SQLInput < Input
  Plugin.register_input('sql', self)

  class SQLInputFormatContext < FormatContext
    include TimeFormatContextMixin
    include HostnameContextMixin

    def initialize(params)
      @last_values = params[:last_values] || {}
      super
    end

    def last_value(column)
      @last_values[column.to_sym]
    end
  end

  def initialize
    require 'sequel'
    require 'strscan'
    super
  end

  config_param :url, :string

  class QueryEntry
    include Fluent::Configurable

    #config_param :last_value_store_path, :string  # TODO
    config_param :select, :string     # %{last_value:time}
    config_param :interval, :time, :default => 60
    #config_param :schedule, :string, :default => nil  # TODO cron format
    config_param :tag, :string
    config_param :tag_column, :string, :default => nil
    config_param :time_column, :string, :default => nil

    def initialize(outer)
      @outer = outer
      @finished = false
      @thread = nil
      @next_time = Time.now
      super()
    end

    def configure(conf)
      super
      @query_format = FormatString.new(@select)
    end

    def start
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @finished = true
      @thread.join if @thread
    end

    private
    def run
      until @finished
        now = Time.now
        if @next_time <= now
          run_query(@next_time)
          @next_time = calc_next_time(@next_time)
        end
        sleep [1, @next_time - now].max
      end
    end

    def calc_next_time(before_time)
      before_time + @interval
    end

    def run_query(context_time)
      ess = {}

      @outer.execute {|db|
        context = SQLInputFormatContext.new(:time=>context_time)
        query = @query_format.format(context)

        db.fetch("SELECT #{query}") {|row|
          if @time_column
            t = row.delete(@time_column.to_sym)
            if t.is_a?(String)
              time = Time.parse(t).to_i
            else
              time = t.to_i
            end
          end
          time ||= context_time.to_i

          tag = @tag
          if @tag_column
            if t = row.delete(@tag_column.to_sym)
              tag = "#{tag}.#{t}"
            end
          end

          record = {}
          row.each_pair {|k,v|
            record[k.to_s] = v
          }

          es = (ess[tag] ||= MultiEventStream.new)
          es.add(time, record)
        }
      }

      ess.each_pair {|tag,es|
        Engine.emit_stream(tag, es)
      }
    end
  end

  def configure(conf)
    super

    @query_entries = []

    conf.elements.select {|e|
      e.name == 'query'
    }.each {|e|
      qe = QueryEntry.new(self)
      qe.configure(e)
      @query_entries << qe
    }
  end

  def start
    @query_entries.each {|qe|
      qe.start
    }
  end

  def shutdown
    @query_entries.each {|qe|
      qe.shutdown
    }
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

