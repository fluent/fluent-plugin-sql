# CREATE TABLE sample(id int auto_increment, text1 text, text2 text,
#   is_delete tinyint default 0, 
#   updated_at timestamp NULL 
#   default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP, PRIMARY KEY (id));
#
# - source section
# select_limit 0     <= important!! If same time of 'updated_at' records exist, you will miss to getting the some records. 
#                       You have to set unlimit for select query. And take care huge records.
#
# <table>
#   table sample
#   tag   sample
#   update_column updated_at
#   time_column updated_at
# </table>
#
# - store section
# <store>
#   type sql_memcached
#   host localhost
#   key id
#   value text1,text2
#   del_flag_col is_delete
#   del_flag_value 1
#   flush_interval 5s
#   max_retry_wait 1800
# </store>
#
# - check memcached
# telnet localhost 11211
# get sample:1     <=  <tag>:<key>
module Fluent

  class SqlMemcachedOutput < BufferedOutput
    Fluent::Plugin.register_output('sql_memcached', self)

    TAG_FORMAT = /(?<dbms>[^\.]+)\.(?<db>[^\.]+)\.(?<table>[^\.]+)$/

    def initialize
      super
      require "memcached"
    end

    def configure(conf)
      super

      @host = conf.has_key?('host') ? conf['host'] : 'localhost'
      @port = conf.has_key?('port') ? conf['port'].to_i : 11211
      @expire = conf.has_key?('expire') ? conf['expire'].to_i : 0

      @key = conf.has_key?('key') ? conf['key'] : 'id'
      @value = conf.has_key?('value') ? conf['value'] : 'id'
      @del_flag_col = conf.has_key?('del_flag_col') ? conf['del_flag_col'] : 'is_delete'
      @del_flag_value = conf.has_key?('del_flag_value') ? conf['del_flag_value'] : '1'

    end

    def start
      super
      @memcached = Memcached.new(@host.to_s + ":" + @port.to_s)
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def shutdown
      @memcached.quit
      super
    end

    def write(chunk)
      chunk.msgpack_each do |tag, time, record|
        tag_parts = tag.match(TAG_FORMAT)
        key = tag_parts['table'] + ":" + record[@key].to_s
        if record[@del_flag_col].to_s == @del_flag_value # "delete flag" is on.
#          @memcached.delete key
          @memcached.set key, "", 1, false # more better :)
        else
          value = "";
          arr = @value.split(",")
          arr.each do |v|
            value << record[v] + "\t"
          end
          @memcached.set key, value.chop, @expire, false # delete last "\t"
        end
      end
    end

  end

end