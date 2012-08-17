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


class FormatString
  def initialize(string)
    @array = []
    parse!(string)
  end

  def format(context)
    result = ''
    @array.each {|x|
      result << x.format(context).to_s
    }
    result
  end

  private
  def parse!(string)
    s = StringScanner.new(string)
    until s.empty?
      if s.scan(/%/)
        if s.scan(/%/)
          # %% => '%'
          add_string("%")

        elsif c = s.scan(/[a-zA-Z0-9_]/)
          # %C => C()
          add_code("#{c}()")

        elsif cs = s.scan(/(?=\{)(?<op>\{\g<op>[^\}]*\}\g<op>|)/)
          # %{code} => code
          add_code(cs[1..-2])

        else
          raise ConfigError, "invalid format string: #{string}"
        end

      else
        str = s.scan(/[^\%]+/)
        add_string(str)
      end
    end
  end

  def add_string(str)
    str.define_singleton_method(:format) {|context|
      str
    }
    @array << str
  end

  def add_code(code)
    code.define_singleton_method(:format) {|context|
      context.instance_eval(code)
    }
    @array << code
  end
end


class FormatContext
  def initialize(params)
  end
end


module TimeFormatContextMixin
  def initialize(params)
    @time = params[:time] || Time.now
    super
  end

  %w[Y C y m B b h d e j H k i l P p M S L N z Z A a u U W G g V s n t c D F v x X r R T].each {|c|
    define_method(c) do
      @time.strftime("%#{c}")
    end
  }
end


module HostnameContextMixin
  def initialize(params)
    @hostname = params[:hostname] || `hostname`.strip
  end

  def hostname
    @hostname
  end
end


end
