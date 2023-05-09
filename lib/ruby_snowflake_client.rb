# frozen_string_literal: true

#$LOAD_PATH << File.dirname(__FILE__)
#$LOAD_PATH << File.expand_path("../ext", __dir__)# File.dirname(__FILE__) + "/../ext"
#$LOAD_PATH << "/Users/alexstoick/Desktop/go-ruby-snowflake-connector/ext"

require "ffi"
require "benchmark"
require_relative "../ext/ruby_snowflake_client" # built bundle of the go files

module Snowflake
  class Result
    def get_rows_with_blk(&blk)
      GC.disable
      arr = get_rows(&blk)
    ensure
      GC.enable
      GC.start
    end

    def get_all_rows
      GC.disable
      arr = get_rows.to_a
      return arr
    ensure
      GC.enable
      GC.start
    end
  end
end
