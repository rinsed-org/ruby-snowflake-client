# frozen_string_literal: true

#$LOAD_PATH << File.dirname(__FILE__)
#$LOAD_PATH << File.expand_path("../ext", __dir__)# File.dirname(__FILE__) + "/../ext"
#$LOAD_PATH << "/Users/alexstoick/Desktop/go-ruby-snowflake-connector/ext"

require "ffi"
require "benchmark"
require_relative "../ext/ruby_snowflake_client" # built bundle of the go files

module Snowflake
  class Result
    FINALIZER = lambda { |object_id| p "finalizing result %d" % object_id }

    def get_rows_rb
      GC.disable
      arr = _internal_rows.to_a.map(&:to_h)
      return arr
    ensure
      GC.enable
      GC.start
    end

    private
      def _internal_rows
        arr = get_rows.to_a
      end
  end
end
