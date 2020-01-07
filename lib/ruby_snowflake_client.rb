# frozen_string_literal: true

require "ruby_snowflake_client_ext" # build bundle of the go files

module Snowflake
  class Result
    attr_reader :query_duration

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
