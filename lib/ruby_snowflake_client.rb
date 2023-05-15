# frozen_string_literal: true

require "ruby_snowflake_client_ext" # build bundle of the go files

module Snowflake
  class Client
    # Wrap the private _connect method, as sending kwargs to Go would require
    # wrapping the function in C as the current CGO has a limitation on
    # accepting variadic arguments for functions.
    def connect(account:"", warehouse:"", database:"", schema: "", user: "", password: "", role: "")
      _connect(account, warehouse, database, schema, user, password, role)
      true
    end
  end


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
