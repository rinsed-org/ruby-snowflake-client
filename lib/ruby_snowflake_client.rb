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

    def get_all_rows(&blk)
      GC.disable
      if blk
        _get_rows(&blk)
      else
        _get_rows.to_a
      end
    ensure
      GC.enable
      GC.start
    end
  end
end
