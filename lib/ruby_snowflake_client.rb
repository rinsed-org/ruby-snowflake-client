# frozen_string_literal: true

module Snowflake
  #require "ruby_snowflake_client_ext" # build bundle of the go files
  require_relative "../ext/ruby_snowflake_client_ext" # build bundle of the go files

  class Client
    attr_reader :error
    # Wrap the private _connect method, as sending kwargs to Go would require
    # wrapping the function in C as the current CGO has a limitation on
    # accepting variadic arguments for functions.
    def connect(account:"", warehouse:"", database:"", schema: "", user: "", password: "", role: "")
      _connect(account, warehouse, database, schema, user, password, role)
      if error != nil
        raise(error)
      end
      true
    end

    def fetch(sql)
      result = _fetch(sql)
      return result if result.valid?
      raise(result.error)
    end
  end


  class Result
    attr_reader :query_duration, :error

    def valid?
      error == nil
    end

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
