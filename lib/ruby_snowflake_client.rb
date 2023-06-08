# frozen_string_literal: true

module Snowflake
  require "ruby_snowflake_client_ext" # build bundle of the go files

  class Error < StandardError
    attr_reader :details

    def initialize(details)
      @details = details
    end
  end

  class Client
    attr_reader :error
    # Wrap the private _connect method, as sending kwargs to Go would require
    # wrapping the function in C as the current CGO has a limitation on
    # accepting variadic arguments for functions.
    def connect(account:"", warehouse:"", database:"", schema: "", user: "", password: "", role: "")
      @connection_details = {
        account: account,
        warehouse: warehouse,
        database: database,
        schema: schema,
        user: user,
        role: role
      }

      _connect(account.dup, warehouse.dup, database.dup, schema.dup, user.dup, password.dup, role.dup)
      if error != nil
        raise Error.new(@connection_details), error
      end
      true
    end

    def fetch(sql)
      result = _fetch(sql)
      return result if result.valid?
      raise Error.new(@connection_details.merge(sql: sql)), result.error
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
