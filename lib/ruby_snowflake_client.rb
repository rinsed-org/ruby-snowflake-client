# frozen_string_literal: true

module Snowflake
  require "ruby_snowflake_client_ext" # build bundle of the go files
  LOG_LEVEL = 0

  class Error < StandardError
    # This will get pulled through to Sentry, see:
    # https://github.com/getsentry/sentry-ruby/blob/11ecd254c0d2cae2b327f0348074e849095aa32d/sentry-ruby/lib/sentry/error_event.rb#L31-L33
    attr_reader :sentry_context

    def initialize(details)
      @sentry_context = details
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
        while r = next_row do
          yield r
        end
      else
        get_rows_array
      end
    ensure
      GC.enable
    end

    private
      def get_rows_array
        arr = []
        while r = next_row do
          puts "at #{arr.length}" if arr.length % 15000 == 0 && LOG_LEVEL > 0
          arr << r
        end
        arr
      end
  end
end
