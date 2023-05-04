# frozen_string_literal: true

#$LOAD_PATH << File.dirname(__FILE__)
#$LOAD_PATH << File.expand_path("../ext", __dir__)# File.dirname(__FILE__) + "/../ext"
#$LOAD_PATH << "/Users/alexstoick/Desktop/go-ruby-snowflake-connector/ext"

require "ffi"
require "benchmark"
require_relative "../ext/ruby_snowflake_client" # built bundle of the go files
