# frozen_string_literal: true

require_relative "lib/ruby_snowflake_client/version"

Gem::Specification.new do |s|
  s.name    = "ruby_snowflake_client"
  s.version = RubySnowflakeClient::VERSION
  s.summary = "Snowflake connector for Ruby"
  s.author  = "Rinsed"
  s.email   = ["alex@rinsed.co"]
  s.description = <<~DESC
  Using the `Go` library for Snowflake to query and creating native Ruby objects,
  using C bindings.
  DESC
  s.license = "MIT" # TODO: double check

  s.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{\A(?:test|spec|features|vendor)/}) }
  end


  s.extensions = %w[ext/extconf.rb]
  s.require_paths = ["lib"]

end
