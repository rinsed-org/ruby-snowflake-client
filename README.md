# `ruby_snowflake_client`

# This is now deprecated in favour of a fully native [Ruby gem](https://github.com/rinsed-org/rb-snowflake-client).

---

The gem provides an interface to allow users to query Snowflake. This wraps around
the [Snowflake Go package](https://github.com/snowflakedb/gosnowflake).

## Why did we create this gem?

Querying Snowflake from Ruby does _not_ currently have an official implementation,
and as a result one is left using the _ODBC adapter_.
However, trying to query large amounts of data from Snowflake using the ODBC adapter
proved to be a very slow operation, and it mangled `Timestamps` quite badly.
On some queries the ODBC would take 15s whereas this gem would average around 3s.
The speed gains are _incremental_ - the more data you fetch the bigger the
difference between the ODBC adapter and this gem.

## Using the gem in your application

To use the gem update your `Gemfile`:

```
gem "ruby_snowflake_client", github: "rinsed-org/ruby-snowflake-client", tag: "v0.0.1", require: false
```

It's important to mark it as `required: false`, otherwise the Rails auto-loading
can create issues with the library defined classes.

**The gem also requires Go to be available on the system where the install is
being done to compile the library.**
To install `Go` on your operating system please follow the instructions on the
[official website](https://go.dev/doc/install).

To use the library:

```ruby
require "ruby_snowflake_client"

client = ::Snowflake::Client.new
client.connect(
  "SNOWFLAKE_ACCOUNT",
  "SNOWFLAKE_WAREHOUSE",
  "SNOWFLAKE_DATABASE",
  "SNOWFLAKE_SCHEMA",
  "SNOWFLAKE_USER",
  "SNOWFLAKE_PASSWORD",
  "SNOWFLAKE_ROLE",
)

result = client.fetch("SELECT 1;")
# => #<Snowflake::Result:0x000000010b1543f0 @query_duration=1.054745708>
result.get_all_rows
# => [{"1"=>"1"}]
```

The gem defines 2 classes:
- `Snowflake::Client` with a `connect` method
- `Snowflake::Result` with 2 methods:
  - `get_all_rows`
  - `get_rows_with_blk(&blk)`

### Caveats

The current implementation pauses the GC before running, and then unpauses it
after it's finished. This was a compromise as the `get_rows` method was consistently
`SEGFAULT` or creating _issues_ for the Ruby GC (marking pointers to nil for
collection resulting in `[BUG] try to mark T_NONE object`).
My **hunch** is that the issue appears due to the fact that
_Go holds references to C objects which point to Ruby objects_, and when the
Ruby GC moves the references the Go/C side will be left behind pointing to `nil`
objects. This fact, compounded by the sheer number of objects that get allocated
as part of the method, created a perfect storm which I could only resolve by
disabling the GC.

It's important to note that the `get_rows_with_blk` method - has a smaller
memory usage footprint as this will only allocate 1 row (hash) at a time rather than
allocate for your entire result set.

## Development

This Gem was build using Go 1.20.4 - to develop this locally it is recommended
to use the same version of Go.

### Updating dependencies
To update the underlying Go packages, you will need to:

```sh

cd ext
go get -u ./...
go mod vendor
```

The above command will update **all** dependencies, if you wish to update only
a particular dependency you can run:
`get pkg-to-update@v1.6.2`

After that you will need to run `go mod vendor` - as we are vendoring our
dependencies.

### Go file structure

The _extension_ that is built is called `ruby_snowflake_client_ext`, and the
entry point for this is the `ruby_snowflake.go` file. This contains the function
`Init_ruby_snowflake_client_ext` which defines the Ruby classes and their methods.

There are some other helper files. It is important to note that due to the nature
of how Go gets compiled into C **you cannot have both `export` and C functions**
declared in the same file. This is why there is a split of some of the helper
files.

### Building the project

To build the project locally, you will need to run:

 ```sh
cd ext
ruby extconf.rb
```

This will create a `Makefile` in the `ext` folder. You can now run `make` from
within the `ext` folder - which will create a `ruby_snowflake_client_ext.bundle`
file. The extension will vary depending on your platform.

You can now run `bundle exec irb` and then `require "ruby_snowflake_client"`,
and you should see a message `"init ruby snowflake client"` in your terminal
alongside the `true` return value.

Remember that if you make changes to any of the Go files you will need to
regenerate the compiled bundled - by re-running `make` in the `ext` folder.

Happy developing!
