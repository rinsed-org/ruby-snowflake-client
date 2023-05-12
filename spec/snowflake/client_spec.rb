require "spec_helper"

RSpec.describe Snowflake::Client do
  subject(:client) { described_class.new }

  describe "#connect" do
    context "when the account is empty" do
      it "will return an error" do
        expect {
          client.connect("","","","","","","")
        }.to raise_error(ArgumentError, "Snowflake Config Creation Error: '260000: account is empty'")
      end
    end

    context "when all values are valid" do
      it "will not raise an error" do
        expect(
          client.connect("acc","warehouse","database","schema","user","pwd","role")
        ).to eq(false)
      end
    end

    context "with values from env" do
      it "will not raise an error" do
        expect(
          client.connect(
            ENV["SNOWFLAKE_ACCOUNT"],
            ENV["SNOWFLAKE_WAREHOUSE"],
            "",
            "",
            ENV["SNOWFLAKE_USER"],
            ENV["SNOWFLAKE_PASSWORD"],
            ""
          )
        ).to eq(false)
      end
    end
  end

  describe "#fetch" do
    before do
      client.connect(
        ENV["SNOWFLAKE_ACCOUNT"],
        ENV["SNOWFLAKE_WAREHOUSE"],
        #"ruby_snowflake_client_testing",
        "",
        "",
        ENV["SNOWFLAKE_USER"],
        ENV["SNOWFLAKE_PASSWORD"],
        ""
      )
    end

    let(:query) { "" }
    let(:result) { client.fetch(query) }

    context "with a simple query returning string" do
      let(:query) { "SELECT 1;" }
      it "should return a Snowflake::Result" do
        expect(result).to be_a(Snowflake::Result)
      end

      it "should respond to get_all_rows" do
        rows = result.get_all_rows
        expect(rows.length).to eq(1)
        expect(rows).to eq(
          [{"1" => "1"}]
        )
      end

      it "should respond to get_rows_with_blk" do
        expect { |b| result.get_rows_with_blk(&b) }.to yield_with_args({"1" => "1"})
      end
    end

    context "with a more complex query" do
      # We have setup a simple table in our Snowflake account with the below structure:
      # CREATE TABLE ruby_snowflake_client_testing.public.test_datatypes
      #   (ID int, NAME string, DOB date, CREATED_AT timestamp, COFFES_PER_WEEK float);
      # And inserted some test data:
      # INSERT INTO test_datatypes
      #    VALUES (1, 'John Smith', '1990-10-17', current_timestamp(), 3.41),
      #    (2, 'Jane Smith', '1990-01-09', current_timestamp(), 3.525);
      let(:query) { "SELECT * from ruby_snowflake_client_testing.public.test_datatypes;" }
      let(:expected_john) do
        {
          "coffes_per_week" => 3.41,
          "id" => "1", # notice how int goes to string!
          "dob" => be_within(0.01).of(Time.new(1990, 10, 17,0,0,0, 0)),
          "created_at" => be_within(0.01).of(Time.new(2023,5,12,4,22,8,0)),
          "name" => "John Smith",
        }
      end
      let(:expected_jane) do
        {
          "coffes_per_week" => 3.525,
          "id" => "2", # notice how int goes to string!
          "dob" => be_within(0.01).of(Time.new(1990,1,9,0,0,0, 0)),
          "created_at" => be_within(0.01).of(Time.new(2023,5,12,4,22,8,0)),
          "name" => "Jane Smith",
        }
      end

      it "should return 2 rows with the right data types" do
        rows = result.get_all_rows
        require "date"
        expect(rows.length).to eq(2)
        john = rows[0]
        jane = rows[1]
        expect(john).to match(expected_john)
        expect(jane).to match(expected_jane)
      end
    end

    context "with a large amount of data" do
      # We have setup a very simple table with the below statement:
      # CREATE TABLE ruby_snowflake_client_testing.public.large_table (ID int PRIMARY KEY, random_text string);
      # We than ran a couple of inserts with large number of rows:
      # INSERT INTO ruby_snowflake_client_testing.public.large_table
      #   SELECT random()%50000, randstr(64, random()) FROM table(generator(rowCount => 50000));

      let(:limit) { 0 }
      let(:query) { "SELECT * FROM ruby_snowflake_client_testing.public.large_table LIMIT #{limit}" }

      context "fetching 50k rows" do
        let(:limit) { 50_000 }
        it "should work" do
          rows = result.get_all_rows
          expect(rows.length).to eq 50000
          expect((-50000...50000)).to include(rows[0]["id"].to_i)
        end
      end

      context "fetching 150k rows" do
        let(:limit) { 150_000 }
        it "should work" do
          rows = result.get_all_rows
          expect(rows.length).to eq 150000
          expect((-50000...50000)).to include(rows[0]["id"].to_i)
        end
      end
    end

  end
end
