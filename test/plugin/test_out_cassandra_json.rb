require "helper"
require "fluent/plugin/out_cassandra_json.rb"

class CassandraJsonOutputTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
    @cluster = Cassandra.cluster(hosts: [ENV.fetch("CASSANDRA_HOST", "127.0.0.1")])
    @session = @cluster.connect
    @session.execute("CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH REPLICATION = { 'class' :'SimpleStrategy', 'replication_factor': 1 }")
    @session.execute("DROP TABLE IF EXISTS test_keyspace.test_table")
    @session.execute("CREATE TABLE test_keyspace.test_table (id bigint, col1 text, col2 timestamp, col3 boolean, col4 double, col5 list<int>, col6 set<text>, col7 map<text, text>, PRIMARY KEY (id))")
  end

  CONFIG = %[
    hosts #{ENV.fetch("CASSANDRA_HOST", "127.0.0.1")}
    port #{ENV.fetch("CASSANDRA_PORT", "9042")}
    keyspace test_keyspace
    table test_table
  ]

  test "write data" do
    driver = create_driver
    time = Time.local(2018, 5, 8, 14, 10, 5)
    data = {
      "id" => 1,
      "col1" => "textdata",
      "col2" => time,
      "col3" => true,
      "col4" => 1.23,
      "col5" => [1, 2, 3],
      "col6" => ["one", "two", "three"],
      "col7" => {"key1" => "val1", "key2" => "val2"},
    }
    invalid_data = data.merge("id" => "invalid")
    driver.run do
      driver.feed("tag", Time.now.to_i, data)
      driver.feed("tag", Time.now.to_i, data)
      driver.feed("tag", Time.now.to_i, invalid_data)
    end
    result = @session.execute("SELECT * FROM test_keyspace.test_table")
    assert { result.size == 1 }

    first = result.each.to_a[0]
    expected = data.dup
    expected["col6"] = Set.new(data["col6"])
    assert { first["id"] == expected["id"] }
    (1..7).each do |i|
      assert { first["col#{i}"] == expected["col#{i}"] }
    end
  end

  test "quoted data" do
    driver = create_driver
    time = Time.local(2018, 5, 8, 14, 10, 5)
    data = {
      "id" => 1,
      "col1" => "text'data",
      "col2" => time,
      "col3" => true,
      "col4" => 1.23,
      "col5" => [1, 2, 3],
      "col6" => ["one", "t\"wo", "th'r''ee"],
      "col7" => {"key1" => "val1", "key2" => "val2"},
    }
    invalid_data = data.merge("id" => "invalid")
    driver.run do
      driver.feed("tag", Time.now.to_i, data)
      driver.feed("tag", Time.now.to_i, data)
      driver.feed("tag", Time.now.to_i, invalid_data)
    end
    result = @session.execute("SELECT * FROM test_keyspace.test_table")
    assert { result.size == 1 }

    first = result.each.to_a[0]
    data["col6"] = Set.new(data["col6"])
    assert { first["id"] == data["id"] }
    (1..7).each do |i|
      assert { first["col#{i}"] == data["col#{i}"] }
    end
  end

  test "write data (if not exist)" do
    conf = %[
      hosts #{ENV.fetch("CASSANDRA_HOST", "127.0.0.1")}
      port #{ENV.fetch("CASSANDRA_PORT", "9042")}
      keyspace test_keyspace
      table test_table
      if_not_exists true
    ]
    driver = create_driver(conf)
    time = Time.local(2018, 5, 8, 14, 10, 5)
    data = {
      "id" => 1,
      "col1" => "textdata",
      "col2" => time,
      "col3" => true,
      "col4" => 1.23,
      "col5" => [1, 2, 3],
      "col6" => ["one", "two", "three"],
      "col7" => {"key1" => "val1", "key2" => "val2"},
    }
    driver.run do
      driver.feed("tag", Time.now.to_i, data)
    end
    result = @session.execute("SELECT * FROM test_keyspace.test_table")
    assert { result.size == 1 }

    first = result.each.to_a[0]
    data["col6"] = Set.new(data["col6"])
    assert { first["id"] == data["id"] }
    (1..7).each do |i|
      assert { first["col#{i}"] == data["col#{i}"] }
    end
  end

  test "write data (ttl)" do
    conf = %[
      hosts #{ENV.fetch("CASSANDRA_HOST", "127.0.0.1")}
      port #{ENV.fetch("CASSANDRA_PORT", "9042")}
      keyspace test_keyspace
      table test_table
      ttl 30
    ]
    driver = create_driver(conf)
    time = Time.local(2018, 5, 8, 14, 10, 5)
    data = {
      "id" => 1,
      "col1" => "textdata",
      "col2" => time,
      "col3" => true,
      "col4" => 1.23,
      "col5" => [1, 2, 3],
      "col6" => ["one", "two", "three"],
      "col7" => {"key1" => "val1", "key2" => "val2"},
    }
    driver.run do
      driver.feed("tag", Time.now.to_i, data)
    end
    result = @session.execute("SELECT * FROM test_keyspace.test_table")
    assert { result.size == 1 }

    first = result.each.to_a[0]
    data["col6"] = Set.new(data["col6"])
    assert { first["id"] == data["id"] }
    (1..7).each do |i|
      assert { first["col#{i}"] == data["col#{i}"] }
    end
  end

  private

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::CassandraJsonOutput).configure(conf)
  end
end
