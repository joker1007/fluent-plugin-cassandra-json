# fluent-plugin-cassandra-json

[Fluentd](https://fluentd.org/) output plugin to insert json data to cassandra.

This plugin support complex data type like collection.

## Installation

### RubyGems

```
$ gem install fluent-plugin-cassandra-json
```

### Bundler

Add following line to your Gemfile:

```ruby
gem "fluent-plugin-cassandra-json"
```

And then execute:

```
$ bundle
```

## Plugin helpers

* [inject](https://docs.fluentd.org/v1.0/articles/api-plugin-helper-inject)
* [formatter](https://docs.fluentd.org/v1.0/articles/api-plugin-helper-formatter)

* See also: [Output Plugin Overview](https://docs.fluentd.org/v1.0/articles/output-plugin-overview)

## Configuration

### hosts (array) (required)

The entire list of cluster members for initial lookup

### port (integer) (optional)

Cassandra native protocol port

Default value: `9042`.

### username (string) (optional)

Cluster username

### password (string) (optional)

Cluster password

### cluster_options (hash) (optional)

Other Cluster option parameters

Default value: `{}`.

### consistency (enum) (optional)

Set consistency level

Available values: any, one, two, three, quorum, all, local_quorum, each_quorum, serial, local_serial, local_one

Default value: `one`.

### keyspace (string) (required)

Target keyspace name

### table (string) (required)

Target table name

### if_not_exists (bool) (optional)

Use IF NOT EXIST option on INSERT

### ttl (integer) (optional)

Use TTL option on INSERT

### idempotent (bool) (optional)

Specify whether this statement can be retried safely on timeout

### default_unset (bool) (optional)

Specify whether column not defined in the JSON is set to null or is ignored (If false, column not defined in the JSON is set to null. It is cassandra default)

Default value: `false`.

### skip_invalid_rows (bool) (optional)

Treat request as success, even if invalid rows exist

Default value: `true`.


### \<format\> section (optional) (multiple)

#### @type () (optional)

Default value: `json`.


## Copyright

* Copyright(c) 2018- joker1007
* License
  * Apache License, Version 2.0
