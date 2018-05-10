#
# Copyright 2018- joker1007
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "fluent/plugin/output"
require "cassandra"
require "oj"

module Fluent
  module Plugin
    class CassandraJsonOutput < Fluent::Plugin::Output
      Fluent::Plugin.register_output("cassandra_json", self)

      helpers :inject, :formatter

      config_param :hosts, :array,
        desc: "The entire list of cluster members for initial lookup"
      config_param :port, :integer, default: 9042,
        desc: "Cassandra native protocol port"
      config_param :username, :string, default: nil,
        desc: "Cluster username"
      config_param :password, :string, default: nil, secret: true,
        desc: "Cluster password"
      config_param :cluster_options, :hash, default: {},
        desc: "Other Cluster option parameters"

      config_param :consistency, :enum, list: [:any, :one, :two, :three, :quorum, :all, :local_quorum,
:each_quorum, :serial, :local_serial, :local_one], default: :one,
        desc: "Set consistency level"

      config_param :keyspace, :string,
        desc: "Target keyspace name"
      config_param :table, :string,
        desc: "Target table name"

      config_param :if_not_exists, :bool, default: false,
        desc: "Use IF NOT EXIST option on INSERT"
      config_param :ttl, :integer, default: nil,
        desc: "Use TTL option on INSERT"

      config_param :skip_invalid_rows, :bool, default: true,
        desc: "Treat request as success, even if invalid rows exist"

      ## Formatter
      config_section :format do
        config_set_default :@type, 'json'
      end

      def configure(conf)
        super

        if @hosts.empty?
          raise Fluent::ConfigError, "`hosts` has at least one host"
        end

        @cluster_options = @cluster_options.map { |k, v| [k.to_sym, v] }.to_h
        @cluster_options.merge!(hosts: @hosts, port: @port)
        @cluster_options.merge!(username: @username) if @username
        @cluster_options.merge!(password: @password) if @password
        formatter_config = conf.elements("format")[0]
        @formatter = formatter_create(usage: 'out_bigquery_for_insert', type: 'json', conf: formatter_config)
      end

      def start
        super

        @cluster = Cassandra.cluster(@cluster_options)
        @session = @cluster.connect(@keyspace)
      end

      def stop
        super

        @session.close
        @cluster.close
      end

      def format(tag, time, record)
        record = inject_values_to_record(tag, time, record)
        @formatter.format(tag, time, record)
      end

      def multi_workers_ready?
        true
      end

      def try_write(chunk)
        if chunk.empty?
          commit_write(chunk.unique_id)
          return
        end
        keyspace = extract_placeholders(@keyspace, chunk.metadata)
        table = extract_placeholders(@table, chunk.metadata)

        futures = chunk.open do |io|
          io.map do |line|
            cql = "INSERT INTO #{keyspace}.#{table} JSON '#{line}'"
            cql << " IF NOT EXISTS" if @if_not_exists
            cql << " USING TTL #{@ttl}" if @ttl && @ttl > 0
            future = @session.execute_async(cql, consistency: @consistency)
            future.on_failure do |error|
              if @skip_invalid_rows
                @log.warn("failed to insert", record: line, error: error)
              else
                @log.error("failed to insert", record: line, error: error)
              end
            end
          end
        end
        combined = Cassandra::Future.all(futures)

        combined.on_complete do |value, error|
          if error.nil? || @skip_invalid_rows
            commit_write(chunk.unique_id)
          else
            rollback_write(chunk.unique_id)
          end
        end
      end
    end
  end
end
