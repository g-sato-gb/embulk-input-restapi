require 'faraday'
require 'json'
require 'jsonpath'

module Embulk
  module Input

    class Restapi < InputPlugin
      Plugin.register_input("restapi", self)

      def self.transaction(config, &control)
        task = {
          'base_url' => config.param('base_url', :string, default: nil),
          'path' => config.param('path', :string, default: nil),
          'json_root' => config.param('json_root', :string, default: "$"),
          'method' => config.param('method', :string, default: 'get'),
          'headers' => config.param('headers', :array, default: []),
          'params' => config.param('params', :array, default: []),
          'columns' => config.param('columns', :array, default: []),
        }
        columns = task['columns'].each_with_index.map do |c, i|
          Column.new(i, c["name"], c["type"].to_sym)
        end
        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        task_reports = yield(task, columns, count)

        next_config_diff = {}
        return next_config_diff
      end

      def run
        response = request
        find_by_path(response, @task["json_root"]).each do |order|
          @page_builder.add(make_record(@task["columns"], order))
        end
        @page_builder.finish
        task_report = {}
        return task_report
      end

      def make_record(columns, js_resource)
        columns.map do |column|
          name = column["name"]
          path = column["path"]
          val = path.nil? ? js_resource[name] : find_by_path(js_resource, path)

          v = val.nil? ? "" : val
          type = column["type"]
          case type
            when "string"
              v
            when "long"
              v.to_i
            when "double"
              v.to_f
            when "boolean"
              if v.nil?
                nil
              elsif v.kind_of?(String)
                ["yes", "true", "1"].include?(v.downcase)
              elsif v.kind_of?(Numeric)
                !v.zero?
              else
                !!v
              end
            when "timestamp"
              v.empty? ? nil : Time.iso8601(v)
            else
              raise "Unsupported type #{type}"
          end
        end
      end

      def find_by_path(e, path)
        JsonPath.on(e, path).first
      end

      def request
        conn = Faraday.new(:url => @task[:base_url]) do |builder|
          builder.request  :url_encoded
          builder.adapter  :net_http
        end

        response = conn.get do |req|
          req.url @task[:path]
          req.headers['Content-Type'] = 'application/json'
          @task[:headers].each do |head|
            req.headers[head["name"]] = head["value"]
          end
          @task[:params].each do |param|
            req.params[param["name"]] = param["value"]
          end
        end
        response.body
      end
    end

  end
end
