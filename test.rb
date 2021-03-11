# ruby -Ilib test.rb
require 'rubygems'
gem 'activerecord', '= 5.2.4.5'
require 'active_record'
require 'net/http'

class MultiHTTP
  def initialize(hosts)
    @max_retries = 3
    @conns = []
    for host in hosts
      conn = Net::HTTP.new(host, 8123)
      conn.open_timeout = 1
      @conns.append(conn)
    end
  end
  def post(*args)
    #puts(*args)

    retries = @max_retries
    conns = @conns.clone
    last_err = nil

    while retries > 0 && conns.any?
      retries -= 1
      conn = conns.delete(conns.sample)

      begin
        return conn.post(*args)
      rescue Net::OpenTimeout, Errno::ECONNREFUSED => e
        # TODO: Log error for given server
        last_err = e
      end
    end

    raise last_err
  end
end

class Test
  class Base < ActiveRecord::Base
    self.abstract_class = true
    establish_connection(
      adapter: 'clickhouse',
      connection: MultiHTTP.new(['172.17.0.3', '127.17.0.2', '172.17.0.2']),
      #host: '172.17.0.2',
      database: 'default',
    )
  end

  def execute(query)
    Base.connection.execute(query)
  end
end

t = Test.new
while true
  begin
    puts t.execute 'select 1'
  rescue StandardError => e
    puts "sql failed: #{e}"
  end
  sleep 1
end
