#!/bin/env ruby
require 'json'
require 'net/http'

class NSQPublisher
  def self.publish(topic, payload)
					puts "payload like : #{payload}"
     path = "/pub?topic=#{topic}"
     req = Net::HTTP::Post.new(path, initheader = {'Content-Type' =>'application/json'})
     req.body = payload
     http = Net::HTTP.new("10.18.1.4", 4151)
     http.read_timeout = 5
     http.open_timeout = 5
     http.start { |http|
         http.request(req)
     }
  end
end

payload = {
  :command => "metric",
  :metric => {
   :steps => 10,
   :activity_type_id => 3,
   :start_ts => Time.now.to_i,
   :end_ts => Time.now.to_i
  }
}

puts NSQPublisher.publish("write_test", payload.to_json).inspect
