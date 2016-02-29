#!/bin/env ruby
require 'json'
require 'net/http'

class NSQPublisher
  def self.publish(topic, payload)
					puts "payload like : #{payload}"
     path = "/pub?topic=#{topic}"
     req = Net::HTTP::Post.new(path, initheader = {'Content-Type' =>'application/json'})
     req.body = payload
#     http = Net::HTTP.new("10.18.1.4", 4151)
     http = Net::HTTP.new("127.0.0.1", 4151)
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
   :steps => 11,
   :points => 3,
   :activity_type_id => 3,
   :timestamp => Time.now.to_i,
   :user_id => 1
  }
}

puts NSQPublisher.publish("write_test", payload.to_json).inspect
