to start pointspaced, you gotta do
nsqd -lookupd-tcp-address=127.0.0.1:4160 &
nsqadmin -lookupd-http-address=127.0.0.1:4161 &
nsqlookupd &
./pointspaced worker
redis-server &
then you should be able to do ruby scripts/enqueue.rb

