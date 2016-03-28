## building

to start pointspaced, you gotta do
nsqd -lookupd-tcp-address=127.0.0.1:4160 &
nsqadmin -lookupd-http-address=127.0.0.1:4161 &
nsqlookupd &
./pointspaced worker
redis-server &
then you should be able to do ruby scripts/enqueue.rb


## todo

* finish implementation details around main algo stuff
* think about how api might work for gauges
* think about how api might work for single users, multiple users, and aggregate of all data into 1 type of queries
    * california vs new york
    * people use fitbit vs people that use healthkit
    * results for my list of followers
    * etc etc


* hashzilla
  * concurrency benchmarks / test parallel
  * test with random read keys
  * test insert not linear
  * lua experiment

