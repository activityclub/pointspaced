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

```
	// we will write 10, 11, 1 point
	WritePoint("points", 1, 10, 3, 1458061005) // 2016-03-15 16:56:45
	WritePoint("points", 1, 11, 3, 1458061008) // 2016-03-15 16:56:48
	WritePoint("points", 1, 1, 3, 1458061011)  // 2016-03-15 16:56:51
	// lets try to read all but the last 16:56:45 to 16:56:50
	ReadBuckets([]int64{1}, "points", []int64{3}, 1458061005, 1458061010, "0")
```
```
1460078760.683793 [0 127.0.0.1:49279] "SCRIPT" "LOAD" "local sum = 0\nfor _, packed in ipairs(ARGV) do\n  local unpacked = cmsgpack.unpack(packed)\n  local cscore\n  for i, v in ipairs(redis.call('HGETALL', unpacked[1])) do\n    if i % 2 == 1 then\n    cscore = tonumber(v)\n    else\n      if cscore >= unpacked[2] and cscore <= unpacked[3] then\n        sum = sum + v\n      end\n    end\n  end\nend\nreturn sum"
1460078760.693822 [0 127.0.0.1:49279] "flushall"
1460078760.694720 [0 127.0.0.1:49279] "HINCRBY" "hz:1:0:points:2016" "201603" "10"
1460078760.694837 [0 127.0.0.1:49279] "HINCRBY" "hz:1:3:points:2016" "201603" "10"
1460078760.694957 [0 127.0.0.1:49279] "HINCRBY" "hz:1:0:points:201603" "20160315" "10"
1460078760.695067 [0 127.0.0.1:49279] "HINCRBY" "hz:1:3:points:201603" "20160315" "10"
1460078760.695169 [0 127.0.0.1:49279] "HINCRBY" "hz:1:0:points:20160315" "2016031516" "10"
1460078760.695278 [0 127.0.0.1:49279] "HINCRBY" "hz:1:3:points:20160315" "2016031516" "10"
1460078760.695378 [0 127.0.0.1:49279] "HINCRBY" "hz:1:0:points:2016031516" "201603151656" "10"
1460078760.695493 [0 127.0.0.1:49279] "HINCRBY" "hz:1:3:points:2016031516" "201603151656" "10"
1460078760.695662 [0 127.0.0.1:49279] "HINCRBY" "hz:1:0:points:201603151656" "20160315165645" "10"
1460078760.695783 [0 127.0.0.1:49279] "HINCRBY" "hz:1:3:points:201603151656" "20160315165645" "10"
1460078760.695899 [0 127.0.0.1:49279] "HINCRBY" "hz:1:0:points:2016" "201603" "11"
1460078760.696010 [0 127.0.0.1:49279] "HINCRBY" "hz:1:3:points:2016" "201603" "11"
1460078760.696134 [0 127.0.0.1:49279] "HINCRBY" "hz:1:0:points:201603" "20160315" "11"
1460078760.696256 [0 127.0.0.1:49279] "HINCRBY" "hz:1:3:points:201603" "20160315" "11"
1460078760.696373 [0 127.0.0.1:49279] "HINCRBY" "hz:1:0:points:20160315" "2016031516" "11"
1460078760.696461 [0 127.0.0.1:49279] "HINCRBY" "hz:1:3:points:20160315" "2016031516" "11"
1460078760.696574 [0 127.0.0.1:49279] "HINCRBY" "hz:1:0:points:2016031516" "201603151656" "11"
1460078760.696682 [0 127.0.0.1:49279] "HINCRBY" "hz:1:3:points:2016031516" "201603151656" "11"
1460078760.696826 [0 127.0.0.1:49279] "HINCRBY" "hz:1:0:points:201603151656" "20160315165648" "11"
1460078760.696931 [0 127.0.0.1:49279] "HINCRBY" "hz:1:3:points:201603151656" "20160315165648" "11"
1460078760.697043 [0 127.0.0.1:49279] "HINCRBY" "hz:1:0:points:2016" "201603" "1"
1460078760.697138 [0 127.0.0.1:49279] "HINCRBY" "hz:1:3:points:2016" "201603" "1"
1460078760.697283 [0 127.0.0.1:49279] "HINCRBY" "hz:1:0:points:201603" "20160315" "1"
1460078760.697401 [0 127.0.0.1:49279] "HINCRBY" "hz:1:3:points:201603" "20160315" "1"
1460078760.697545 [0 127.0.0.1:49279] "HINCRBY" "hz:1:0:points:20160315" "2016031516" "1"
1460078760.697682 [0 127.0.0.1:49279] "HINCRBY" "hz:1:3:points:20160315" "2016031516" "1"
1460078760.697783 [0 127.0.0.1:49279] "HINCRBY" "hz:1:0:points:2016031516" "201603151656" "1"
1460078760.697918 [0 127.0.0.1:49279] "HINCRBY" "hz:1:3:points:2016031516" "201603151656" "1"
1460078760.698018 [0 127.0.0.1:49279] "HINCRBY" "hz:1:0:points:201603151656" "20160315165651" "1"
1460078760.698137 [0 127.0.0.1:49279] "HINCRBY" "hz:1:3:points:201603151656" "20160315165651" "1"
1460078760.698529 [0 127.0.0.1:49279] "EVALSHA" "55da2d225f3d9725e7a6ab64359b466ba7d721b2" "0" "\x93\xbahz:1:0:points:201603151656\xcf\x00\x00\x12U\xf0l\x8b\xcd\xcf\x00\x00\x12U\xf0l\x8b\xd2"
1460078760.698570 [0 lua] "HGETALL" "hz:1:0:points:201603151656"
```
