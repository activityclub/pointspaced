package persistence

import "os"
import "pointspaced/psdcontext"
import "testing"
import "time"
import "fmt"
import "math/rand"
import _ "net/http/pprof"
import "log"
import "net/http"
import "github.com/garyburd/redigo/redis"

var randomTimestamps = []int64{}

func TestMain(m *testing.M) {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	// WARNING DO NOT RUN IN PROD!
	psdcontext.PrepareContext("../conf/settings.toml")
	psdcontext.Ctx.RedisPool = NewRedisPool(":6379")

	/* load scripts */

	rx := psdcontext.Ctx.RedisPool.Get()

	psdcontext.Ctx.AgScript = redis.NewScript(-1, `local sum = 0
for _, packed in ipairs(ARGV) do
  local unpacked = cmsgpack.unpack(packed)
  local cscore
  for i, v in ipairs(redis.call('HGETALL', unpacked[1])) do
    if i % 2 == 1 then
    cscore = tonumber(v)
    else
      if cscore >= unpacked[2] and cscore <= unpacked[3] then
        sum = sum + v
      end
    end
  end
end
return sum`)

	/*
	   	psdcontext.Ctx.AgScript = redis.NewScript(-1, `local sum = 0
	   local pos = 1
	   for _, key in ipairs(KEYS) do
	     local bulk = redis.call('HGETALL', key)
	     local result = {}
	     local cscore
	     local offset_a = pos
	     local offset_b = pos+1
	     for i, v in ipairs(bulk) do
	       if i % 2 == 1 then
	         cscore = v
	       else
	         if cscore >= ARGV[offset_a] and cscore <= ARGV[offset_b] then
	           sum = sum + v
	         end
	       end
	     end
	     pos = pos +  2
	   end
	   return sum`)
	*/

	err := psdcontext.Ctx.AgScript.Load(rx)
	if err != nil {
		panic(err)
	}
	rx.Close()

	// generate a crapload of random numbers
	fmt.Println("[Prep] preparing random numbers")
	rand.Seed(time.Now().UTC().UnixNano())
	for {
		min := 1426623393
		max := 1489695427
		n := randInt(min, max)
		randomTimestamps = append(randomTimestamps, int64(n))
		if len(randomTimestamps) >= 100000 {
			break
		}
	}
	fmt.Println("[Prep] done preparing random numbers")

	os.Exit(m.Run())
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func TestACR(t *testing.T) {
	//testMetricRWInterface(t, NewMetricManagerACR())
}

func TestSimple(t *testing.T) {
	//testMetricRWInterface(t, NewMetricManagerSimple())
}

func TestHZ(t *testing.T) {
	testMetricRWInterface(t, NewMetricManagerHZ())
}

/*
func BenchmarkSimple_WriteOneHundred(b *testing.B) {
	benchmarkWriteN(b, NewMetricManagerSimple(), 100)
}

func BenchmarkSimple_WriteOneThousand(b *testing.B) {
	benchmarkWriteN(b, NewMetricManagerSimple(), 1000)
}

func BenchmarkSimple_WriteTenThousand(b *testing.B) {
	benchmarkWriteN(b, NewMetricManagerSimple(), 10000)
}

func BenchmarkSimple_ShortRead(b *testing.B) {
	benchShortRead(b, NewMetricManagerSimple())
}

func BenchmarkSimple_MediumRead(b *testing.B) {
	benchMediumRead(b, NewMetricManagerSimple())
}

func BenchmarkSimple_LongRead(b *testing.B) {
	benchLongRead(b, NewMetricManagerSimple())
}

func BenchmarkSimple_MultiUserRead(b *testing.B) {
	benchMultiUserLongRead(b, NewMetricManagerSimple())
}

func BenchmarkSimple_ManyMultiUserRead(b *testing.B) {
	benchManyMultiUserLongRead(b, NewMetricManagerSimple())
}

// --------------

func BenchmarkACR_WriteOneHundred(b *testing.B) {
	benchmarkWriteN(b, NewMetricManagerACR(), 100)
}

func BenchmarkACR_WriteOneThousand(b *testing.B) {
	benchmarkWriteN(b, NewMetricManagerACR(), 1000)
}

func BenchmarkACR_WriteTenThousand(b *testing.B) {
	benchmarkWriteN(b, NewMetricManagerACR(), 10000)
}

func BenchmarkACR_ShortRead(b *testing.B) {
	benchShortRead(b, NewMetricManagerACR())
}

func BenchmarkACR_MediumRead(b *testing.B) {
	benchMediumRead(b, NewMetricManagerACR())
}

func BenchmarkACR_LongRead(b *testing.B) {
	benchLongRead(b, NewMetricManagerACR())
}

func BenchmarkACR_MultiUserRead(b *testing.B) {
	benchMultiUserLongRead(b, NewMetricManagerACR())
}

func BenchmarkACR_ManyMultiUserRead(b *testing.B) {
	benchManyMultiUserLongRead(b, NewMetricManagerACR())
}
*/

// ------
func BenchmarkHZ_WriteOneHundred(b *testing.B) {
	benchmarkWriteN(b, NewMetricManagerHZ(), 100)
}

func BenchmarkHZ_WriteOneThousand(b *testing.B) {
	benchmarkWriteN(b, NewMetricManagerHZ(), 1000)
}

func BenchmarkHZ_WriteTenThousand(b *testing.B) {
	benchmarkWriteN(b, NewMetricManagerHZ(), 10000)
}

func BenchmarkHZ_ShortRead(b *testing.B) {
	benchShortRead(b, NewMetricManagerHZ())
}

func BenchmarkHZ_MediumRead(b *testing.B) {
	benchMediumRead(b, NewMetricManagerHZ())
}

func BenchmarkHZ_LongRead(b *testing.B) {
	benchLongRead(b, NewMetricManagerHZ())
}

func BenchmarkHZ_MultiUserRead(b *testing.B) {
	benchMultiUserLongRead(b, NewMetricManagerHZ())
}

func BenchmarkHZ_ManyMultiUserRead(b *testing.B) {
	benchManyMultiUserLongRead(b, NewMetricManagerHZ())
}

func testMetricRWInterface(t *testing.T, mm MetricRW) {
	//testValidRead(t, mm)
	//testMultiDayValidRead(t, mm)
	//testEvenLongerMultiDayValidRead(t, mm)
	//testReallyLongValidRead(t, mm)
	//testMultiUserLongRead(t, mm)
	testSmallKeyQuery(t, mm)
	//testNegativeQuery(t, mm)
}

func clearRedisCompletely() {
	r := psdcontext.Ctx.RedisPool.Get()
	r.Do("flushall")
	r.Close()
}

func writeSpecificThings(offset int, t *testing.T, mm MetricRW) {
}

func testSmallKeyQuery(t *testing.T, mm MetricRW) {
	clearRedisCompletely()

	opts := make(map[string]string)
	opts["thing"] = "points"
	opts["uid"] = "1"
	opts["aid"] = "10"
	opts["value"] = "100"
	opts["ts"] = "1458061005"
	mm.WritePoint(opts)
	opts["value"] = "200"
	opts["aid"] = "11"
	opts["ts"] = "1458061006"
	mm.WritePoint(opts)
	opts["thing"] = "steps"
	opts["value"] = "200"
	opts["aid"] = "3"
	opts["ts"] = "1458061007"
	mm.WritePoint(opts)

	sum := mm.QueryBuckets("1", "points", "all", 1458061005, 1458061010)
	if sum != 300 {
		t.Logf("Incorrect Sum.  Expected 300, Received %d", sum)
		t.Fail()
	}
}

/*
func testNegativeQuery(t *testing.T, mm MetricRW) {
	clearRedisCompletely()

	opts := make(map[string]string)
	opts["thing"] = "steps"
	opts["uid"] = "1"
	opts["aid"] = "3"
	opts["value"] = "100"
	opts["ts"] = "1458061005"
	opts["tz"] = "-28800"
	opts["sid"] = "1"
	opts["did"] = "1"
	opts["gid"] = "1"
	err := mm.WritePoint(opts)
	if err != nil {
		t.Fail()
	}

	opts = make(map[string]string)
	opts["thing"] = "steps"
	opts["uid"] = "1"
	opts["aid"] = "3"
	opts["value"] = "-20"
	opts["ts"] = "1458061007"
	err = mm.WritePoint(opts)
	if err != nil {
		t.Fail()
	}

	opts = make(map[string]string)
	opts["thing"] = "steps"
	opts["uid"] = "2"
	opts["gid"] = "2"
	opts["value"] = "-100"
	opts["ts"] = "1458061005"
	err = mm.WritePoint(opts)
	if err != nil {
		t.Fail()
	}

	qopts := make(map[string][]string)
	qopts["uids"] = []string{"1", "2"}
	res := mm.QueryBuckets("steps", "uids", qopts, 1458061005, 1458061010)
	if res.XToSum["1"] != 80 {
		t.Logf("Incorrect Sum.  Expected 80, Received %d", res.XToSum["1"])
		t.Fail()
	}
	if res.XToSum["2"] != -100 {
		t.Logf("Incorrect Sum.  Expected -100, Received %d", res.XToSum["2"])
		t.Fail()
	}
}
*/

func testValidRead(t *testing.T, mm MetricRW) {
	clearRedisCompletely()

	// we will write 10 points
	err := mm.OldWritePoint("points", 1, 10, 3, 1458061005)
	if err != nil {
		t.Fail()
	}

	// add 11 points
	err = mm.OldWritePoint("points", 1, 11, 3, 1458061008)
	if err != nil {
		t.Fail()
	}

	// add 1 point
	err = mm.OldWritePoint("points", 1, 1, 3, 1458061011)
	if err != nil {
		t.Fail()
	}

	// lets try to read all but the last
	res := mm.ReadBuckets([]int64{1}, "points", []int64{3}, 1458061005, 1458061010)
	if res.UserToSum["1"] != 21 {
		fmt.Println("XXX", res.UserToSum)
		t.Logf("Incorrect Sum.  Expected 21, Received %d", res.UserToSum["1"])
		t.Fail()
	}
}

func testReallyLongValidRead(t *testing.T, mm MetricRW) {
	clearRedisCompletely()
	res := mm.ReadBuckets([]int64{1}, "points", []int64{3}, 1268082893, 1458061010)
	if res.UserToSum["1"] != 0 {
		t.Errorf("Incorrect Sum.  Expected 0, Received %d", res.UserToSum["1"])
	}
}

func testMultiDayValidRead(t *testing.T, mm MetricRW) {

	clearRedisCompletely()
	// we will write 10 points

	err := mm.OldWritePoint("points", 1, 10, 3, 1458061005)
	if err != nil {
		t.Fail()
	}

	// add 11 points
	err = mm.OldWritePoint("points", 1, 11, 3, 1458061008)
	if err != nil {
		t.Fail()
	}

	// add 1 point
	err = mm.OldWritePoint("points", 1, 1, 3, 1458061011)
	if err != nil {
		t.Fail()
	}
	res := mm.ReadBuckets([]int64{1}, "points", []int64{3}, 1455481907, 1458061010)
	if res.UserToSum["1"] != 21 {
		t.Errorf("Incorrect Sum.  Expected 21, Received %d", res.UserToSum["1"])
	}
}

func testEvenLongerMultiDayValidRead(t *testing.T, mm MetricRW) {
	clearRedisCompletely()

	// we will write 10 points
	err := mm.OldWritePoint("points", 1, 100, 3, 1451635204)
	if err != nil {
		t.Fail()
	}

	err = mm.OldWritePoint("points", 1, 200, 3, 1454313600)
	if err != nil {
		t.Fail()
	}

	err = mm.OldWritePoint("points", 1, 10, 3, 1458061005)
	if err != nil {
		t.Fail()
	}

	// add 11 points
	err = mm.OldWritePoint("points", 1, 11, 3, 1458061008)
	if err != nil {
		t.Fail()
	}

	// add 1 point
	err = mm.OldWritePoint("points", 1, 1, 3, 1458061011)
	if err != nil {
		t.Fail()
	}

	res := mm.ReadBuckets([]int64{1}, "points", []int64{3}, 1451635200, 1458061011)
	if res.UserToSum["1"] != 322 {
		fmt.Println("res", res)
		t.Errorf("Incorrect Sum.  Expected 322, Received %d", res.UserToSum["1"])
	}
	res = mm.ReadBuckets([]int64{1}, "points", []int64{3}, 1451635205, 1458061010)
	if res.UserToSum["1"] != 221 {
		t.Errorf("Incorrect Sum.  Expected 221, Received %d", res.UserToSum["1"])
	}
}

func testMultiUserLongRead(t *testing.T, mm MetricRW) {
	clearRedisCompletely()

	for _, uid := range []int64{1, 2, 3, 327} {

		err := mm.OldWritePoint("points", uid, 1000, 3, 1451635204)
		if err != nil {
			t.Fail()
		}

		err = mm.OldWritePoint("points", uid, 200, 3, 1454313600)
		if err != nil {
			t.Fail()
		}

		err = mm.OldWritePoint("points", uid, 2802, 3, 1458061005)
		if err != nil {
			t.Fail()
		}

		err = mm.OldWritePoint("points", uid, 11, 3, 1458061008)
		if err != nil {
			t.Fail()
		}

		err = mm.OldWritePoint("points", uid, 1, 3, 1458061011)
		if err != nil {
			t.Fail()
		}

		err = mm.OldWritePoint("points", uid, -56, 3, 1458061011)
		if err != nil {
			t.Fail()
		}
	}

	res := mm.ReadBuckets([]int64{1, 2, 3, 327}, "points", []int64{3}, 1451635200, 1458061011)
	for _, uid := range []int64{1, 2, 3, 327} {
		s := fmt.Sprintf("%d", uid)
		if res.UserToSum[s] != 3958 {
			t.Errorf("Incorrect Sum For Uid %d.  Expected 3958, Received %d", uid, res.UserToSum[s])
		}
	}
	for _, uid := range []int64{1, 2, 3, 327} {
		res = mm.ReadBuckets([]int64{1, 2, 3, 327}, "points", []int64{3}, 1451635205, 1458061010)
		s := fmt.Sprintf("%d", uid)
		if res.UserToSum[s] != 3013 {
			t.Errorf("Incorrect Sum.  Expected 3013, Received %d", res.UserToSum[s])
		}
	}
}

func shuffleTsArray() []int64 {
	dest := make([]int64, len(randomTimestamps))
	perm := rand.Perm(len(randomTimestamps))
	for i, v := range perm {
		dest[v] = randomTimestamps[i]
	}
	return dest
}

func benchmarkWriteN(b *testing.B, mm MetricRW, amnt int) {

	for i := 0; i < b.N; i++ {

		src := shuffleTsArray()
		clearRedisCompletely()

		iteration := 0
		for {
			for _, uid := range []int64{1, 2, 3, 327, 4, 22, 77, 24, 99, 1929, 2854, 412, 413, 728, 729, 828, 17000, 17001, 17002, 17003, 17004, 17005, 17006, 17007, 17008, 17009, 17010, 17011,
				17012, 17013, 17014, 17015, 17016, 17017, 17018, 17019, 17020, 17021, 17022, 17023, 17024, 17025, 17026, 17027, 17028, 17029, 17030, 17031, 17032, 17033, 17034, 17035, 17036, 17037, 17038, 17039, 17040} {
				err := mm.OldWritePoint("steps", uid, 10, 3, src[iteration])
				if err != nil {
					fmt.Println(err.Error())
					b.Fail()
				}
			}

			iteration += 1
			if iteration >= amnt {
				break
			}
		}
	}
}

func benchShortRead(b *testing.B, mm MetricRW) {

	clearRedisCompletely()
	for i := 0; i < b.N; i++ {
		mm.ReadBuckets([]int64{1}, "steps", []int64{3}, 1458061005, 1458061010)
	}

}

func benchMediumRead(b *testing.B, mm MetricRW) {

	clearRedisCompletely()
	for i := 0; i < b.N; i++ {
		mm.ReadBuckets([]int64{1}, "steps", []int64{3}, 1456262072, 1458162884)
	}

}

func benchLongRead(b *testing.B, mm MetricRW) {

	clearRedisCompletely()
	for i := 0; i < b.N; i++ {
		mm.ReadBuckets([]int64{1}, "steps", []int64{3}, 1268082893, 1458061010)
	}

}

func benchMultiUserLongRead(b *testing.B, mm MetricRW) {

	clearRedisCompletely()
	for i := 0; i < b.N; i++ {
		mm.ReadBuckets([]int64{1}, "steps", []int64{1, 2, 3, 327, 4, 22, 77, 24, 99, 1929, 2854, 412}, 1268082893, 1458061010)
	}

}

func benchManyMultiUserLongRead(b *testing.B, mm MetricRW) {

	clearRedisCompletely()
	for i := 0; i < b.N; i++ {
		mm.ReadBuckets([]int64{1}, "steps", []int64{1, 2, 3, 327, 4, 22, 77, 24, 99, 1929, 2854, 412, 413, 728, 729, 828, 17000, 17001, 17002, 17003, 17004, 17005, 17006, 17007, 17008, 17009, 17010, 17011,
			17012, 17013, 17014, 17015, 17016, 17017, 17018, 17019, 17020, 17021, 17022, 17023, 17024, 17025, 17026, 17027, 17028, 17029, 17030, 17031, 17032, 17033, 17034, 17035, 17036, 17037, 17038, 17039, 17040,
		}, 1268082893, 1458061010)
	}

}
