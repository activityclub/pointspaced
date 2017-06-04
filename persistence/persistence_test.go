package persistence

import "os"
import "github.com/activityclub/pointspaced/psdcontext"
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

	psdcontext.Ctx.AgScript = redis.NewScript(-1, `
local sum = 0
local atids = {}
local aids = {}
for _, packed in ipairs(ARGV) do
        local unpacked = cmsgpack.unpack(packed)
        local res = redis.call('HGETALL', unpacked[1])
        for i, v in ipairs(res) do
                if i % 2 == 0 then
                        local cscore = tonumber(v)
			local idx = 0
                        local tsv = 0
                        local aid = "all"
                        local atid = "all"
                        for word in string.gmatch(res[i-1], '[^:]+') do
		                if idx == 0 then
		                   tsv = tonumber(word)
				elseif idx == 1 then
				   atid = word
				elseif idx == 2 then
				   aid = word
		                end
                                idx = idx + 1
                        end
                        if tsv >= unpacked[2] and tsv <= unpacked[3] then
				if unpacked[4] == "all" or unpacked[4] == atid then
					if unpacked[5] == "all" or unpacked[5] == aid then
	                                	sum = sum + cscore
						if aids[aid] then
						else
						  atids[#atids+1] = tonumber(atid)
						  aids[aid] = true
						end
					end
				end
                        end
                end
        end
end
return {sum, atids}`)

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

func TestHZ(t *testing.T) {
	testMetricRWInterface(t, NewMetricManagerHZ())
}
func TestCountHZ(t *testing.T) {
	testCountRWInterface(t, NewCountManagerHZ())
}

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
	testValidRead(t, mm)
	testOffsets(t, mm)
	testMultiDayValidRead(t, mm)
	testEvenLongerMultiDayValidRead(t, mm)
	testReallyLongValidRead(t, mm)
	testMultiUserLongRead(t, mm)
	testSmallKeyQuery(t, mm)
	testNegativeQuery(t, mm)
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
	opts["atid"] = "10"
	opts["aid"] = "1001"
	opts["value"] = "100"
	opts["ts1"] = "1458061005"
	opts["ts2"] = "1458061005"
	mm.WritePoint(opts)
	opts["value"] = "105"
	opts["ts1"] = "1458061005"
	opts["ts2"] = "1458061006"
	mm.WritePoint(opts)

	sum, _ := mm.QueryBucketsLua("1", "points", "all", "all", 1458061005, 1458061010)
	if sum != 105 {
		t.Logf("Incorrect Sum.  Expected 105, Received %d", sum)
		t.Fail()
	}
}

func testSmallKeyQueryLua(t *testing.T, mm MetricRW) {
	clearRedisCompletely()

	opts := make(map[string]string)
	opts["thing"] = "points"
	opts["uid"] = "1"
	opts["atid"] = "10"
	opts["aid"] = "1001"
	opts["value"] = "100"
	opts["ts1"] = "1458061005"
	opts["ts2"] = "1458061005"
	mm.WritePoint(opts)
	opts["value"] = "105"
	opts["ts1"] = "1458061005"
	opts["ts2"] = "1458061006"
	mm.WritePoint(opts)

	sum, _ := mm.QueryBucketsLua("1", "points", "all", "all", 1458061005, 1458061010)
	if sum != 105 {
		t.Logf("Incorrect Sum.  Expected 105, Received %d", sum)
		t.Fail()
	}
}

func testNegativeQuery(t *testing.T, mm MetricRW) {
	clearRedisCompletely()

	opts := make(map[string]string)
	opts["thing"] = "steps"
	opts["uid"] = "1"
	opts["aid"] = "3"
	opts["atid"] = "3"
	opts["value"] = "100"
	opts["ts1"] = "1458061005"
	opts["ts2"] = "1458061005"
	err := mm.WritePoint(opts)
	if err != nil {
		t.Fail()
	}

	opts = make(map[string]string)
	opts["thing"] = "steps"
	opts["uid"] = "1"
	opts["aid"] = "3"
	opts["atid"] = "3"
	opts["value"] = "-20"
	opts["ts1"] = "1458061007"
	opts["ts2"] = "1458061007"
	err = mm.WritePoint(opts)
	if err != nil {
		t.Fail()
	}

	opts = make(map[string]string)
	opts["thing"] = "steps"
	opts["uid"] = "2"
	opts["aid"] = "3"
	opts["atid"] = "3"
	opts["value"] = "-100"
	opts["ts1"] = "1458061005"
	opts["ts2"] = "1458061005"
	err = mm.WritePoint(opts)
	if err != nil {
		t.Fail()
	}

	sum, _ := mm.QueryBucketsLua("1", "steps", "all", "all", 1458061005, 1458061010)
	if sum != 80 {
		t.Logf("Incorrect Sum.  Expected 80, Received %d", sum)
		t.Fail()
	}
}

func testOffsets(t *testing.T, mm MetricRW) {
	clearRedisCompletely()

	opts := make(map[string]string)
	opts["thing"] = "points"
	opts["uid"] = "1"
	opts["atid"] = "10"
	opts["aid"] = "1001"
	opts["value"] = "100"
	opts["ts1"] = "1471478499"
	opts["ts2"] = "1471478499"
	mm.WritePoint(opts)

	uids := []string{"1"}
	offsets := []string{"28800"}
	things := []string{"points"}
	ts1 := int64(1471392000) // 2016-08-17 00:00:00
	ts2 := int64(1471478399) // 2016-08-17 23:59:59
	res, _ := mm.MultiQueryBucketsWithOffsets(uids, offsets, things, "all", ts1, ts2)
	if res.Data["1"]["points"] != int64(100) {
		fmt.Println("XXX", res.Data)
		t.Logf("Incorrect Points. Expected 100: ", res.Data["1"]["points"])
		t.Fail()
	}
}

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

func testCountRWInterface(t *testing.T, cm CountRW) {
	countTestEvenLongerMultiDayValidRead(t, cm)
}

func countTestEvenLongerMultiDayValidRead(t *testing.T, cm CountRW) {
	clearRedisCompletely()
	/*
		st1 := int64(1461826800)
		st2 := int64(1461914059)
		//requests := cm.requestsForRange(st1, st2)
		cm.ReadCount("new_hifive", st1, st2)
		fmt.Println("xxx")
	*/

	// we will write 10 points
	err := cm.IncrementCount("newsignup", 1451635204, 100)
	if err != nil {
		t.Fail()
	}

	err = cm.IncrementCount("newsignup", 1454313600, 200)
	if err != nil {
		t.Fail()
	}

	err = cm.IncrementCount("newsignup", 1458061005, 10)
	if err != nil {
		t.Fail()
	}

	// add 11 points
	err = cm.IncrementCount("newsignup", 1458061008, 11)
	if err != nil {
		t.Fail()
	}

	// add 1 point
	err = cm.IncrementCount("newsignup", 1458061011, 1)
	if err != nil {
		t.Fail()
	}

	res := cm.ReadCount("newsignup", 1451635200, 1458061011)
	if res != 322 {
		fmt.Println("res", res)
		t.Errorf("Incorrect Sum.  Expected 322, Received %d", res)
	}
	res = cm.ReadCount("newsignup", 1451635205, 1458061010)
	if res != 221 {
		t.Errorf("Incorrect Sum.  Expected 221, Received %d", res)
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
