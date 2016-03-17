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

var randomTimestamps = []int64{}

func TestMain(m *testing.M) {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	// WARNING DO NOT RUN IN PROD!
	psdcontext.Ctx.RedisPool = NewRedisPool(":6379")

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
	testMetricRWInterface(t, NewMetricManagerSimple())
}

func TestHZ(t *testing.T) {
	//testMetricRWInterface(t, NewMetricManagerHZ())
}

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

func testMetricRWInterface(t *testing.T, mm MetricRW) {
	testValidRead(t, mm)
	testMultiDayValidRead(t, mm)
	testEvenLongerMultiDayValidRead(t, mm)
	testReallyLongValidRead(t, mm)
}

func clearRedisCompletely() {
	r := psdcontext.Ctx.RedisPool.Get()
	r.Do("flushall")
	r.Close()
}

func testValidRead(t *testing.T, mm MetricRW) {
	clearRedisCompletely()

	// we will write 10 points
	err := mm.WritePoint("points", 1, 10, 3, 1458061005)
	if err != nil {
		t.Fail()
	}

	// add 11 points
	err = mm.WritePoint("points", 1, 11, 3, 1458061008)
	if err != nil {
		t.Fail()
	}

	// add 1 point
	err = mm.WritePoint("points", 1, 1, 3, 1458061011)
	if err != nil {
		t.Fail()
	}

	// lets try to read all but the last
	res := mm.ReadBuckets([]int64{1}, "points", []int64{3}, 1458061005, 1458061010, "0")
	if res.UserToSum["1"] != 21 {
		t.Logf("Incorrect Sum.  Expected 21, Received %d", res.UserToSum["1"])
		t.Fail()
	}
}

func testReallyLongValidRead(t *testing.T, mm MetricRW) {
	clearRedisCompletely()
	res := mm.ReadBuckets([]int64{1}, "points", []int64{3}, 1268082893, 1458061010, "0")
	if res.UserToSum["1"] != 0 {
		t.Errorf("Incorrect Sum.  Expected 0, Received %d", res.UserToSum["1"])
	}
}

func testMultiDayValidRead(t *testing.T, mm MetricRW) {

	clearRedisCompletely()
	// we will write 10 points

	err := mm.WritePoint("points", 1, 10, 3, 1458061005)
	if err != nil {
		t.Fail()
	}

	// add 11 points
	err = mm.WritePoint("points", 1, 11, 3, 1458061008)
	if err != nil {
		t.Fail()
	}

	// add 1 point
	err = mm.WritePoint("points", 1, 1, 3, 1458061011)
	if err != nil {
		t.Fail()
	}
	res := mm.ReadBuckets([]int64{1}, "points", []int64{3}, 1455481907, 1458061010, "0")
	if res.UserToSum["1"] != 21 {
		t.Errorf("Incorrect Sum.  Expected 21, Received %d", res.UserToSum["1"])
	}
}

func testEvenLongerMultiDayValidRead(t *testing.T, mm MetricRW) {
	clearRedisCompletely()

	// we will write 10 points
	err := mm.WritePoint("points", 1, 100, 3, 1451635204)
	if err != nil {
		t.Fail()
	}

	err = mm.WritePoint("points", 1, 200, 3, 1454313600)
	if err != nil {
		t.Fail()
	}

	err = mm.WritePoint("points", 1, 10, 3, 1458061005)
	if err != nil {
		t.Fail()
	}

	// add 11 points
	err = mm.WritePoint("points", 1, 11, 3, 1458061008)
	if err != nil {
		t.Fail()
	}

	// add 1 point
	err = mm.WritePoint("points", 1, 1, 3, 1458061011)
	if err != nil {
		t.Fail()
	}

	res := mm.ReadBuckets([]int64{1}, "points", []int64{3}, 1451635200, 1458061011, "0")
	if res.UserToSum["1"] != 322 {
		fmt.Println("res", res)
		t.Errorf("Incorrect Sum.  Expected 322, Received %d", res.UserToSum["1"])
	}
	res = mm.ReadBuckets([]int64{1}, "points", []int64{3}, 1451635205, 1458061010, "0")
	if res.UserToSum["1"] != 221 {
		t.Errorf("Incorrect Sum.  Expected 221, Received %d", res.UserToSum["1"])
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
			err := mm.WritePoint("steps", 1, 10, 3, src[iteration])
			if err != nil {
				fmt.Println(err.Error())
				b.Fail()
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
		mm.ReadBuckets([]int64{1}, "steps", []int64{3}, 1458061005, 1458061010, "0")
	}

}

func benchMediumRead(b *testing.B, mm MetricRW) {

	clearRedisCompletely()
	for i := 0; i < b.N; i++ {
		mm.ReadBuckets([]int64{1}, "steps", []int64{3}, 1456262072, 1458162884, "0")
	}

}

func benchLongRead(b *testing.B, mm MetricRW) {

	clearRedisCompletely()
	for i := 0; i < b.N; i++ {
		mm.ReadBuckets([]int64{1}, "steps", []int64{3}, 1268082893, 1458061010, "0")
	}

}
