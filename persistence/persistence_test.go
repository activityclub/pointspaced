package persistence

import "testing"
import "time"
import "pointspaced/psdcontext"
import "os"

func TestMain(m *testing.M) {
	// WARNING DO NOT RUN IN PROD!
	psdcontext.Ctx.RedisPool = NewRedisPool(":6379")
	os.Exit(m.Run())
}

/*
func TestValidWriteMZ(t *testing.T) {
	mm := NewMetricManagerMZ()
	err := mm.MetricRW.WritePoint("steps", 1, 10, 3, time.Now().Unix())
	if err != nil {
		t.Fail()
	}
}
*/

func TestValidReadMZ(t *testing.T) {
	mm := NewMetricManagerMZ()

	r := psdcontext.Ctx.RedisPool.Get()
	r.Do("flushall")
	r.Close()

	// we will write 10 points
	err := mm.MetricRW.WritePoint("points", 1, 10, 3, 1458061005)
	if err != nil {
		t.Fail()
	}

	// add 11 points
	err = mm.MetricRW.WritePoint("points", 1, 11, 3, 1458061008)
	if err != nil {
		t.Fail()
	}

	// add 1 point
	err = mm.MetricRW.WritePoint("points", 1, 1, 3, 1458061011)
	if err != nil {
		t.Fail()
	}

	// lets try to read all but the last
	res := mm.ReadBuckets([]int64{1}, "points", []int64{3}, 1458061005, 1458061010, "0")
	if res.UserToSum["1"] != 21 {
		t.Errorf("Incorrect Sum.  Expected 21, Received %d", res.UserToSum["1"])
	}
}

func TestMultiDayValidReadMZ(t *testing.T) {
	mm := NewMetricManagerMZ()

	r := psdcontext.Ctx.RedisPool.Get()
	r.Do("flushall")
	r.Close()
	// we will write 10 points

	err := mm.MetricRW.WritePoint("points", 1, 10, 3, 1458061005)
	if err != nil {
		t.Fail()
	}

	// add 11 points
	err = mm.MetricRW.WritePoint("points", 1, 11, 3, 1458061008)
	if err != nil {
		t.Fail()
	}

	// add 1 point
	err = mm.MetricRW.WritePoint("points", 1, 1, 3, 1458061011)
	if err != nil {
		t.Fail()
	}
	res := mm.ReadBuckets([]int64{1}, "points", []int64{3}, 1455481907, 1458061010, "0")
	if res.UserToSum["1"] != 21 {
		t.Errorf("Incorrect Sum.  Expected 21, Received %d", res.UserToSum["1"])
	}
}

func TestEvenLongerMultiDayValidReadMZ(t *testing.T) {
	mm := NewMetricManagerMZ()

	r := psdcontext.Ctx.RedisPool.Get()
	r.Do("flushall")
	r.Close()
	// we will write 10 points
	err := mm.MetricRW.WritePoint("points", 1, 100, 3, 1451635204)
	if err != nil {
		t.Fail()
	}

	err = mm.MetricRW.WritePoint("points", 1, 200, 3, 1454313600)
	if err != nil {
		t.Fail()
	}

	err = mm.MetricRW.WritePoint("points", 1, 10, 3, 1458061005)
	if err != nil {
		t.Fail()
	}

	// add 11 points
	err = mm.MetricRW.WritePoint("points", 1, 11, 3, 1458061008)
	if err != nil {
		t.Fail()
	}

	// add 1 point
	err = mm.MetricRW.WritePoint("points", 1, 1, 3, 1458061011)
	if err != nil {
		t.Fail()
	}
	res := mm.ReadBuckets([]int64{1}, "points", []int64{3}, 1451635200, 1458061011, "0")
	if res.UserToSum["1"] != 322 {
		t.Errorf("Incorrect Sum.  Expected 322, Received %d", res.UserToSum["1"])
	}

	res = mm.ReadBuckets([]int64{1}, "points", []int64{3}, 1451635205, 1458061010, "0")
	if res.UserToSum["1"] != 221 {
		t.Errorf("Incorrect Sum.  Expected 221, Received %d", res.UserToSum["1"])
	}
}

func BenchmarkWrite100MZ(b *testing.B) {

	mm := NewMetricManagerMZ()
	iteration := 0
	for {
		err := mm.MetricRW.WritePoint("steps", 1, 10, 3, time.Now().Unix())
		if err != nil {
			b.Fail()
		}

		iteration += 1
		if iteration >= 100 {
			break
		}
	}

}
