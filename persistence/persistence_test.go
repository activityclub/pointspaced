package persistence

import "testing"
import "time"
import "pointspaced/psdcontext"

func TestValidWrite(t *testing.T) {

	//cfg_file := c.GlobalString("config")
	//psdcontext.PrepareContext(cfg_file)
	psdcontext.Ctx.RedisPool = NewRedisPool(":6379")

	mm := NewMetricManager()
	err := mm.MetricWriter.WritePoint("steps", 1, 10, 3, time.Now().Unix())
	if err != nil {
		t.Fail()
	}
}

func TestInvalidWrite(t *testing.T) {
	psdcontext.Ctx.RedisPool = NewRedisPool(":6379")
	mm := NewMetricManager()
	err := mm.MetricWriter.WritePoint("steps", 1, 10, 3, 1430838226)
	if err == nil {
		t.Fail()
	}
}

func BenchmarkWrite100(b *testing.B) {

	psdcontext.Ctx.RedisPool = NewRedisPool(":6379")

	mm := NewMetricManager()
	iteration := 0
	for {
		err := mm.MetricWriter.WritePoint("steps", 1, 10, 3, time.Now().Unix())
		if err != nil {
			b.Fail()
		}

		iteration += 1
		if iteration >= 100 {
			break
		}
	}

}
