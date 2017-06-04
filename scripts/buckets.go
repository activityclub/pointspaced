package main

import (
	"fmt"
	"github.com/activityclub/pointspaced/persistence"
	"github.com/activityclub/pointspaced/psdcontext"
	"math/rand"
	"time"
)

func randMetric(uid, atype int64, metric string) {
	val := int64(rand.Intn(999))
	fmt.Println(val)
	ts := time.Now().Unix()
	rs := persistence.RedisSeeder{}
	rs.WritePoint(metric, uid, val, atype, ts)
}

func main2() {
	mm := persistence.NewMetricManagerSimple()
	uids := []int64{327}
	atypes := []int64{0}

	qr := mm.ReadBuckets(uids, "points", atypes, 1454693071, 1454693071+60, "1")
	fmt.Println(qr)
}

func main() {
	psdcontext.Ctx.RedisPool = persistence.NewRedisPool(":6379")
	fmt.Println("hi")

	for {
		randMetric(327, 3, "points")
		randMetric(1, 3, "points")
		randMetric(327, 3, "calories")
		randMetric(1, 3, "calories")

		randMetric(327, 5, "points")
		randMetric(1, 5, "points")
		randMetric(327, 5, "calories")
		randMetric(1, 5, "calories")

		time.Sleep(1 * time.Second)
	}
}
