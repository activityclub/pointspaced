package main

import (
	"fmt"
	"math/rand"
	"pointspaced/persistence"
	"pointspaced/psdcontext"
	"time"
)

func randMetric(uid, atype int64, metric string) {
	val := int64(rand.Intn(999))
	fmt.Println(val)
	ts := time.Now().Unix()
	rs := persistence.RedisSeeder{}
	rs.WritePoint(metric, uid, val, atype, ts)
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
