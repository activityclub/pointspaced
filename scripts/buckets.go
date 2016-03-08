package main

import (
	"fmt"
	"math/rand"
	"pointspaced/persistence"
	"pointspaced/psdcontext"
	"time"
)

func randMetric(uid int64, metric string) {
	m := int64(rand.Intn(999))
	fmt.Println(m)
	ts := time.Now().Unix()
	persistence.WriteMetric(uid, m, 3, ts, metric)
	persistence.WriteMetric(uid, m, 5, ts, metric)
}

func main() {
	psdcontext.Ctx.RedisPool = persistence.NewRedisPool(":6379")
	fmt.Println("hi")

	for {
		randMetric(327, "points")
		randMetric(1, "points")
		randMetric(327, "calories")
		randMetric(1, "calories")

		time.Sleep(1 * time.Second)
	}
}
