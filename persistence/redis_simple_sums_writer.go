package persistence

import (
	"fmt"
	"pointspaced/psdcontext"
	"time"
)

func WriteMetric(uid, val, atype, ts int64, metric string) {
	t := time.Unix(ts, 0)

	bucket_for_day := bucket_for_day(t)
	bucket_with_hour := bucket_with_hour(t, t.Hour())

	addToBucket(uid, 0, val, bucket_for_day, metric)
	addToBucket(uid, atype, val, bucket_for_day, metric)
	addToBucket(uid, 0, val, bucket_with_hour, metric)
	addToBucket(uid, atype, val, bucket_with_hour, metric)
}

func addToBucket(uid, atype, val int64, bucket, metric string) {
	psdcontext.Ctx.RedisPool = NewRedisPool(":6379")
	r := psdcontext.Ctx.RedisPool.Get()
	key := makeKey(uid, atype, bucket, metric)
	_, err := r.Do("INCRBY", key, val)
	if err != nil {
		fmt.Println(err)
	}
	r.Close()
}
