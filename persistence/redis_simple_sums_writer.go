package persistence

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"pointspaced/psdcontext"
	"strconv"
	"time"
)

func WriteMetric(uid, atype, ts, val int64, metric string) {
	t := time.Unix(ts, 0).UTC()

	bucket_for_day := bucket_for_day(t)
	bucket_with_hour := bucket_with_hour(t, t.Hour())

	addToBucket(uid, val, bucket_for_day, metric)
	addToBucket(uid, val, bucket_with_hour, metric)
}

func addToBucket(uid, val int64, bucket, metric string) {
	r := self.pool.Get()
	key := makeKey(uid, bucket, metric)
	_, err := r.Do("INCRBY", key, val)
	if err != nil {
		fmt.Println(err)
	}
	r.Close()
}
