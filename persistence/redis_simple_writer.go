package persistence

import (
	"fmt"
	"pointspaced/psdcontext"
	"time"
)

type RedisSimple struct {
}

func (self RedisSimple) WritePoint(flavor string, userId int64, value int64, activityTypeId int64, timestamp int64) error {

	t := time.Unix(timestamp, 0)

	bucket_for_day := bucket_for_day(t)
	bucket_with_hour := bucket_with_hour(t, t.Hour())

	addToBucket(userId, 0, value, bucket_for_day, flavor)
	addToBucket(userId, activityTypeId, value, bucket_for_day, flavor)
	addToBucket(userId, 0, value, bucket_with_hour, flavor)
	addToBucket(userId, activityTypeId, value, bucket_with_hour, flavor)

	return nil
}

func addToBucket(uid, atype, val int64, bucket, metric string) {
	r := psdcontext.Ctx.RedisPool.Get()
	key := makeKey(uid, atype, bucket, metric)
	_, err := r.Do("INCRBY", key, val)
	if err != nil {
		fmt.Println(err)
	}
	r.Close()
}
