package persistence

import (
	"fmt"
	"pointspaced/psdcontext"
	"time"
)

func (self RedisSimple) WritePoint(flavor string, userId int64, value int64, activityTypeId int64, timestamp int64) error {

	t := time.Unix(timestamp, 0)

	bucket_for_sec := bucket_for_sec(t)
	bucket_for_min := bucket_for_min(t)
	bucket_for_hour := bucket_for_hour(t)
	bucket_for_day := bucket_for_day(t)
	bucket_for_month := bucket_for_month(t)

	addToBucket(userId, 0, value, bucket_for_sec, flavor)
	addToBucket(userId, 0, value, bucket_for_min, flavor)
	addToBucket(userId, 0, value, bucket_for_hour, flavor)
	addToBucket(userId, 0, value, bucket_for_day, flavor)
	addToBucket(userId, 0, value, bucket_for_month, flavor)

	addToBucket(userId, activityTypeId, value, bucket_for_sec, flavor)
	addToBucket(userId, activityTypeId, value, bucket_for_min, flavor)
	addToBucket(userId, activityTypeId, value, bucket_for_hour, flavor)
	addToBucket(userId, activityTypeId, value, bucket_for_day, flavor)
	addToBucket(userId, activityTypeId, value, bucket_for_month, flavor)
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
