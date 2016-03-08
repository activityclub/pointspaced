package persistence

import (
	"math/rand"
	"time"
)

type RedisSeeder struct {
}

func (self RedisSeeder) WritePoint(flavor string, userId int64, value int64, activityTypeId int64, timestamp int64) error {

	rval := int64(rand.Intn(5184000))
	t := time.Unix(timestamp-rval, 0)

	bucket_for_day := bucket_for_day(t)
	bucket_with_hour := bucket_with_hour(t, t.Hour())

	addToBucket(userId, 0, value, bucket_for_day, flavor)
	addToBucket(userId, activityTypeId, value, bucket_for_day, flavor)
	addToBucket(userId, 0, value, bucket_with_hour, flavor)
	addToBucket(userId, activityTypeId, value, bucket_with_hour, flavor)

	return nil
}
