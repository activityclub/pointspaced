package persistence

import (
	"fmt"
	"math/rand"
	"time"
)

type RedisSeeder struct {
}

func (self RedisSeeder) WritePoint(flavor string, userId int64, value int64, activityTypeId int64, timestamp int64) error {

	rval := int64(rand.Intn(5184000))
	t := time.Unix(timestamp-rval, 0)

	bucket_for_min := bucket_for_min(t)
	fmt.Println("min ", bucket_for_min)
	bucket_for_hour := bucket_for_hour(t)
	bucket_for_day := bucket_for_day(t)

	//addToBucketWithSeconds(userId, 0, value, bucket_for_min, flavor, t.Second())
	addToBucket(userId, 0, value, bucket_for_hour, flavor)
	addToBucket(userId, 0, value, bucket_for_day, flavor)

	return nil
}
