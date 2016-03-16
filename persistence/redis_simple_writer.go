package persistence

import (
	"fmt"
	"pointspaced/psdcontext"
	"time"
)

func (self RedisSimple) WritePoint(flavor string, userId int64, value int64, activityTypeId int64, timestamp int64) error {

	t := time.Unix(timestamp, 0)

	bucket_for_min := bucket_for_min(t)
	fmt.Println("min ", bucket_for_min)
	bucket_for_hour := bucket_for_hour(t)
	bucket_for_day := bucket_for_day(t)

	addToBucketWithSeconds(userId, 0, value, bucket_for_min, flavor, t.Second())
	addToBucket(userId, 0, value, bucket_for_hour, flavor)
	addToBucket(userId, 0, value, bucket_for_day, flavor)
	return nil
}

func addToBucketWithSeconds(uid, atype, val int64, bucket, metric string, seconds int) {
	r := psdcontext.Ctx.RedisPool.Get()

	fmt.Println("sec ", seconds)
	//  ZADD bucket13 200 5.01

	key := makeKey(uid, atype, bucket, metric)
	_, err := r.Do("ZADD", key, val, seconds)
	if err != nil {
		fmt.Println(err)
	}
	r.Close()
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
