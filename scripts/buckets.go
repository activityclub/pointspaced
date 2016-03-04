package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"math/rand"
	"time"
)

type PoolHolder struct {
	pool *redis.Pool
}

func main() {
	fmt.Println("query from to")
	ph := PoolHolder{}
	ph.pool = NewRedisPool(":6379")
	var from, to int64
	from = 1457100000 - (86400 * 4) - 3600
	to = 1457100000

	total := ph.total_steps(from, to)
	fmt.Println("total ", total)
}

func bucket_for_day(t time.Time) string {
	format := t.Format("20060102")
	return fmt.Sprintf("%s", format)
}

func bucket_with_hour(t time.Time, hour int) string {
	format := t.Format("20060102")
	return fmt.Sprintf("%s%02d", format, hour)
}

func (self *PoolHolder) total_steps(from, to int64) int {
	from_utc := time.Unix(from, 0).UTC()
	to_utc := time.Unix(to, 0).UTC()
	fmt.Println(to_utc)

	var buckets []string

	hour := from_utc.Hour()
	for {
		if hour > 23 {
			break
		}
		bucket := bucket_with_hour(from_utc, hour)
		fmt.Println(bucket)
		buckets = append(buckets, bucket)
		hour += 1
		from_utc = from_utc.Add(time.Hour)
	}

	fmt.Println("----")
	for {
		if from_utc.Unix() >= (to_utc.Unix() - 86400) {
			break
		}
		bucket := bucket_for_day(from_utc)
		fmt.Println(bucket)

		from_utc = from_utc.Add(time.Hour * 24)
	}

	fmt.Println("----")
	hour = from_utc.Hour()
	for {
		if from_utc.Unix() > to_utc.Unix() {
			break
		}
		bucket := bucket_with_hour(from_utc, hour)
		fmt.Println(bucket)
		hour += 1
		from_utc = from_utc.Add(time.Hour)
	}

	fmt.Println("----")
	fmt.Println(buckets)
	self.readBuckets(buckets)

	return 0
}

func main2() {
	fmt.Println("hi")

	ts := time.Now().Unix()

	ph := PoolHolder{}
	ph.pool = NewRedisPool(":6379")
	i := int64(3601)
	for {
		steps := rand.Intn(999)
		fmt.Println(steps)
		ph.addToBuckets("327", steps, ts-i)
		i += 3602

		if i > 999999 {
			break
		}
	}
}

func (self *PoolHolder) addToBuckets(user string, val int, ts int64) {
	t := time.Unix(ts, 0).UTC()

	format := t.Format("20060102")
	bucket_for_day := fmt.Sprintf("%s", format)
	bucket_with_hour := fmt.Sprintf("%s%02d", format, t.Hour())
	self.addToBucket(user, bucket_for_day, val)
	self.addToBucket(user, bucket_with_hour, val)
}

func (self *PoolHolder) readBuckets(buckets []string) {
	//addToBucket("327", "2016030209", 10)
	r := self.pool.Get()
	w, ww := r.Do("MGET", buckets)
	fmt.Println("bye ", w, ww)
	r.Close()
}

func (self *PoolHolder) addToBucket(user, bucket string, val int) {
	//addToBucket("327", "2016030209", 10)
	r := self.pool.Get()
	key := "psd:"
	strAType := "0"
	flavor := "steps"
	key = key + user + ":" + strAType + ":" + flavor + ":" + bucket
	w, ww := r.Do("INCRBY", key, val)
	fmt.Println("bye ", w, ww)
	r.Close()
}

func NewRedisPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
