package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

type PoolHolder struct {
	pool *redis.Pool
}

func main() {
	fmt.Println("hi")

	ts := time.Now().Unix()

	ph := PoolHolder{}
	ph.pool = NewRedisPool(":6379")
	i := int64(3601)
	for {
		ph.addToBuckets("327", 10, ts-i)
		i += 3602

		if i > 999999 {
			break
		}
	}
}

func (self *PoolHolder) addToBuckets(user string, val, ts int64) {
	t := time.Unix(ts, 0).UTC()

	format := t.Format("20060102")
	bucket_for_day := fmt.Sprintf("%s", format)
	bucket_with_hour := fmt.Sprintf("%s%02d", format, t.Hour())
	self.addToBucket(user, bucket_for_day, val)
	self.addToBucket(user, bucket_with_hour, val)
}

func (self *PoolHolder) addToBucket(user, bucket string, val int64) {
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
