package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

func main() {
	fmt.Println("hi")

	//timestamp := time.Now().Unix()

	addToBucket("327", "2016-03-02", 10)
	addToBucket("327", "2016-03-02 9", 10)

	addToBucket("327", "2016-03-02", 15)
	addToBucket("327", "2016-03-02 10", 15)

	addToBucket("327", "2016-03-02", 20)
	addToBucket("327", "2016-03-02 11", 20)
}

func addToBucket(user, bucket string, val int) {
	pool := NewRedisPool(":6379")
	r := pool.Get()
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
