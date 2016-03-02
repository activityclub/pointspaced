package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

func main() {
	fmt.Println("hi")
	pool := NewRedisPool(":6379")
	r := pool.Get()

	key := "psd:"
	key = key + "327" + ":" + "0" + ":" + "1" + ":" + "123"
	value := 55
	timestamp := time.Now().Unix()
	r.Do("ZINCRBY", key, value, timestamp)
	fmt.Println("bye ")
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
