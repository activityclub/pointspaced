package persistence

import "time"
import "github.com/garyburd/redigo/redis"

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

type QueryResponse struct {
	UserToSum map[string]int64 `json:"results"`
}

type MetricWriter interface {
	WritePoint(flavor string, userId int64, value int64, activityTypeId int64, timestamp int64) error
	ReadBuckets(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64) QueryResponse
}

type MetricManager struct {
	MetricWriter
}

func NewMetricManager() *MetricManager {
	mm := MetricManager{}

	// TODO make configurable?
	mm.MetricWriter = RedisSimple{}

	return &mm
}
