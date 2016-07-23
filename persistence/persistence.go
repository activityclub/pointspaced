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
		/*
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		*/
	}
}

type XQueryResponse struct {
	XToSum map[string]int64 `json:"results"`
}

type QueryResponse struct {
	UserToSum map[string]int64 `json:"results"`
	XToSum    map[string]int64 `json:"results"`
}

type MUResponse struct {
	Values map[string]int64   `json:"values"`
	ATids  map[string][]int64 `json:"atids"`
}

type MUMTResponse struct {
	Data map[string]map[string]interface{} `json:"data"`
}

type MetricRW interface {
	WritePoint(opts map[string]string) error
	OldWritePoint(thing string, userId, value, activityId, ts int64) error
	ReadBuckets(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64) QueryResponse
	QueryBuckets(uid, thing, aid, atid string, start_ts int64, end_ts int64) int64
	QueryBucketsLua(uid, thing, aid, atid string, start_ts int64, end_ts int64) (int64, []int64)
	MultiUserQuery(uids []string, thing, atid string, start_ts int64, end_ts int64) (*MUResponse, error)
	MultiUserMultiThingQuery(uids, things []string, atid string, start_ts int64, end_ts int64) (*MUMTResponse, error)
}

type CountRW interface {
	IncrementCount(thing string, ts, value int64) error
	ReadCount(thing string, start_ts, end_ts int64) int64
	requestsForRange(start_ts int64, end_ts int64) map[string]RedisHZRequest
}

type MetricManager struct {
	MetricRW
}

type CountManager struct {
	CountRW
}

func NewMetricManagerHZ() *MetricManager {
	mm := MetricManager{}
	mm.MetricRW = RedisHZ{}
	return &mm
}

func NewCountManagerHZ() *CountManager {
	cm := CountManager{}
	cm.CountRW = RedisHZ{}
	return &cm
}
