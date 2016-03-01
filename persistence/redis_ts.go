package persistence

//import "time"
//import "fmt"
//import "strconv"
//import "errors"
import "time"
import "github.com/garyburd/redigo/redis"

//import "pointspaced/psdcontext"
import timeseries "github.com/donnpebe/go-redis-timeseries"

type RedisTs struct{}

func (self RedisTs) WritePoint(flavor string, userId int64, value int64, activityTypeId int64, timestamp int64) {

	if (value == 0) ||
		(userId == 0) ||
		(timestamp == 0) ||
		(activityTypeId == 0) ||
		(flavor == "") {
		return
	}

	if timestamp < 1430838227 {
		return
	}

	//	r := psdcontext.Ctx.RedisPool.Get()

	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ts := timeseries.NewTimeSeries("psdts::device1", 1*time.Second, 500*time.Hour, conn)
	now := time.Now()
	err = ts.Add("data10000", now)
	if err != nil && err != timeseries.ErrNotFound {
		panic(err)
	}

	//	r.Close()
}
