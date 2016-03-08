package persistence

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"pointspaced/psdcontext"
	"strconv"
	"time"
)

type QueryResponse struct {
	UserToSum map[string]int64 `json:"results"`
}

type SimpleSum struct {
	From time.Time
}

func ReadBuckets(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64) QueryResponse {

	qr := QueryResponse{}
	qr.UserToSum = make(map[string]int64)

	for _, uid := range uids {
		buckets := bucketsForRange(uid, start_ts, end_ts)
		qr.UserToSum[strconv.FormatInt(uid, 10)] = sumFromRedis(buckets)
	}
	return qr
}

func bucket_for_day(t time.Time) string {
	format := t.Format("20060102")
	return fmt.Sprintf("%s", format)
}

func bucket_with_hour(t time.Time, hour int) string {
	format := t.Format("20060102")
	return fmt.Sprintf("%s%02d", format, hour)
}

func bucketsForRange(uid, start_ts, end_ts int64) []string {
	list := make([]string, 0)

	simple := SimpleSum{}
	simple.From = time.Unix(start_ts, 0)
	to := time.Unix(end_ts, 0)

	for _, b := range simple.day_buckets_before_full_days(uid) {
		list = append(list, b)
	}
	for _, b := range simple.full_day_buckets(uid, to) {
		list = append(list, b)
	}
	for _, b := range simple.day_buckets_after_full_days(uid, to) {
		list = append(list, b)
	}

	return list
}

func (self *SimpleSum) day_buckets_before_full_days(uid int64) []string {
	list := make([]string, 0)
	hour := self.From.Hour()
	for {
		if hour > 23 {
			break
		}
		bucket := bucket_with_hour(self.From, hour)
		list = append(list, makeKey(uid, bucket))
		hour += 1
		self.From = self.From.Add(time.Hour)
	}

	return list
}

func (self *SimpleSum) full_day_buckets(uid int64, to time.Time) []string {
	list := make([]string, 0)
	for {
		if self.From.Unix() >= (to.Unix() - 86400) {
			break
		}
		bucket := bucket_for_day(self.From)
		list = append(list, makeKey(uid, bucket))

		self.From = self.From.Add(time.Hour * 24)
	}
	return list
}

func (self *SimpleSum) day_buckets_after_full_days(uid int64, to time.Time) []string {
	list := make([]string, 0)
	hour := self.From.Hour()
	for {
		if self.From.Unix() >= to.Unix() {
			break
		}
		bucket := bucket_with_hour(self.From, hour)
		list = append(list, makeKey(uid, bucket))
		hour += 1
		self.From = self.From.Add(time.Hour)
	}
	return list
}

func makeKey(uid int64, bucket string) string {
	key := "psd:"
	strAType := "0"
	flavor := "steps"
	key = key + strconv.FormatInt(uid, 10) + ":" + strAType + ":" + flavor + ":" + bucket
	return key
}

func sumFromRedis(buckets []string) int64 {
	psdcontext.Ctx.RedisPool = NewRedisPool(":6379")
	r := psdcontext.Ctx.RedisPool.Get()

	for _, b := range buckets {
		r.Send("GET", b)
	}
	r.Flush()
	var sum int64
	sum = 0
	for _, _ = range buckets {
		v, err := redis.Int(r.Receive())
		if err != nil && err.Error() != "redigo: nil returned" {
			fmt.Println(err)
		}
		sum += int64(v)
	}

	r.Close()
	return sum
}
