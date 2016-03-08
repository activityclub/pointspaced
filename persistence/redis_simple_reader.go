package persistence

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"pointspaced/psdcontext"
	"strconv"
	"time"
)

type SimpleSum struct {
	From time.Time
}

type RedisSimple struct {
}

func (self RedisSimple) ReadBuckets(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64) QueryResponse {

	qr := QueryResponse{}
	qr.UserToSum = make(map[string]int64)

	buckets := bucketsForRange(start_ts, end_ts)
	for _, uid := range uids {
		sum := int64(0)
		for _, atype := range aTypes {
			sum += sumFromRedis(buckets, uid, atype, metric)
		}
		qr.UserToSum[strconv.FormatInt(uid, 10)] = sum
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

func bucketsForRange(start_ts, end_ts int64) []string {
	list := make([]string, 0)

	simple := SimpleSum{}
	simple.From = time.Unix(start_ts, 0)
	to := time.Unix(end_ts, 0)

	for _, b := range simple.hour_buckets_before_full_days() {
		list = append(list, b)
	}
	for _, b := range simple.full_day_buckets(to) {
		list = append(list, b)
	}
	for _, b := range simple.hour_buckets_after_full_days(to) {
		list = append(list, b)
	}

	return list
}

func (self *SimpleSum) hour_buckets_before_full_days() []string {
	list := make([]string, 0)
	hour := self.From.Hour()
	for {
		if hour > 23 {
			break
		}
		bucket := bucket_with_hour(self.From, hour)
		list = append(list, bucket)
		hour += 1
		self.From = self.From.Add(time.Hour)
	}

	return list
}

func (self *SimpleSum) full_day_buckets(to time.Time) []string {
	list := make([]string, 0)
	for {
		if self.From.Unix() >= (to.Unix() - 86400) {
			break
		}
		bucket := bucket_for_day(self.From)
		list = append(list, bucket)

		self.From = self.From.Add(time.Hour * 24)
	}
	return list
}

func (self *SimpleSum) hour_buckets_after_full_days(to time.Time) []string {
	list := make([]string, 0)
	hour := self.From.Hour()
	for {
		if self.From.Unix() > to.Unix() {
			break
		}
		bucket := bucket_with_hour(self.From, hour)
		list = append(list, bucket)
		hour += 1
		self.From = self.From.Add(time.Hour)
	}
	return list
}

func makeKey(uid, atype int64, bucket, metric string) string {
	key := "psd:"
	strAType := strconv.FormatInt(atype, 10)
	key = key + strconv.FormatInt(uid, 10) + ":" + strAType + ":" + metric + ":" + bucket
	return key
}

func sumFromRedis(buckets []string, uid, atype int64, metric string) int64 {
	r := psdcontext.Ctx.RedisPool.Get()

	for _, b := range buckets {
		r.Send("GET", makeKey(uid, atype, b, metric))
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
