package persistence

import (
	"fmt"
	"pointspaced/psdcontext"
	"time"
)

type QueryResponse struct {
	UserToSum map[string]int64 `json:"results"`
}

func ReadBuckets(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64) QueryResponse {
	psdcontext.Ctx.RedisPool = NewRedisPool(":6379")
	//r := psdcontext.Ctx.RedisPool.Get()

	qr := QueryResponse{}
	qr.UserToSum = make(map[string]int64)
	qr.UserToSum["327"] = 2342342
	qr.UserToSum["1"] = 12342342

	buckets := bucketsForRange(start_ts, end_ts)

	fmt.Println("hi ", buckets)
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

func bucketsForRange(start_ts int64, end_ts int64) []string {
	list := make([]string, 0)

	for _, b := range day_buckets_before_full_days() {
		list = append(list, b)
	}
	for _, b := range full_day_buckets() {
		list = append(list, b)
	}
	for _, b := range day_buckets_after_full_days() {
		list = append(list, b)
	}

	return list
}

func day_buckets_before_full_days() []string {
	temp := []string{"test", "Test2"}
	return temp
}

func full_day_buckets() []string {
	temp := []string{"test1", "Test2"}
	return temp
}

func day_buckets_after_full_days() []string {
	temp := []string{"test2", "Test2"}
	return temp
}
