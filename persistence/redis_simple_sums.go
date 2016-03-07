package persistence

import (
	"fmt"
	"pointspaced/psdcontext"
)

type QueryResponse struct {
	UserToSum map[string]int64 `json:"results"`
}

func ReadBuckets(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64) QueryResponse {
	psdcontext.Ctx.RedisPool = NewRedisPool(":6379")
	r := psdcontext.Ctx.RedisPool.Get()
	fmt.Println("hi ", r)

	qr := QueryResponse{}
	qr.UserToSum = make(map[string]int64)
	qr.UserToSum["327"] = 2342342
	qr.UserToSum["1"] = 12342342

	buckets := bucketsForRange(start_ts, end_ts)

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

	list1 := day_buckets_before_full_days()
	list2 := full_day_buckets()
	list3 := day_buckets_after_full_days()

	return append(append(list1, list2), list3)
}

func day_buckets_before_full_days() []string {
	temp := []string{"test", "Test2"}
	return temp
}

func full_day_buckets() []string {
}

func day_buckets_after_full_days() []string {
}
