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

func (self RedisSimple) ReadBuckets(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64, debug string) QueryResponse {

	qr := QueryResponse{}
	qr.UserToSum = make(map[string]int64)

	buckets := bucketsForRange(start_ts, end_ts)
	fmt.Println(buckets)

	if debug == "1" {
		//qr.Debug = nil
	}
	for _, uid := range uids {
		sum := int64(0)
		for _, atype := range aTypes {
			sum += sumFromRedisMinBuckets(buckets, uid, atype, metric)
		}
		qr.UserToSum[strconv.FormatInt(uid, 10)] = sum
	}

	return qr
}

func bucketsForRange(start_ts, end_ts int64) map[string][]int {
	min_hash := make(map[string]int)
	max_hash := make(map[string]int)
	final_hash := make(map[string][]int)

	from := time.Unix(start_ts, 0)
	to := time.Unix(end_ts, 0)

	for {
		if from.Unix() > to.Unix() {
			break
		}
		bucket := bucket_for_min(from)
		min_hash[bucket] = -1
		from = from.Add(time.Second)
	}

	from = time.Unix(start_ts, 0)
	for {
		if from.Unix() > to.Unix() {
			break
		}
		bucket := bucket_for_min(from)
		if min_hash[bucket] == -1 {
			min_hash[bucket] = from.Second()
		}
		max_hash[bucket] = from.Second()
		from = from.Add(time.Second)
	}

	for key := range min_hash {
		min := min_hash[key]
		if min == -1 {
			min = 0
		}
		max := max_hash[key]
		final_hash[key] = []int{min, max}
	}

	return final_hash
}
func bucket_for_day(t time.Time) string {
	format := t.Format("20060102")
	return fmt.Sprintf("%s", format)
}

func bucket_for_hour(t time.Time) string {
	format := t.Format("20060102")
	return fmt.Sprintf("%s", format)
}

func bucket_for_min(t time.Time) string {
	format := t.Format("200601021504")
	return fmt.Sprintf("%s", format)
}

/*
func bucketsForRange2(start_ts, end_ts int64) []string {
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
		if self.From.Unix() >= to.Unix()+3600 {
			break
		}
		bucket := bucket_with_hour(self.From, hour)
		list = append(list, bucket)
		hour += 1
		self.From = self.From.Add(time.Hour)
	}
	return list
}*/

func makeKey(uid, atype int64, bucket, metric string) string {
	key := "psd:"
	strAType := strconv.FormatInt(atype, 10)
	key = key + strconv.FormatInt(uid, 10) + ":" + strAType + ":" + metric + ":" + bucket
	return key
}

func sumFromRedisMinBuckets(buckets map[string][]int, uid, atype int64, metric string) int64 {
	r := psdcontext.Ctx.RedisPool.Get()
	for b := range buckets {
		key := makeKey(uid, atype, b, metric)
		r.Send("ZRANGE", key, "0", "-1", "WITHSCORES")
	}
	r.Flush()
	sum := int64(0)
	for b := range buckets {
		val := buckets[b]
		min := val[0]
		max := val[1]

		theMap, err := redis.IntMap(r.Receive())
		if err != nil {
			panic(err)
		}
		for mkey := range theMap {
			sec_total := theMap[mkey]
			mkey_int, _ := strconv.ParseInt(mkey, 10, 32)

			if int(mkey_int) >= min && int(mkey_int) <= max {
				sum += int64(sec_total)
			}
		}
	}
	r.Close()
	return sum
}
