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

	secs, _, _, days, _ := deltasForRange(start_ts, end_ts)

	var full_days, before_hours, after_hours []string
	var before, after int64
	if days > 0.0 && secs > 3600 {
		before, full_days, after = splitDays(start_ts, end_ts)
		//fmt.Println(full_days)
		//fmt.Println(before, after)
		for _, uid := range uids {
			sum := int64(0)
			for _, atype := range aTypes {
				sum += sumFromRedis(full_days, uid, atype, metric)
			}
			qr.UserToSum[strconv.FormatInt(uid, 10)] = sum
		}

		// look at before and after and find full hours
		before, before_hours, _ = splitHours(start_ts, start_ts+before)
		// query each bfull_hours
		_, after_hours, after = splitHours(end_ts-after, end_ts)
		// query each afull_hours

		fmt.Println(before_hours)
		fmt.Println(after_hours)

		// no more full hours avail, go down to full mins, then secs
		buckets := bucketsForRange(start_ts, start_ts+before)
		//fmt.Println(buckets)

		for _, uid := range uids {
			sum := int64(0)
			for _, atype := range aTypes {
				sum += sumFromRedisMinBuckets(buckets, uid, atype, metric)
			}
			qr.UserToSum[strconv.FormatInt(uid, 10)] += sum
		}

		buckets = bucketsForRange(end_ts-after, end_ts)
		for _, uid := range uids {
			sum := int64(0)
			for _, atype := range aTypes {
				sum += sumFromRedisMinBuckets(buckets, uid, atype, metric)
			}
			qr.UserToSum[strconv.FormatInt(uid, 10)] += sum
		}
	} else {
		buckets := bucketsForRange(start_ts, end_ts)

		for _, uid := range uids {
			sum := int64(0)
			for _, atype := range aTypes {
				sum += sumFromRedisMinBuckets(buckets, uid, atype, metric)
			}
			qr.UserToSum[strconv.FormatInt(uid, 10)] = sum
		}
	}

	return qr
}

func deltasForRange(start_ts, end_ts int64) (secs int64, mins, hours, days, months float64) {
	secs = end_ts - start_ts
	mins = float64(secs) / 60.0
	hours = mins / 60.0
	days = hours / 24.0
	months = days / 30.0
	return
}

func splitHours(start_ts, end_ts int64) (before int64, buckets []string, after int64) {
	from := time.Unix(start_ts, 0)
	to := time.Unix(end_ts, 0)
	buckets = make([]string, 0)

	minCount := 0

	for {
		if from.Unix() > start_ts {
			break
		}
		from = from.Add(time.Minute)
		minCount += 1
	}

	before = int64((minCount * 60) + from.Second())
	from = time.Unix(from.Unix()-int64(from.Second()), 0)

	minCount = -1
	for {
		if to.Unix() < end_ts {
			break
		}
		to = to.Add(time.Minute * -1)
		minCount += 1
	}

	after = int64((minCount * 60) + to.Second())
	to = time.Unix(to.Unix()-int64(to.Second()), 0)

	for {
		if from.Unix() > to.Unix() {
			break
		}
		bucket := bucket_for_hour(from)
		buckets = append(buckets, bucket)
		from = from.Add(time.Hour)
	}

	return
}

func splitDays(start_ts, end_ts int64) (before int64, buckets []string, after int64) {
	from := time.Unix(start_ts, 0)
	to := time.Unix(end_ts, 0)
	buckets = make([]string, 0)

	startDay := from.Day()
	hourCount := 0

	for {
		if from.Day() > startDay {
			break
		}
		from = from.Add(time.Hour)
		hourCount += 1
	}

	// this are the seconds before 1st full day
	before = int64((hourCount * 3600) + (from.Minute() * 60) + from.Second())
	// make from full day at 00:00
	from = time.Unix(from.Unix()-int64((from.Minute()*60)-from.Second()), 0)

	endDay := to.Day()

	hourCount = -1
	for {
		if to.Day() < endDay {
			break
		}
		to = to.Add(time.Hour * -1)
		hourCount += 1
	}

	after = int64((hourCount * 3600) + (to.Minute() * 60) + to.Second())
	to = time.Unix(to.Unix()-after, 0)

	for {
		if from.Unix() > to.Unix() {
			break
		}
		bucket := bucket_for_day(from)
		buckets = append(buckets, bucket)
		from = from.Add(time.Hour * 24)
	}

	return
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
	format := t.Format("2006010215")
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
	ordered_list := make([]string, 0)
	for b := range buckets {
		key := makeKey(uid, atype, b, metric)
		r.Send("ZRANGE", key, "0", "-1", "WITHSCORES")
		ordered_list = append(ordered_list, b)
	}
	r.Flush()
	sum := int64(0)
	for _, b := range ordered_list {
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
