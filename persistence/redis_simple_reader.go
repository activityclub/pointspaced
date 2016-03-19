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

func before_times(ts int64) (min, hour, day, month, year time.Time) {
	from := time.Unix(ts, 0)
	min = from
	if from.Second() > 0 {
		secs_til_min := time.Duration(60 - from.Second())
		from = from.Add(time.Second * secs_til_min)
		min = from
	}
	hour = from
	if from.Minute() > 0 {
		mins_til_hour := time.Duration(60 - from.Minute())
		from = from.Add(time.Minute * mins_til_hour)
		hour = from
	}
	day = from
	if from.Hour() > 0 {
		hours_til_day := time.Duration(24 - from.Hour())
		from = from.Add(time.Hour * hours_til_day)
		day = from
	}
	for {
		if from.Day() == 1 {
			break
		}
		from = from.Add(time.Hour * 24)
	}
	month = from

	for {
		if from.Day() == 1 && from.Month() == 1 {
			break
		}
		from = from.Add(time.Hour * 24)
	}
	year = from

	return
}

func after_times(ts int64) (min, hour, day, month, year time.Time) {
	to := time.Unix(ts, 0)
	if to.Second() > 0 {
		secs_til_min := time.Duration(to.Second())
		to = to.Add(time.Second * secs_til_min * -1)
		min = to
	}
	if to.Minute() > 0 {
		mins_til_hour := time.Duration(to.Minute())
		to = to.Add(time.Minute * mins_til_hour * -1)
		hour = to
	}
	if to.Hour() > 0 {
		hours_til_day := time.Duration(to.Hour())
		to = to.Add(time.Hour * hours_til_day * -1)
		day = to
	}
	for {
		if to.Day() == 1 && to.Hour() == 0 {
			break
		}
		to = to.Add(time.Hour * -1)
	}
	month = to
	for {
		if to.Day() == 1 && to.Month() == 1 {
			break
		}
		to = to.Add(time.Hour * -24)
	}
	year = to
	return
}

func addSumNormalBuckets(buckets []string, uids []int64, metric string, aTypes []int64, qr *QueryResponse) {
	for _, uid := range uids {
		sum := int64(0)
		for _, atype := range aTypes {
			sum += sumFromRedis(buckets, uid, atype, metric)
		}

		qr.UserToSum[strconv.FormatInt(uid, 10)] += sum
	}
}

func determineRangeType(start_ts, end_ts int64) int {
	delta := end_ts - start_ts
	if delta/31536000 > 0 {
		return 6 // year
	}
	if delta/2592000 > 0 {
		return 5 // month
	}
	if delta/86400 > 0 {
		return 4 // day
	}
	if delta/3600 > 0 {
		return 3 // hour
	}
	if delta/60 > 0 {
		return 2
	}

	return 1
}

func readBucketsType2(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64) QueryResponse {
	qr := QueryResponse{}
	qr.UserToSum = make(map[string]int64)

	min, bhour, _, _, _ := before_times(start_ts)

	sec_buckets := bucketsForSecs(start_ts, min.Unix()-1)
	min_buckets := bucketsForMins(min.Unix(), bhour.Unix()-1)

	addSumNormalBuckets(sec_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(min_buckets, uids, metric, aTypes, &qr)

	min, _, _, _, _ = after_times(end_ts)

	sec_buckets = bucketsForSecs(min.Unix(), end_ts)

	addSumNormalBuckets(sec_buckets, uids, metric, aTypes, &qr)

	return qr
}

func readBucketsType3(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64) QueryResponse {
	qr := QueryResponse{}
	qr.UserToSum = make(map[string]int64)

	min, hour, bday, _, _ := before_times(start_ts)

	sec_buckets := bucketsForSecs(start_ts, min.Unix()-1)
	min_buckets := bucketsForMins(min.Unix(), hour.Unix()-1)
	hour_buckets := bucketsForHours(hour.Unix(), bday.Unix()-1)

	addSumNormalBuckets(sec_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(min_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(hour_buckets, uids, metric, aTypes, &qr)

	var aday time.Time
	min, hour, aday, _, _ = after_times(end_ts)

	sec_buckets = bucketsForSecs(min.Unix(), end_ts)
	min_buckets = bucketsForMins(hour.Unix(), min.Unix()-1)
	hour_buckets = bucketsForHours(aday.Unix(), hour.Unix()-1)

	addSumNormalBuckets(sec_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(min_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(hour_buckets, uids, metric, aTypes, &qr)

	return qr
}

func readBucketsType4(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64) QueryResponse {
	qr := QueryResponse{}
	qr.UserToSum = make(map[string]int64)

	min, hour, bday, _, _ := before_times(start_ts)

	sec_buckets := bucketsForSecs(start_ts, min.Unix()-1)
	min_buckets := bucketsForMins(min.Unix(), hour.Unix()-1)
	hour_buckets := bucketsForHours(hour.Unix(), bday.Unix()-1)

	addSumNormalBuckets(sec_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(min_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(hour_buckets, uids, metric, aTypes, &qr)

	//fmt.Println(min_buckets)
	//fmt.Println(sec_buckets)
	//fmt.Println(hour_buckets)
	//fmt.Println("before_times: ", min, hour, bday)
	//fmt.Println(bday)

	var aday time.Time
	min, hour, aday, _, _ = after_times(end_ts)

	//fmt.Println("after_times: ", min, hour, aday)

	day_buckets := bucketsForDays(bday.Unix(), aday.Unix()-1)
	sec_buckets = bucketsForSecs(min.Unix(), end_ts)
	min_buckets = bucketsForMins(hour.Unix(), min.Unix()-1)
	hour_buckets = bucketsForHours(aday.Unix(), hour.Unix()-1)

	addSumNormalBuckets(sec_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(min_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(day_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(hour_buckets, uids, metric, aTypes, &qr)

	//fmt.Println(day_buckets)
	//fmt.Println(min_buckets)
	//fmt.Println(sec_buckets)
	//fmt.Println(hour_buckets)
	//fmt.Println(day)
	//fmt.Println(hour)
	//fmt.Println(min)

	return qr
}

func readBucketsType5(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64) QueryResponse {
	qr := QueryResponse{}
	qr.UserToSum = make(map[string]int64)

	min, hour, day, bmonth, _ := before_times(start_ts)

	sec_buckets := bucketsForSecs(start_ts, min.Unix()-1)
	min_buckets := bucketsForMins(min.Unix(), hour.Unix()-1)
	hour_buckets := bucketsForHours(hour.Unix(), day.Unix()-1)
	day_buckets := bucketsForDays(day.Unix(), bmonth.Unix()-1)

	addSumNormalBuckets(sec_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(min_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(hour_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(day_buckets, uids, metric, aTypes, &qr)

	var amonth time.Time
	min, hour, day, amonth, _ = after_times(end_ts)

	sec_buckets = bucketsForSecs(min.Unix(), end_ts)
	min_buckets = bucketsForMins(hour.Unix(), min.Unix()-1)
	hour_buckets = bucketsForHours(day.Unix(), hour.Unix()-1)
	day_buckets = bucketsForDays(amonth.Unix(), day.Unix()-1)
	month_buckets := bucketsForMonths(bmonth.Unix(), amonth.Unix()-1)

	addSumNormalBuckets(sec_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(min_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(day_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(hour_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(month_buckets, uids, metric, aTypes, &qr)

	return qr
}

func readBucketsType6(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64) QueryResponse {
	qr := QueryResponse{}
	qr.UserToSum = make(map[string]int64)

	min, hour, day, month, byear := before_times(start_ts)

	sec_buckets := bucketsForSecs(start_ts, min.Unix()-1)
	min_buckets := bucketsForMins(min.Unix(), hour.Unix()-1)
	hour_buckets := bucketsForHours(hour.Unix(), day.Unix()-1)
	day_buckets := bucketsForDays(day.Unix(), month.Unix()-1)

	month_buckets := bucketsForMonths(month.Unix(), byear.Unix()-1)

	addSumNormalBuckets(sec_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(min_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(hour_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(day_buckets, uids, metric, aTypes, &qr)

	var ayear time.Time
	min, hour, day, month, ayear = after_times(end_ts)

	sec_buckets = bucketsForSecs(min.Unix(), end_ts)
	min_buckets = bucketsForMins(hour.Unix(), min.Unix()-1)
	hour_buckets = bucketsForHours(day.Unix(), hour.Unix()-1)
	day_buckets = bucketsForDays(month.Unix(), day.Unix()-1)
	month_buckets = bucketsForMonths(ayear.Unix(), month.Unix()-1)
	year_buckets := bucketsForMonths(byear.Unix(), ayear.Unix()-1)

	addSumNormalBuckets(sec_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(min_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(day_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(hour_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(month_buckets, uids, metric, aTypes, &qr)
	addSumNormalBuckets(year_buckets, uids, metric, aTypes, &qr)

	return qr
}

func (self RedisSimple) ReadBuckets(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64, debug string) QueryResponse {

	timeRangeType := determineRangeType(start_ts, end_ts)
	//fmt.Println("type ", timeRangeType)

	if timeRangeType == 1 {
		qr := QueryResponse{}
		qr.UserToSum = make(map[string]int64)
		sec_buckets := bucketsForSecs(start_ts, end_ts)
		addSumNormalBuckets(sec_buckets, uids, metric, aTypes, &qr)
		return qr
	}
	if timeRangeType == 2 {
		return readBucketsType2(uids, metric, aTypes, start_ts, end_ts)
	}
	if timeRangeType == 3 {
		return readBucketsType3(uids, metric, aTypes, start_ts, end_ts)
	}
	if timeRangeType == 4 {
		return readBucketsType4(uids, metric, aTypes, start_ts, end_ts)
	}
	if timeRangeType == 5 {
		return readBucketsType5(uids, metric, aTypes, start_ts, end_ts)
	}
	if timeRangeType == 6 {
		return readBucketsType6(uids, metric, aTypes, start_ts, end_ts)
	}

	return QueryResponse{}
}

func bucketsForYears(start_ts, end_ts int64) []string {
	results := make([]string, 0)

	from := time.Unix(start_ts, 0)
	to := time.Unix(end_ts, 0)

	for {
		if from.Unix() > to.Unix() {
			break
		}
		bucket := bucket_for_year(from)
		results = append(results, bucket)

		curYear := from.Year()

		for {
			if from.Year() > curYear {
				break
			}
			from = from.Add(time.Hour * 24 * 365)
		}
	}

	return results
}

func bucketsForMonths(start_ts, end_ts int64) []string {
	results := make([]string, 0)

	from := time.Unix(start_ts, 0)
	to := time.Unix(end_ts, 0)

	for {
		if from.Unix() > to.Unix() {
			break
		}
		bucket := bucket_for_month(from)
		results = append(results, bucket)

		curMonth := from.Month()
		curYear := from.Year()

		for {
			if from.Month() > curMonth || from.Year() > curYear {
				break
			}
			from = from.Add(time.Hour * 24)
		}
	}

	return results
}

func bucketsForDays(start_ts, end_ts int64) []string {
	results := make([]string, 0)

	from := time.Unix(start_ts, 0)
	to := time.Unix(end_ts, 0)

	for {
		if from.Unix() > to.Unix() {
			break
		}
		bucket := bucket_for_day(from)
		results = append(results, bucket)
		from = from.Add(time.Hour * 24)
	}

	return results
}

func bucketsForHours(start_ts, end_ts int64) []string {
	results := make([]string, 0)

	from := time.Unix(start_ts, 0)
	to := time.Unix(end_ts, 0)

	for {
		if from.Unix() > to.Unix() {
			break
		}
		bucket := bucket_for_hour(from)
		results = append(results, bucket)
		from = from.Add(time.Hour)
	}

	return results
}

func bucketsForMins(start_ts, end_ts int64) []string {
	results := make([]string, 0)

	from := time.Unix(start_ts, 0)
	to := time.Unix(end_ts, 0)

	for {
		if from.Unix() > to.Unix() {
			break
		}
		bucket := bucket_for_min(from)
		results = append(results, bucket)
		from = from.Add(time.Minute)
	}

	return results
}

func bucketsForSecs(start_ts, end_ts int64) []string {
	results := make([]string, 0)

	from := time.Unix(start_ts, 0)
	to := time.Unix(end_ts, 0)

	for {
		if from.Unix() > to.Unix() {
			break
		}
		bucket := bucket_for_sec(from)
		results = append(results, bucket)
		from = from.Add(time.Second)
	}

	return results
}

func bucket_for_year(t time.Time) string {
	format := t.Format("2006")
	return fmt.Sprintf("%s", format)
}
func bucket_for_month(t time.Time) string {
	format := t.Format("200601")
	return fmt.Sprintf("%s", format)
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
func bucket_for_sec(t time.Time) string {
	format := t.Format("20060102150405")
	return fmt.Sprintf("%s", format)
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
