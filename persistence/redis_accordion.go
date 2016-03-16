package persistence

import "time"
import "fmt"
import "strconv"
import "errors"
import "pointspaced/psdcontext"
import "github.com/garyburd/redigo/redis"

type RedisACR struct {
	from time.Time
}

type RedisACRRequest struct {
	TimeBucket  string
	Granularity string
	ScoreMin    int
	ScoreMax    int
}

func (self *RedisACRRequest) QueryMin() int64 {
	s := strconv.Itoa(self.ScoreMin)
	f := ""
	if len(s) == 1 {
		f = self.TimeBucket + "0" + s
	} else {
		f = self.TimeBucket + s
	}
	v, err := strconv.ParseInt(f, 10, 64)
	if err != nil {
		panic(err)
	}
	return v
}

func (self *RedisACRRequest) QueryMax() int64 {
	s := strconv.Itoa(self.ScoreMax)
	f := ""
	if len(s) == 1 {
		f = self.TimeBucket + "0" + s
	} else {
		f = self.TimeBucket + s
	}
	v, err := strconv.ParseInt(f, 10, 64)
	if err != nil {
		panic(err)
	}
	return v
}

func NewRedisACRRequest(bucket string, granularity string, scoremin int, scoremax int) RedisACRRequest {
	r := RedisACRRequest{}
	r.TimeBucket = bucket
	r.Granularity = granularity
	r.ScoreMin = scoremin
	r.ScoreMax = scoremax
	return r
}

func (self RedisACR) ReadBuckets(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64, debug string) QueryResponse {

	fmt.Println("[Read] START", start_ts, "END", end_ts)
	requests := self.requestsForRange(start_ts, end_ts)
	fmt.Println("[Read] Requests:", requests)

	r := psdcontext.Ctx.RedisPool.Get()

	sum := int64(0)
	for _, request := range requests {
		key := "psd:"
		key = key + "1" + ":" + "0" + ":" + metric + ":" + request.TimeBucket
		r.Send("ZRANGE", key, 0, -1, "WITHSCORES")
	}

	// trigger pipelined call
	r.Flush()

	for _, request := range requests {
		response, err := redis.Values(r.Receive())
		if err != nil {
			panic(err) // TODO
		}
		for idx, vx := range response {
			if idx%2 == 1 {
				strval := string(vx.([]uint8))

				intval, err := strconv.ParseInt(strval, 10, 64)
				if err != nil {
					panic(err)
				}

				myts, err := strconv.ParseInt(string(response[idx-1].([]uint8)), 10, 64)
				if err != nil {
					panic(err)
				}

				if myts >= request.QueryMin() && myts <= request.QueryMax() {
					sum += intval
					//					fmt.Println("ACCEPT", intval, request.TimeBucket, myts, request.QueryMin(), request.QueryMax())
				} else {
					//					fmt.Println("REJECT", intval, request.TimeBucket, myts, request.QueryMin(), request.QueryMax())
				}
			}
		}
	}

	qr := QueryResponse{}
	qr.UserToSum = map[string]int64{}
	qr.UserToSum["1"] = sum
	r.Close()

	return qr
}

func (self *RedisACR) requestsForRange(start_ts int64, end_ts int64) []RedisACRRequest {
	from := time.Unix(start_ts, 0).UTC()
	to := time.Unix(end_ts, 0).UTC()

	reqs := []RedisACRRequest{}
	cursor := NewRedisACRCursor(from, to)

	fmt.Println("[Read] requestsForRange FROM", from, "TO", to)

	tmp := map[string]RedisACRRequest{}

	// OK our goal is to iterate chunk
	// by chunk using our cursor
	// each loop we will increment our cursor the maximum
	// amount
	//
	// we must
	// 1 - not use a key that excludes the beginning of cursor
	// 2 - not use a key that goes past the end of the window (ie - "to")
	// 3. - not use a key that would be better summarized by using a larger key

	for {
		if cursor.HasReachedEnd() {
			break
		}

		if cursor.CanJumpYear() {
			tmp[cursor.YearKey()] = NewRedisACRRequest(cursor.YearKey(), "year", 1, 12)

		} else if cursor.CanJumpMonth() {

			entry, exists := tmp[cursor.YearKey()]
			if exists {
				if entry.ScoreMin > cursor.Month() {
					entry.ScoreMin = cursor.Month()
				}
				if entry.ScoreMax < cursor.Month() {
					entry.ScoreMax = cursor.Month()
				}
				tmp[cursor.YearKey()] = entry
			} else {
				tmp[cursor.YearKey()] = NewRedisACRRequest(cursor.YearKey(), "month", cursor.Month(), cursor.Month())
			}
			cursor.JumpMonth()

		} else if cursor.CanJumpDay() {

			entry, exists := tmp[cursor.MonthKey()]
			if exists {
				if entry.ScoreMin > cursor.Day() {
					entry.ScoreMin = cursor.Day()
				}
				if entry.ScoreMax < cursor.Day() {
					entry.ScoreMax = cursor.Day()
				}
				tmp[cursor.MonthKey()] = entry
			} else {
				tmp[cursor.MonthKey()] = NewRedisACRRequest(cursor.MonthKey(), "day", cursor.Day(), cursor.Day())
			}

			cursor.JumpDay()

		} else if cursor.CanJumpHour() {

			entry, exists := tmp[cursor.DayKey()]
			if exists {
				if entry.ScoreMin > cursor.Hour() {
					entry.ScoreMin = cursor.Hour()
				}
				if entry.ScoreMax < cursor.Hour() {
					entry.ScoreMax = cursor.Hour()
				}
				tmp[cursor.DayKey()] = entry
			} else {
				tmp[cursor.DayKey()] = NewRedisACRRequest(cursor.DayKey(), "hour", cursor.Hour(), cursor.Hour())
			}

			cursor.JumpHour()

		} else if cursor.CanJumpMinute() {

			entry, exists := tmp[cursor.HourKey()]
			if exists {
				if entry.ScoreMin > cursor.Minute() {
					entry.ScoreMin = cursor.Minute()
				}
				if entry.ScoreMax < cursor.Minute() {
					entry.ScoreMax = cursor.Minute()
				}
				tmp[cursor.HourKey()] = entry
			} else {
				tmp[cursor.HourKey()] = NewRedisACRRequest(cursor.HourKey(), "minute", cursor.Minute(), cursor.Minute())
			}
			cursor.JumpMinute()

		} else if cursor.CanJumpSecond() {

			entry, exists := tmp[cursor.MinuteKey()]
			if exists {
				if entry.ScoreMin > cursor.Second() {
					entry.ScoreMin = cursor.Second()
				}
				if entry.ScoreMax < cursor.Second() {
					entry.ScoreMax = cursor.Second()
				}
				tmp[cursor.MinuteKey()] = entry
			} else {
				tmp[cursor.MinuteKey()] = NewRedisACRRequest(cursor.MinuteKey(), "second", cursor.Second(), cursor.Second())
			}
			cursor.JumpSecond()

		} else {
			entry, exists := tmp[cursor.MinuteKey()]
			if exists {
				if entry.ScoreMin > cursor.Second() {
					entry.ScoreMin = cursor.Second()
				}
				if entry.ScoreMax < cursor.Second() {
					entry.ScoreMax = cursor.Second()
				}
				tmp[cursor.MinuteKey()] = entry
			} else {
				tmp[cursor.MinuteKey()] = NewRedisACRRequest(cursor.MinuteKey(), "second", cursor.Second(), cursor.Second())
			}

			cursor.JumpSecond()
		}
	}

	//								t := proposed_time.Format("20060102150405")
	for _, item := range tmp {
		reqs = append(reqs, item)
	}

	return reqs

}

func (self RedisACR) bucketsForRange(start_ts int64, end_ts int64) map[string][]string {

	buckets := map[string][]string{}

	from := time.Unix(start_ts, 0).UTC()
	to := time.Unix(end_ts, 0).UTC()

	fmt.Println("FROM", from, "TO", to)

	for {
		if from.Unix() >= to.Unix() {
			break
		}

		delta := to.Unix() - from.Unix()
		if delta < 3600 {
			// we must use minutes, cuz we have less than 1 hour

			buckets["minute"] = append(buckets["minute"], from.Format("20060102150405"))
			from = from.Add(time.Minute)

		} else if delta < 86400 {
			// we must use hours, cuz we have less than 1 day
			buckets["hour"] = append(buckets["hour"], from.Format("200601021504"))
			from = from.Add(time.Hour)

		} else if delta < 2678400 {
			buckets["day"] = append(buckets["day"], from.Format("2006010215"))
			from = from.Add(time.Hour * 24)

		} else {
			buckets["month"] = append(buckets["month"], from.Format("20060102"))
			from = from.Add(time.Hour * 24 * 31)
		}
	}

	// MB full months before years
	// Y full years
	// MA months after years
	// DA days after months
	// HA hours after days
	// MA minutes after hours
	// SA seconds after minutes

	return buckets
}

func (self RedisACR) WritePoint(flavor string, userId int64, value int64, activityTypeId int64, timestamp int64) error {

	if (value == 0) ||
		(userId == 0) ||
		(timestamp == 0) ||
		(activityTypeId == 0) ||
		(flavor == "") {
		return errors.New("invalid arguments")
	}
	if timestamp < 1430838227 {
		return errors.New("invalid timestamp")
	}

	buckets, err := self.bucketsForJob(timestamp)

	r := psdcontext.Ctx.RedisPool.Get()

	if err != nil {
		return err
	} else {
		//		fmt.Println("\t[Buckets] ->")
		for idx, bucket := range buckets {
			// figure out when this is

			strUserId := strconv.FormatInt(userId, 10)

			set_timestamp := time.Unix(timestamp, int64(0)).UTC().Format("20060102150405")
			if len(buckets) > idx+1 {
				set_timestamp = buckets[idx+1]
			}

			for _, aType := range []int64{0, activityTypeId} {

				strAType := strconv.FormatInt(aType, 10)

				key := "psd:"
				key = key + strUserId + ":" + strAType + ":" + flavor + ":" + bucket

				//				fmt.Println("\t\t=>", bucket, "ZINCRBY", key, set_timestamp, value)

				// TODO LOCKING
				// KEY SCORE MEMBER
				r.Do("ZINCRBY", key, value, set_timestamp)
			}
		}
	}

	r.Close()
	return nil
}

func (self *RedisACR) bucketsForJob(ts int64) ([]string, error) {

	out := []string{}

	if ts <= 0 {
		return out, errors.New("timestamp was invalid")
	}

	unixtime := time.Unix(ts, int64(0)).UTC()

	// now what buckets will we write too considering this is our UTC ts
	out = append(out, unixtime.Format("2006"))
	out = append(out, unixtime.Format("200601"))
	out = append(out, unixtime.Format("20060102"))
	out = append(out, unixtime.Format("2006010215"))
	out = append(out, unixtime.Format("200601021504"))

	return out, nil
}
