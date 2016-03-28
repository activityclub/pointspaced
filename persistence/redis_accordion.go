package persistence

import "time"
import "sync"
import "strconv"
import "errors"
import "pointspaced/psdcontext"
import "github.com/garyburd/redigo/redis"

type RedisACR struct{}

type RedisACRRequest struct {
	TimeBucket string
	ScoreMin   int
	ScoreMax   int
}

func (self RedisACRRequest) QueryMin() int {
	var f string
	if self.ScoreMin < 10 {
		f = self.TimeBucket + "0" + strconv.Itoa(self.ScoreMin)
	} else {
		f = self.TimeBucket + strconv.Itoa(self.ScoreMin)
	}
	v, err := strconv.Atoi(f)
	//v, err := strconv.ParseInt(f, 10, 64)
	if err != nil {
		panic(err)
	}
	return v
}

func (self RedisACRRequest) QueryMax() int {
	s := strconv.Itoa(self.ScoreMax)
	var f string
	if len(s) == 1 {
		f = self.TimeBucket + "0" + s
	} else {
		f = self.TimeBucket + s
	}
	v, err := strconv.Atoi(f)
	//v, err := strconv.ParseInt(f, 10, 64)
	if err != nil {
		panic(err)
	}
	return v
}

func NewRedisACRRequest(bucket string, scoremin int, scoremax int) RedisACRRequest {
	r := RedisACRRequest{}
	r.TimeBucket = bucket
	r.ScoreMin = scoremin
	r.ScoreMax = scoremax
	return r
}

func (self RedisACR) ReadBuckets(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64, debug string) QueryResponse {

	//fmt.Println("[Accordion] START", start_ts, "END", end_ts)
	requests := self.requestsForRange(start_ts, end_ts)
	//fmt.Println("[Accordion] Requests:", requests)

	qr := QueryResponse{}
	qr.UserToSum = make(map[int64]int64, len(uids))

	var wg sync.WaitGroup
	wg.Add(len(uids))

	results := make(chan map[int64]int64)

	for _, uid := range uids {

		go func(uid int64) {

			struid := strconv.FormatInt(uid, 10)

			r := psdcontext.Ctx.RedisPool.Get()
			sum := int64(0)
			keys := make([]string, len(requests))
			idx := 0
			for k, request := range requests {
				keys[idx] = k
				key := "acr:" + struid + ":0:" + metric + ":" + request.TimeBucket
				r.Send("ZRANGE", key, 0, -1, "WITHSCORES")
				idx += 1
			}
			r.Flush()

			for _, k := range keys {
				request := requests[k]
				response, err := redis.Values(r.Receive())
				if err != nil {
					panic(err) // TODO
				}

				for idx, vx := range response {
					if idx%2 == 1 {
						myts, err := strconv.Atoi(string(response[idx-1].([]uint8)))
						//myts, err := strconv.ParseInt(string(response[idx-1].([]uint8)), 10, 64)
						if err != nil {
							panic(err)
						}
						if myts >= request.QueryMin() && myts <= request.QueryMax() {
							intval, err := strconv.ParseInt(string(vx.([]uint8)), 10, 64)
							if err != nil {
								panic(err)
							}
							sum += intval
						}
					}
				}
			}

			entry := map[int64]int64{}
			entry[uid] = sum

			r.Close()

			results <- entry

		}(uid)

	}

	go func() {
		for entry := range results {
			for k, v := range entry {
				qr.UserToSum[k] = v
			}
			wg.Done()
		}
	}()

	wg.Wait()

	return qr
}

func (self RedisACR) requestsForRange(start_ts int64, end_ts int64) map[string]RedisACRRequest {
	from := time.Unix(start_ts, 0).UTC()
	to := time.Unix(end_ts, 0).UTC()

	cursor := NewRedisACRCursor(from, to)

	//	fmt.Println("[Accordion] requestsForRange FROM", from, "TO", to)

	reqs := map[string]RedisACRRequest{}

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

		//		fmt.Println("HRM", cursor.Cursor, "TO", cursor.To)

		if cursor.CanJumpYear() {
			reqs[cursor.YearKey()] = NewRedisACRRequest(cursor.YearKey(), 1, 12)
			cursor.JumpYear()

		} else if cursor.CanJumpMonth() {

			entry, exists := reqs[cursor.YearKey()]
			if exists {
				if entry.ScoreMin > cursor.Month() {
					entry.ScoreMin = cursor.Month()
				}
				if entry.ScoreMax < cursor.Month() {
					entry.ScoreMax = cursor.Month()
				}
				reqs[cursor.YearKey()] = entry
			} else {
				reqs[cursor.YearKey()] = NewRedisACRRequest(cursor.YearKey(), cursor.Month(), cursor.Month())
			}
			cursor.JumpMonth()

		} else if cursor.CanJumpDay() {

			entry, exists := reqs[cursor.MonthKey()]
			if exists {
				if entry.ScoreMin > cursor.Day() {
					entry.ScoreMin = cursor.Day()
				}
				if entry.ScoreMax < cursor.Day() {
					entry.ScoreMax = cursor.Day()
				}
				reqs[cursor.MonthKey()] = entry
			} else {
				reqs[cursor.MonthKey()] = NewRedisACRRequest(cursor.MonthKey(), cursor.Day(), cursor.Day())
			}

			cursor.JumpDay()

		} else if cursor.CanJumpHour() {

			entry, exists := reqs[cursor.DayKey()]
			if exists {
				if entry.ScoreMin > cursor.Hour() {
					entry.ScoreMin = cursor.Hour()
				}
				if entry.ScoreMax < cursor.Hour() {
					entry.ScoreMax = cursor.Hour()
				}
				reqs[cursor.DayKey()] = entry
			} else {
				reqs[cursor.DayKey()] = NewRedisACRRequest(cursor.DayKey(), cursor.Hour(), cursor.Hour())
			}

			cursor.JumpHour()

		} else if cursor.CanJumpMinute() {

			entry, exists := reqs[cursor.HourKey()]
			if exists {
				if entry.ScoreMin > cursor.Minute() {
					entry.ScoreMin = cursor.Minute()
				}
				if entry.ScoreMax < cursor.Minute() {
					entry.ScoreMax = cursor.Minute()
				}
				reqs[cursor.HourKey()] = entry
			} else {
				reqs[cursor.HourKey()] = NewRedisACRRequest(cursor.HourKey(), cursor.Minute(), cursor.Minute())
			}
			cursor.JumpMinute()

		} else if cursor.CanJumpSecond() {

			entry, exists := reqs[cursor.MinuteKey()]
			if exists {
				if entry.ScoreMin > cursor.Second() {
					entry.ScoreMin = cursor.Second()
				}
				if entry.ScoreMax < cursor.Second() {
					entry.ScoreMax = cursor.Second()
				}
				reqs[cursor.MinuteKey()] = entry
			} else {
				reqs[cursor.MinuteKey()] = NewRedisACRRequest(cursor.MinuteKey(), cursor.Second(), cursor.Second())
			}
			cursor.JumpSecond()

		} else {
			entry, exists := reqs[cursor.MinuteKey()]
			if exists {
				if entry.ScoreMin > cursor.Second() {
					entry.ScoreMin = cursor.Second()
				}
				if entry.ScoreMax < cursor.Second() {
					entry.ScoreMax = cursor.Second()
				}
				reqs[cursor.MinuteKey()] = entry
			} else {
				reqs[cursor.MinuteKey()] = NewRedisACRRequest(cursor.MinuteKey(), cursor.Second(), cursor.Second())
			}

			cursor.JumpSecond()
		}
	}

	return reqs

}

func (self RedisACR) WritePoint(flavor string, userId int64, value int64, activityTypeId int64, timestamp int64) error {

	if (value == 0) ||
		(userId == 0) ||
		(timestamp == 0) ||
		(activityTypeId == 0) ||
		(flavor == "") {
		return errors.New("invalid arguments")
	}

	strUserId := strconv.FormatInt(userId, 10)

	buckets, err := self.bucketsForJob(timestamp)

	r := psdcontext.Ctx.RedisPool.Get()

	if err != nil {
		return err
	} else {
		//		fmt.Println("\t[Buckets] ->")
		for idx, bucket := range buckets {
			// figure out when this is

			set_timestamp := time.Unix(timestamp, int64(0)).UTC().Format("20060102150405")
			if len(buckets) > idx+1 {
				set_timestamp = buckets[idx+1]
			}

			for _, aType := range []int64{0, activityTypeId} {

				strAType := strconv.FormatInt(aType, 10)

				key := "acr:" + strUserId + ":" + strAType + ":" + flavor + ":" + bucket

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
