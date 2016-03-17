package persistence

import "time"
import "strconv"
import "errors"
import "pointspaced/psdcontext"
import "github.com/garyburd/redigo/redis"

type RedisHZ struct {
}

type RedisHZRequest struct {
	TimeBucket string
	ScoreMin   int
	ScoreMax   int
}

func (self RedisHZRequest) QueryMin() string {
	var f string
	if self.ScoreMin < 10 {
		f = self.TimeBucket + "0" + strconv.Itoa(self.ScoreMin)
	} else {
		f = self.TimeBucket + strconv.Itoa(self.ScoreMin)
	}
	return f
}

func (self RedisHZRequest) QueryMax() string {
	s := strconv.Itoa(self.ScoreMax)
	var f string
	if len(s) == 1 {
		f = self.TimeBucket + "0" + s
	} else {
		f = self.TimeBucket + s
	}
	return f
}

func NewRedisHZRequest(bucket string, scoremin int, scoremax int) RedisHZRequest {
	r := RedisHZRequest{}
	r.TimeBucket = bucket
	r.ScoreMin = scoremin
	r.ScoreMax = scoremax
	return r
}

func (self RedisHZ) ReadBuckets(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64, debug string) QueryResponse {

	//fmt.Println("[Hashzilla] START", start_ts, "END", end_ts)
	requests := self.requestsForRange(start_ts, end_ts)
	//fmt.Println("[Hashzilla] Requests:", requests)

	r := psdcontext.Ctx.RedisPool.Get()
	defer r.Close()

	rlen := len(requests)
	cmd := make([]interface{}, (rlen*3)+1)
	cmd[0] = rlen
	aidx := rlen + 1
	idx := 1

	for _, request := range requests {
		key := "hz:1:0:" + metric + ":" + request.TimeBucket

		cmd[idx] = key
		idx += 1

		cmd[aidx] = request.QueryMin()
		aidx += 1
		cmd[aidx] = request.QueryMax()
		aidx += 1
	}

	response, err := redis.Int64(psdcontext.Ctx.AgScript.Do(r, cmd...))
	if err != nil {
		panic(err)
	}

	qr := QueryResponse{}
	qr.UserToSum = map[string]int64{"1": response}

	return qr
}

func (self RedisHZ) requestsForRange(start_ts int64, end_ts int64) map[string]RedisHZRequest {
	from := time.Unix(start_ts, 0).UTC()
	to := time.Unix(end_ts, 0).UTC()

	cursor := NewRedisACRCursor(from, to)

	//fmt.Println("[Hashzilla] requestsForRange FROM", from, "TO", to)

	reqs := map[string]RedisHZRequest{}

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
			reqs[cursor.YearKey()] = NewRedisHZRequest(cursor.YearKey(), 1, 12)
			cursor.JumpYear()

		} else if cursor.CanJumpMonth() {

			entry, exists := reqs[cursor.YearKey()]
			if exists {
				ch := false
				if entry.ScoreMin > cursor.Month() {
					ch = true
					entry.ScoreMin = cursor.Month()
				}
				if entry.ScoreMax < cursor.Month() {
					ch = true
					entry.ScoreMax = cursor.Month()
				}
				if ch {
					reqs[cursor.YearKey()] = entry
				}
			} else {
				reqs[cursor.YearKey()] = NewRedisHZRequest(cursor.YearKey(), cursor.Month(), cursor.Month())
			}
			cursor.JumpMonth()

		} else if cursor.CanJumpDay() {

			entry, exists := reqs[cursor.MonthKey()]
			if exists {
				ch := false
				if entry.ScoreMin > cursor.Day() {
					ch = true
					entry.ScoreMin = cursor.Day()
				}
				if entry.ScoreMax < cursor.Day() {
					ch = true
					entry.ScoreMax = cursor.Day()
				}
				if ch {
					reqs[cursor.MonthKey()] = entry
				}
			} else {
				reqs[cursor.MonthKey()] = NewRedisHZRequest(cursor.MonthKey(), cursor.Day(), cursor.Day())
			}

			cursor.JumpDay()

		} else if cursor.CanJumpHour() {

			entry, exists := reqs[cursor.DayKey()]
			if exists {
				ch := false
				if entry.ScoreMin > cursor.Hour() {
					entry.ScoreMin = cursor.Hour()
					ch = true
				}
				if entry.ScoreMax < cursor.Hour() {
					entry.ScoreMax = cursor.Hour()
					ch = true
				}
				if ch {
					reqs[cursor.DayKey()] = entry
				}
			} else {
				reqs[cursor.DayKey()] = NewRedisHZRequest(cursor.DayKey(), cursor.Hour(), cursor.Hour())
			}

			cursor.JumpHour()

		} else if cursor.CanJumpMinute() {

			entry, exists := reqs[cursor.HourKey()]
			if exists {
				ch := false
				if entry.ScoreMin > cursor.Minute() {
					ch = true
					entry.ScoreMin = cursor.Minute()
				}
				if entry.ScoreMax < cursor.Minute() {
					ch = true
					entry.ScoreMax = cursor.Minute()
				}
				if ch {
					reqs[cursor.HourKey()] = entry
				}
			} else {
				reqs[cursor.HourKey()] = NewRedisHZRequest(cursor.HourKey(), cursor.Minute(), cursor.Minute())
			}
			cursor.JumpMinute()

		} else if cursor.CanJumpSecond() {

			entry, exists := reqs[cursor.MinuteKey()]
			if exists {
				ch := false
				if entry.ScoreMin > cursor.Second() {
					ch = true
					entry.ScoreMin = cursor.Second()
				}
				if entry.ScoreMax < cursor.Second() {
					ch = true
					entry.ScoreMax = cursor.Second()
				}
				if ch {
					reqs[cursor.MinuteKey()] = entry
				}
			} else {
				reqs[cursor.MinuteKey()] = NewRedisHZRequest(cursor.MinuteKey(), cursor.Second(), cursor.Second())
			}
			cursor.JumpSecond()

		} else {
			entry, exists := reqs[cursor.MinuteKey()]
			if exists {
				ch := false
				if entry.ScoreMin > cursor.Second() {
					ch = true
					entry.ScoreMin = cursor.Second()
				}
				if entry.ScoreMax < cursor.Second() {
					ch = true
					entry.ScoreMax = cursor.Second()
				}
				if ch {
					reqs[cursor.MinuteKey()] = entry
				}
			} else {
				reqs[cursor.MinuteKey()] = NewRedisHZRequest(cursor.MinuteKey(), cursor.Second(), cursor.Second())
			}

			cursor.JumpSecond()
		}
	}

	return reqs

}

func (self RedisHZ) WritePoint(flavor string, userId int64, value int64, activityTypeId int64, timestamp int64) error {

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

				key := "hz:" + strUserId + ":" + strAType + ":" + flavor + ":" + bucket

				// TODO LOCKING
				r.Do("HINCRBY", key, set_timestamp, value)
			}
		}
	}

	r.Close()
	return nil
}

func (self *RedisHZ) bucketsForJob(ts int64) ([]string, error) {

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
