package persistence

import "time"
import "fmt"
import "strconv"
import "errors"
import "pointspaced/psdcontext"
import "github.com/garyburd/redigo/redis"

type RedisMZCursor struct {
	Cursor time.Time
	To     time.Time
}

func NewRedisMZCursor(from time.Time, to time.Time) RedisMZCursor {
	c := RedisMZCursor{}
	c.Cursor = from
	c.To = to
	return c

}

type RedisMZ struct {
	from time.Time
}

type RedisMZRequest struct {
	TimeBucket  string
	Granularity string
	ScoreMin    int
	ScoreMax    int
}

func NewRedisMZRequest(bucket string, granularity string, scoremin int, scoremax int) RedisMZRequest {
	r := RedisMZRequest{}
	r.TimeBucket = bucket
	r.Granularity = granularity
	r.ScoreMin = scoremin
	r.ScoreMax = scoremax
	return r
}

func (self RedisMZ) ReadBuckets(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64, debug string) QueryResponse {

	fmt.Println("[Read] START", start_ts, "END", end_ts)
	requests := self.requestsForRange(start_ts, end_ts)
	fmt.Println("[Read] Requests:", requests)

	buckets := self.bucketsForRange(start_ts, end_ts)
	fmt.Println("[Read] Buckets Are ", buckets)

	r := psdcontext.Ctx.RedisPool.Get()

	sts := time.Unix(start_ts, 0).UTC()
	ste := time.Unix(end_ts, 0).UTC()
	rmins := map[string]int{}
	rmins["minute"], _ = strconv.Atoi(sts.Format("20060102150405"))
	rmins["hour"], _ = strconv.Atoi(sts.Format("200601021504"))
	rmins["day"], _ = strconv.Atoi(sts.Format("2006010215"))
	rmins["month"], _ = strconv.Atoi(sts.Format("20060102"))
	rmaxs := map[string]int{}
	rmaxs["minute"], _ = strconv.Atoi(ste.Format("20060102150405"))
	rmaxs["hour"], _ = strconv.Atoi(ste.Format("200601021504"))
	rmaxs["day"], _ = strconv.Atoi(ste.Format("2006010215"))
	rmaxs["month"], _ = strconv.Atoi(ste.Format("20060102"))

	sum := int64(0)
	for segment, vals := range buckets {

		fetchme := map[string]int{}
		for _, val := range vals {
			bucket := val[0 : len(val)-2]
			fetchme[bucket] = 1
		}

		for bucket, _ := range fetchme {
			key := "psd:"
			key = key + "1" + ":" + "0" + ":" + metric + ":" + bucket
			r.Send("ZRANGE", key, 0, -1, "WITHSCORES")
		}

		r.Flush()

		for bucket, _ := range fetchme {
			response, err := redis.Values(r.Receive())
			if err != nil {
				panic(err)
			}
			for idx, vx := range response {
				if idx%2 == 1 {
					strval := string(vx.([]uint8))
					intval, _ := strconv.Atoi(strval)
					myts, _ := strconv.Atoi(string(response[idx-1].([]uint8)))
					thev := int64(intval)

					if myts >= rmins[segment] && myts <= rmaxs[segment] {
						sum += thev
						fmt.Println("ACCEPT", thev, bucket, myts, rmins[segment], rmaxs[segment])
					} else {
						fmt.Println("REJECT", thev, bucket, myts, rmins[segment], rmaxs[segment])
					}
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

func (self *RedisMZCursor) CanJumpYear() bool {
	_, M, D := self.Cursor.Date()
	Hour := self.Cursor.Hour()
	Min := self.Cursor.Minute()
	Sec := self.Cursor.Second()
	if (M == 1) && (D == 1) && (Hour == 0) && (Min == 0) && (Sec == 0) {
		proposed_jump_time := self.Cursor.AddDate(1, 0, 0)
		if proposed_jump_time.Unix() > self.To.Unix() {
			return false
		}
		return true
	}
	return false
}

func (self *RedisMZCursor) CanJumpMonth() bool {
	D := self.Cursor.Day()
	Hour := self.Cursor.Hour()
	Min := self.Cursor.Minute()
	Sec := self.Cursor.Second()
	if (D == 1) && (Hour == 0) && (Min == 0) && (Sec == 0) {
		proposed_jump_time := self.Cursor.AddDate(0, 1, 0)
		if proposed_jump_time.Unix() > self.To.Unix() {
			return false
		}
		return true
	}
	return false
}

func (self *RedisMZCursor) CanJumpDay() bool {
	Hour := self.Cursor.Hour()
	Min := self.Cursor.Minute()
	Sec := self.Cursor.Second()
	if (Hour == 0) && (Min == 0) && (Sec == 0) {
		proposed_jump_time := self.Cursor.AddDate(0, 0, 1)
		if proposed_jump_time.Unix() > self.To.Unix() {
			return false
		}
		return true
	}
	return false
}

func (self *RedisMZCursor) CanJumpHour() bool {
	Min := self.Cursor.Minute()
	Sec := self.Cursor.Second()
	if (Min == 0) && (Sec == 0) {
		proposed_jump_time := self.Cursor.Add(time.Hour)
		if proposed_jump_time.Unix() > self.To.Unix() {
			return false
		}
		return true
	}
	return false
}

func (self *RedisMZCursor) CanJumpMinute() bool {
	Sec := self.Cursor.Second()
	if Sec == 0 {
		proposed_jump_time := self.Cursor.Add(time.Minute)
		if proposed_jump_time.Unix() > self.To.Unix() {
			return false
		}
		return true
	}
	return false
}

func (self *RedisMZCursor) CanJumpSecond() bool {
	proposed_jump_time := self.Cursor.Add(time.Second)
	if proposed_jump_time.Unix() > self.To.Unix() {
		return false
	}
	return true
}

func (self *RedisMZCursor) YearKey() string {
	return self.Cursor.Format("2006")
}
func (self *RedisMZCursor) MonthKey() string {
	return self.Cursor.Format("200601")
}
func (self *RedisMZCursor) DayKey() string {
	return self.Cursor.Format("20060102")
}
func (self *RedisMZCursor) HourKey() string {
	return self.Cursor.Format("2006010215")
}
func (self *RedisMZCursor) MinuteKey() string {
	return self.Cursor.Format("200601021504")
}
func (self *RedisMZCursor) SecondKey() string {
	return self.Cursor.Format("20060102150405")
}
func (self *RedisMZCursor) Year() int {
	return self.Cursor.Year()
}
func (self *RedisMZCursor) Month() int {
	return int(self.Cursor.Month())
}
func (self *RedisMZCursor) Day() int {
	return self.Cursor.Day()
}
func (self *RedisMZCursor) Hour() int {
	return self.Cursor.Hour()
}
func (self *RedisMZCursor) Minute() int {
	return self.Cursor.Minute()
}
func (self *RedisMZCursor) Second() int {
	return self.Cursor.Second()
}
func (self *RedisMZCursor) HasReachedEnd() bool {
	if self.Cursor.Unix() >= self.To.Unix() {
		return true
	}
	return false
}
func (self *RedisMZCursor) JumpYear() {
	self.Cursor = self.Cursor.AddDate(1, 0, 0)
}
func (self *RedisMZCursor) JumpMonth() {
	self.Cursor = self.Cursor.AddDate(0, 1, 0)
}
func (self *RedisMZCursor) JumpDay() {
	self.Cursor = self.Cursor.AddDate(0, 0, 1)
}
func (self *RedisMZCursor) JumpHour() {
	self.Cursor = self.Cursor.Add(time.Hour)
}
func (self *RedisMZCursor) JumpMinute() {
	self.Cursor = self.Cursor.Add(time.Minute)
}
func (self *RedisMZCursor) JumpSecond() {
	self.Cursor = self.Cursor.Add(time.Second)
}

func (self *RedisMZ) requestsForRange(start_ts int64, end_ts int64) []RedisMZRequest {
	from := time.Unix(start_ts, 0).UTC()
	to := time.Unix(end_ts, 0).UTC()

	reqs := []RedisMZRequest{}
	cursor := NewRedisMZCursor(from, to)

	fmt.Println("[Read] requestsForRange FROM", from, "TO", to)

	tmp := map[string]RedisMZRequest{}

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
			tmp[cursor.YearKey()] = NewRedisMZRequest(cursor.YearKey(), "year", 1, 12)

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
				tmp[cursor.YearKey()] = NewRedisMZRequest(cursor.YearKey(), "month", cursor.Month(), cursor.Month())
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
				tmp[cursor.MonthKey()] = NewRedisMZRequest(cursor.MonthKey(), "day", cursor.Day(), cursor.Day())
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
				tmp[cursor.DayKey()] = NewRedisMZRequest(cursor.DayKey(), "hour", cursor.Hour(), cursor.Hour())
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
				tmp[cursor.HourKey()] = NewRedisMZRequest(cursor.HourKey(), "minute", cursor.Minute(), cursor.Minute())
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
				tmp[cursor.MinuteKey()] = NewRedisMZRequest(cursor.MinuteKey(), "second", cursor.Second(), cursor.Second())
			}
			cursor.JumpSecond()
		} else {
			//?
		}
	}

	//								t := proposed_time.Format("20060102150405")
	for _, item := range tmp {
		reqs = append(reqs, item)
	}

	return reqs

}

func (self RedisMZ) bucketsForRange(start_ts int64, end_ts int64) map[string][]string {

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

func (self RedisMZ) WritePoint(flavor string, userId int64, value int64, activityTypeId int64, timestamp int64) error {

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
		fmt.Println("\t[Buckets] ->")
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

				fmt.Println("\t\t=>", bucket, "ZINCRBY", key, set_timestamp, value)

				// TODO LOCKING
				// KEY SCORE MEMBER
				r.Do("ZINCRBY", key, value, set_timestamp)
			}
		}
	}

	r.Close()
	return nil
}

func (self *RedisMZ) bucketsForJob(ts int64) ([]string, error) {

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
