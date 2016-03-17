package persistence

import "time"

//import "fmt"
import "strconv"
import "errors"
import "pointspaced/psdcontext"
import "github.com/garyburd/redigo/redis"

type RedisHZ struct {
	AgScript *redis.Script
}

func NewRedisHZ() RedisHZ {
	r := RedisHZ{}
	rx := psdcontext.Ctx.RedisPool.Get()
	/*
			r.AgScript = redis.NewScript(1, `local sum = 0
		local cscore
		for i, v in ipairs(redis.call('HGETALL', KEYS[1])) do
		  if i % 2 == 1 then
		    cscore = v
		  else
		    if cscore >= ARGV[1]  and
		       cscore <= ARGV[2] then
		        sum = sum + v
		      end
		  end
		end
		return sum`)
	*/

	r.AgScript = redis.NewScript(-1, `local sum = 0
local pos = 1
for _, key in ipairs(KEYS) do
  local bulk = redis.call('HGETALL', key)
  local result = {}
  local cscore
  local offset_a = pos
  local offset_b = pos+1
  for i, v in ipairs(bulk) do
    if i % 2 == 1 then
      cscore = v
    else
      if cscore >= ARGV[offset_a] and cscore <= ARGV[offset_b] then
        sum = sum + v
      end
    end
  end
  pos = pos +  2
end
return sum`)

	err := r.AgScript.Load(rx)
	if err != nil {
		panic(err)
	}
	rx.Close()
	return r
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
	/*
		v, err := strconv.Atoi(f)
		//v, err := strconv.ParseInt(f, 10, 64)
		if err != nil {
			panic(err)
		}
		return v
	*/
}

func (self RedisHZRequest) QueryMin2() string {
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
	/*
		v, err := strconv.Atoi(f)
		//v, err := strconv.ParseInt(f, 10, 64)
		if err != nil {
			panic(err)
		}
		return v
	*/
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

	sum := int64(0)
	//	keys := make([]string, len(requests))
	//	flavs := make([]int, len(requests))
	//	idx := 0

	cmd := []interface{}{strconv.Itoa(len(requests))}
	args := []interface{}{}

	for _, request := range requests {
		//		keys[idx] = k
		key := "hz:1:0:" + metric + ":" + request.TimeBucket

		cmd = append(cmd, key)
		args = append(args, request.QueryMin(), request.QueryMax())
		//		args = append(args, request.QueryMax())

		/*

			// can we optimize our gets?
			delta := request.QueryMax() - request.QueryMin()
			//fmt.Println("[DELTA]", delta, request.TimeBucket, request.QueryMax(), request.QueryMin())
			if delta+1 < 6 {
				flavs[idx] = 1

				fields := make([]interface{}, delta+2)
				fields[0] = key
				for {
					if delta < 0 {
						break
					}
					fields[delta+1] = request.QueryMin() + delta
					delta -= 1
				}

				if len(fields) > 1 {
					r.Send("HMGET", fields...)
				} else {
					flavs[idx] = 3
				}
			} else {
				flavs[idx] = 2
				r.Send("HGETALL", key)
			}
		*/

		// In a function, use the script Do method to evaluate the script. The Do
		// method optimistically uses the EVALSHA command. If the script is not
		// loaded, then the Do method falls back to the EVAL command.
		//reply, err = getScript.Do(c, "foo")

		//	self.AgScript.SendHash(r, key, request.QueryMin(), request.QueryMax())

		//idx += 1
	}
	//r.Flush()

	//	for _, a := range args {
	cmd = append(cmd, args...)
	//	}

	self.AgScript.SendHash(r, cmd...)
	r.Flush()
	response, err := redis.Int64(r.Receive())
	if err != nil {
		panic(err)
	}
	sum += response

	//for _, _ = range requests {

	//if flavs[kidx] == 3 {
	//	continue
	//}

	//request := requests[k]
	//		response, err := redis.Int64(r.Receive())
	//	if err != nil {
	//	panic(err) // TODO
	//	}
	/*
		if flavs[kidx] == 1 {
			// handle hmget response
			for _, vx := range response {
				if vx != nil {

					intval, err := strconv.ParseInt(string(vx.([]uint8)), 10, 64)
					if err != nil {
						panic(err)
					}
					sum += intval
				}
			}

			// handle hgetall resp
		} else {

			for idx, vx := range response {
				//fmt.Println(idx, vx)
				if idx%2 == 1 {
					myts, err := strconv.Atoi(string(response[idx-1].([]uint8)))
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
	*/
	//		sum += response

	//	}

	qr := QueryResponse{}
	qr.UserToSum = map[string]int64{}
	qr.UserToSum["1"] = sum
	r.Close()

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
				if entry.ScoreMin > cursor.Month() {
					entry.ScoreMin = cursor.Month()
				}
				if entry.ScoreMax < cursor.Month() {
					entry.ScoreMax = cursor.Month()
				}
				reqs[cursor.YearKey()] = entry
			} else {
				reqs[cursor.YearKey()] = NewRedisHZRequest(cursor.YearKey(), cursor.Month(), cursor.Month())
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
				reqs[cursor.MonthKey()] = NewRedisHZRequest(cursor.MonthKey(), cursor.Day(), cursor.Day())
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
				reqs[cursor.DayKey()] = NewRedisHZRequest(cursor.DayKey(), cursor.Hour(), cursor.Hour())
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
				reqs[cursor.HourKey()] = NewRedisHZRequest(cursor.HourKey(), cursor.Minute(), cursor.Minute())
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
				reqs[cursor.MinuteKey()] = NewRedisHZRequest(cursor.MinuteKey(), cursor.Second(), cursor.Second())
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
