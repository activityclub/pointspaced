package persistence

import "time"
import "strconv"
import "errors"
import "sync"
import "pointspaced/psdcontext"
import "github.com/garyburd/redigo/redis"
import "github.com/ugorji/go/codec"
import "fmt"

//import "strings"

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

func oldGetMyValues(uid int64, results *chan map[int64]int64, requests *map[string]RedisHZRequest, metric string) {
	r := psdcontext.Ctx.RedisPool.Get()
	defer r.Close()

	cmd := []interface{}{}

	cmd = append(cmd, 0)

	struid := strconv.FormatInt(uid, 10)

	for _, request := range *requests {
		// hz:device:tz:user_id:g:activity_id:service:thing:time
		key := "hz:0:0:" + struid + ":0:0:0:" + metric + ":" + request.TimeBucket
		item := []interface{}{}

		qmin, _ := strconv.Atoi(request.QueryMin())
		qmax, _ := strconv.Atoi(request.QueryMax())
		item = append(item, key, qmin, qmax)
		var b []byte = make([]byte, 0, 64)
		var h codec.Handle = new(codec.MsgpackHandle)
		var enc *codec.Encoder = codec.NewEncoderBytes(&b, h)
		var err error = enc.Encode(item)
		if err != nil {
			panic(err)
		}
		cmd = append(cmd, b)
	}

	response, err := redis.Int64(psdcontext.Ctx.AgScript.Do(r, cmd...))
	if err != nil {
		panic(err)
	}

	entry := map[int64]int64{}
	entry[uid] = response
	*results <- entry
}

func getMyValues(key string, results *chan map[string]int64, requests *map[string]RedisHZRequest) {
	r := psdcontext.Ctx.RedisPool.Get()
	defer r.Close()
	cmd := []interface{}{}
	cmd = append(cmd, 0)

	entry := map[string]int64{}

	for _, request := range *requests {
		// hz:device:tz:user_id:g:activity_id:service:thing:time
		fullKey := fmt.Sprintf("%s:%s", key, request.TimeBucket)
		item := []interface{}{}

		qmin, _ := strconv.Atoi(request.QueryMin())
		qmax, _ := strconv.Atoi(request.QueryMax())
		item = append(item, fullKey, qmin, qmax)
		var b []byte = make([]byte, 0, 64)
		var h codec.Handle = new(codec.MsgpackHandle)
		var enc *codec.Encoder = codec.NewEncoderBytes(&b, h)
		var err error = enc.Encode(item)
		if err != nil {
			panic(err)
		}
		cmd = append(cmd, b)
	}

	response, err := redis.Int64(psdcontext.Ctx.AgScript.Do(r, cmd...))
	if err != nil {
		panic(err)
	}
	entry[key] = response

	*results <- entry
}

func (self RedisHZ) QueryBuckets(uid, thing, aid string, start_ts int64, end_ts int64) XQueryResponse {
	qr := XQueryResponse{}
	qr.XToSum = make(map[string]int64)
	return qr
}

func foo() {
	/*
		requests := self.requestsForRange(start_ts, end_ts)
		qr := XQueryResponse{}
		qr.XToSum = make(map[string]int64)

		results := make(chan map[string]int64)

		var wg sync.WaitGroup

		dids := []string{"0"}
		if len(opts["dids"]) > 0 {
			dids = opts["dids"]
		}
		tzs := []string{"0"}
		if len(opts["tzs"]) > 0 {
			tzs = opts["tzs"]
		}
		uids := []string{"0"}
		if len(opts["uids"]) > 0 {
			uids = opts["uids"]
		}
		gids := []string{"0"}
		if len(opts["gids"]) > 0 {
			gids = opts["gids"]
		}
		aids := []string{"0"}
		if len(opts["aids"]) > 0 {
			aids = opts["aids"]
		}
		sids := []string{"0"}
		if len(opts["sids"]) > 0 {
			sids = opts["sids"]
		}

		for _, did := range dids {
			for _, tz := range tzs {
				for _, uid := range uids {
					for _, gid := range gids {
						for _, aid := range aids {
							for _, sid := range sids {
								key := fmt.Sprintf("hz:%s:%s:%s:%s:%s:%s:%s", did, tz, uid, gid, aid, sid, thing)
								wg.Add(1)
								go getMyValues(key, &results, &requests)
							}
						}
					}
				}
			}
		}

		index := 1
		if group == "tzs" {
			index = 2
		} else if group == "uids" {
			index = 3
		} else if group == "gids" {
			index = 4
		} else if group == "aids" {
			index = 5
		} else if group == "sids" {
			index = 6
		}
		go func() {
			for entry := range results {
				for k, v := range entry {
					// hz:0:3600:2:0:0:0:steps
					if group == "" {
						qr.XToSum["0"] += v
					} else {
						tokens := strings.Split(k, ":")
						qr.XToSum[tokens[index]] += v
					}
				}
				wg.Done()
			}
		}()

		wg.Wait()

		return qr
	*/
}

func (self RedisHZ) ReadBuckets(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64) QueryResponse {

	//fmt.Println("[Hashzilla] START", start_ts, "END", end_ts)
	requests := self.requestsForRange(start_ts, end_ts)
	//fmt.Println("[Hashzilla] Requests:", requests)

	qr := QueryResponse{}
	qr.UserToSum = make(map[string]int64, len(uids))

	var wg sync.WaitGroup
	wg.Add(len(uids))

	results := make(chan map[int64]int64)

	for _, uid := range uids {
		go oldGetMyValues(uid, &results, &requests, metric)
	}

	go func() {
		for entry := range results {
			for k, v := range entry {
				qr.UserToSum[fmt.Sprintf("%d", k)] = v
			}
			wg.Done()
		}
	}()

	wg.Wait()

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

func (self RedisHZ) OldWritePoint(thing string, userId, value, activityId, ts int64) error {
	opts := make(map[string]string)
	opts["thing"] = thing
	opts["tz"] = ""
	opts["uid"] = fmt.Sprintf("%d", userId)
	opts["aid"] = fmt.Sprintf("%d", activityId)
	opts["value"] = fmt.Sprintf("%d", value)
	opts["ts"] = fmt.Sprintf("%d", ts)
	opts["sid"] = ""
	opts["did"] = ""
	opts["gid"] = ""
	return self.WritePoint(opts)
}

func (self RedisHZ) WritePoint(opts map[string]string) error {
	tz := opts["tz"]
	thing := opts["thing"]
	sid := opts["sid"]
	did := opts["did"]
	gid := opts["gid"]
	uid := opts["uid"]
	aid := opts["aid"]
	value := opts["value"]
	ts := opts["ts"]
	tsi, _ := strconv.ParseInt(ts, 10, 64)

	if (value == "") ||
		(ts == "") ||
		(thing == "") {
		return errors.New("invalid arguments")
	}

	buckets, err := self.bucketsForJob(tsi)

	r := psdcontext.Ctx.RedisPool.Get()

	if err != nil {
		return err
	} else {
		//		fmt.Println("\t[Buckets] ->")
		for idx, bucket := range buckets {
			// figure out when this is

			set_timestamp := time.Unix(tsi, int64(0)).UTC().Format("20060102150405")
			if len(buckets) > idx+1 {
				set_timestamp = buckets[idx+1]
			}

			// hz:device:tz:user_id:g:activity_id:service:thing:time
			// hz:ios:la:1:m:3:fitbit:points
			// hz:0:0:0:0:0:0:points

			// hz:ios:0:0:0:0:0:points

			for _, d := range []string{"0", did} {
				if d == "" {
					continue
				}
				for _, t := range []string{"0", tz} {
					if t == "" {
						continue
					}
					for _, u := range []string{"0", uid} {
						if u == "" {
							continue
						}
						for _, g := range []string{"0", gid} {
							if g == "" {
								continue
							}
							for _, a := range []string{"0", aid} {
								if a == "" {
									continue
								}
								for _, s := range []string{"0", sid} {
									if s == "" {
										continue
									}

									key := fmt.Sprintf("hz:%s:%s:%s:%s:%s:%s:%s:%s",
										d, t, u, g, a, s, thing, bucket)

									// TODO LOCKING
									r.Send("MULTI")
									r.Send("HINCRBY", key, set_timestamp, value)
									r.Send("EXPIRE", key, psdcontext.Ctx.Config.RedisConfig.Expire)
									_, err := r.Do("EXEC")
									if err != nil {
										return err
									}
								}
							}
						}
					}
				}
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
