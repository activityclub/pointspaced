package persistence

import "time"
import "strconv"
import "errors"
import "pointspaced/psdcontext"
import "github.com/garyburd/redigo/redis"
import "github.com/ugorji/go/codec"
import "fmt"
import "strings"

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

func (self RedisHZ) QueryBuckets(uid, thing, aid, atid string, start_ts int64, end_ts int64) int64 {
	requests := self.requestsForRange(start_ts, end_ts)
	sum := int64(0)
	matchThing := thing2id(thing)
	allAtids := false
	if atid == "all" {
		allAtids = true
	}
	allAids := false
	if aid == "all" {
		allAids = true
	}

	r := psdcontext.Ctx.RedisPool.Get()
	defer r.Close()

	for _, request := range requests {
		key := fmt.Sprintf("hz:%s:%s:%s", matchThing, uid, request.TimeBucket)
		qmin, _ := strconv.ParseInt(request.QueryMin(), 10, 64)
		qmax, _ := strconv.ParseInt(request.QueryMax(), 10, 64)
		r.Send("HGETALL", key)
		r.Flush()
		reply, _ := redis.MultiBulk(r.Receive())
		lastAtid := ""
		lastAid := ""
		lastBucket := int64(0)
		for i, x := range reply {
			if i%2 == 0 {
				bytes := x.([]byte)
				str := string(bytes)
				tokens := strings.Split(str, ":")
				lastBucket, _ = strconv.ParseInt(tokens[0], 10, 64)
				lastAtid = tokens[1]
				lastAid = tokens[2]
			} else {
				bytes := x.([]byte)
				str := string(bytes)
				val, _ := strconv.ParseInt(str, 10, 64)
				if lastBucket > qmax || lastBucket < qmin {
					continue
				}
				if allAtids && allAids {
					sum += val
				} else if allAids == false && lastAid == aid {
					sum += val
				} else if allAtids == false && lastAtid == atid {
					sum += val
				}
			}
		}
	}

	return sum
}

// returns count, and activity type ids that we saw making the count
func (self RedisHZ) QueryBucketsLua(uid, thing, aid, atid string, start_ts int64, end_ts int64) (int64, []int64) {

	requests := self.requestsForRange(start_ts, end_ts)
	matchThing := thing2id(thing)
	cmd := []interface{}{}
	cmd = append(cmd, 0)

	for _, request := range requests {
		key := fmt.Sprintf("hz:%s:%s:%s", matchThing, uid, request.TimeBucket)
		qmin, _ := strconv.ParseInt(request.QueryMin(), 10, 64)
		qmax, _ := strconv.ParseInt(request.QueryMax(), 10, 64)
		item := []interface{}{}
		item = append(item, key, qmin, qmax, atid, aid)
		var b []byte = make([]byte, 0, 64)
		var h codec.Handle = new(codec.MsgpackHandle)
		var enc *codec.Encoder = codec.NewEncoderBytes(&b, h)
		var err error = enc.Encode(item)
		if err != nil {
			panic(err)
		}
		cmd = append(cmd, b)
	}

	r := psdcontext.Ctx.RedisPool.Get()
	defer r.Close()
	responses, err := redis.MultiBulk(psdcontext.Ctx.AgScript.Do(r, cmd...))
	if err != nil {
		panic(err)
	}

	sum := int64(0)
	atids := []int64{}
	for idx, reply := range responses {
		if idx == 0 {
			sum, _ = redis.Int64(reply, err)
		} else if idx == 1 {
			atid_replies, _ := redis.MultiBulk(reply, err)
			for _, atid_reply := range atid_replies {

				ratid, _ := redis.Int64(atid_reply, err)
				atids = append(atids, ratid)

			}
		}
	}
	return sum, atids
}

func (self RedisHZ) MultiUserQuery(uids []string, thing, atid string, start_ts int64, end_ts int64) (*MUResponse, error) {

	requests := self.requestsForRange(start_ts, end_ts)
	matchThing := thing2id(thing)

	r := psdcontext.Ctx.RedisPool.Get()
	defer r.Close()
	muresp := MUResponse{}
	muresp.Values = make(map[string]int64, len(uids))
	muresp.ATids = make(map[string][]int64, len(uids))

	for _, uid := range uids {
		cmd := []interface{}{}
		cmd = append(cmd, 0)

		for _, request := range requests {
			key := fmt.Sprintf("hz:%s:%s:%s", matchThing, uid, request.TimeBucket)
			qmin, _ := strconv.ParseInt(request.QueryMin(), 10, 64)
			qmax, _ := strconv.ParseInt(request.QueryMax(), 10, 64)
			item := []interface{}{}
			item = append(item, key, qmin, qmax, atid, "all")
			var b []byte = make([]byte, 0, 64)
			var h codec.Handle = new(codec.MsgpackHandle)
			var enc *codec.Encoder = codec.NewEncoderBytes(&b, h)
			var err error = enc.Encode(item)
			if err != nil {
				panic(err)
			}
			cmd = append(cmd, b)

		}
		err := psdcontext.Ctx.AgScript.SendHash(r, cmd...)
		if err != nil {
			return nil, err
		}
	}

	r.Flush()

	for _, uid := range uids {
		responses, err := redis.MultiBulk(r.Receive())
		if err != nil {
			continue
		}

		sum := int64(0)
		atids := []int64{}
		for idx, reply := range responses {
			if idx == 0 {
				sum, _ = redis.Int64(reply, err)
			} else if idx == 1 {
				atid_replies, _ := redis.MultiBulk(reply, err)
				for _, atid_reply := range atid_replies {

					ratid, _ := redis.Int64(atid_reply, err)
					atids = append(atids, ratid)

				}
			}
		}

		muresp.Values[uid] = sum
		muresp.ATids[uid] = atids

	}

	return &muresp, nil
}

func (self RedisHZ) ReadBuckets(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64) QueryResponse {

	//fmt.Println("[Hashzilla] START", start_ts, "END", end_ts)
	//requests := self.requestsForRange(start_ts, end_ts)
	//fmt.Println("[Hashzilla] Requests:", requests)

	qr := QueryResponse{}
	qr.UserToSum = make(map[string]int64, len(uids))

	for _, uid := range uids {
		sum, _ := self.QueryBucketsLua(fmt.Sprintf("%d", uid), "points", "all", "all", start_ts, end_ts)
		qr.UserToSum[fmt.Sprintf("%d", uid)] = sum
	}

	return qr
}

func (self RedisHZ) MultiUserMultiThingQuery(uids, things []string, atid string, start_ts int64, end_ts int64) (*MUMTResponse, error) {

	requests := self.requestsForRange(start_ts, end_ts)

	r := psdcontext.Ctx.RedisPool.Get()
	defer r.Close()

	mumtresp := MUMTResponse{}
	mumtresp.Data = make(map[string]map[string]interface{}) //, len(uids))

	for _, uid := range uids {
		for _, thing := range things {
			matchThing := thing2id(thing)
			cmd := []interface{}{}
			cmd = append(cmd, 0)
			for _, request := range requests {
				key := fmt.Sprintf("hz:%s:%s:%s", matchThing, uid, request.TimeBucket)
				qmin, _ := strconv.ParseInt(request.QueryMin(), 10, 64)
				qmax, _ := strconv.ParseInt(request.QueryMax(), 10, 64)
				item := []interface{}{}
				item = append(item, key, qmin, qmax, atid, "all")
				var b []byte = make([]byte, 0, 64)
				var h codec.Handle = new(codec.MsgpackHandle)
				var enc *codec.Encoder = codec.NewEncoderBytes(&b, h)
				var err error = enc.Encode(item)
				if err != nil {
					panic(err)
				}
				cmd = append(cmd, b)
			}
			err := psdcontext.Ctx.AgScript.SendHash(r, cmd...)
			if err != nil {
				return nil, err
			}
		}
	}

	r.Flush()

	for _, uid := range uids {
		for _, thing := range things {
			responses, err := redis.MultiBulk(r.Receive())
			if err != nil {
				continue
			}

			sum := int64(0)
			atids := []int64{}
			for idx, reply := range responses {
				if idx == 0 {
					sum, _ = redis.Int64(reply, err)
				} else if idx == 1 {
					atid_replies, _ := redis.MultiBulk(reply, err)
					for _, atid_reply := range atid_replies {
						ratid, _ := redis.Int64(atid_reply, err)
						atids = append(atids, ratid)
					}
				}
			}

			if mumtresp.Data[uid] == nil {
				mumtresp.Data[uid] = make(map[string]interface{})
			}
			mumtresp.Data[uid][thing] = sum
			if mumtresp.Data[uid]["_atids"] == nil || len(atids) > len(mumtresp.Data[uid]["_atids"].([]int64)) {
				mumtresp.Data[uid]["_atids"] = atids
			}
		}
	}

	return &mumtresp, nil
}

func (self RedisHZ) requestsForRange(start_ts int64, end_ts int64) map[string]RedisHZRequest {
	from := time.Unix(start_ts, 0).UTC()
	to := time.Unix(end_ts, 0).UTC()

	cursor := NewRedisACRCursor(from, to)

	//fmt.Println("[Hashzilla] requestsForRange FROM", from, "TO", to)
	//fmt.Println("Z", from.Format("20060102"))

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

		//fmt.Println("HRM", cursor.Cursor, "TO", cursor.To)

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
					//fmt.Println("S1-b", cursor.DayKey(), cursor.Hour())
					entry.ScoreMin = cursor.Hour()
					ch = true
				}
				if entry.ScoreMax < cursor.Hour() {
					//fmt.Println("S1-c", cursor.DayKey(), cursor.Hour())
					entry.ScoreMax = cursor.Hour()
					ch = true
				}
				if ch {
					reqs[cursor.DayKey()] = entry
				}
			} else {
				//fmt.Println("S1", cursor.DayKey(), cursor.Hour(), cursor.Hour())
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

func (self RedisHZ) OldWritePoint(thing string, userId, value, atid, ts int64) error {
	opts := make(map[string]string)
	opts["thing"] = thing
	opts["uid"] = fmt.Sprintf("%d", userId)
	opts["aid"] = "1001"
	opts["mode"] = "increment"
	opts["atid"] = fmt.Sprintf("%d", atid)
	opts["value"] = fmt.Sprintf("%d", value)
	opts["ts1"] = fmt.Sprintf("%d", ts)
	opts["ts2"] = fmt.Sprintf("%d", ts)
	return self.WritePoint(opts)
}

func thing2id(thing string) string {
	if thing == "steps" {
		return "1"
	}
	if thing == "calories" {
		return "2"
	}
	if thing == "points" {
		return "3"
	}
	if thing == "distance" {
		return "4"
	}
	if thing == "cb" {
		return "5"
	}
	return ""
}

func (self RedisHZ) WritePoint(opts map[string]string) error {

	thing := thing2id(opts["thing"])
	uid := opts["uid"]
	atid := opts["atid"]
	aid := opts["aid"]

	value := opts["value"]
	valuei, _ := strconv.ParseInt(value, 10, 64)

	created_at := opts["ts1"]
	created_ati, _ := strconv.ParseInt(created_at, 10, 64)
	updated_at := opts["ts2"]
	updated_ati, _ := strconv.ParseInt(updated_at, 10, 64)

	if (value == "") ||
		(created_at == "") ||
		(updated_at == "") ||
		(uid == "") ||
		(atid == "") ||
		(aid == "") ||
		(thing == "") {
		return errors.New("invalid arguments")
	}

	mode, useMode := opts["mode"]
	sum := int64(0)
	if useMode == false || mode == "diffadd" {
		sum, _ = self.QueryBucketsLua(uid, opts["thing"], aid, "all", created_ati, updated_ati)
	}
	valuei = valuei - sum
	if valuei == 0 {
		return nil
	}

	//rails - oh activity id 777 now has 4022 points as of $updated_at_ts
	//> psd -> ok so 777 needs to have 4022 as of updated_at_ts, lets do a query using the normal hashzilla stuff, but lets filtre it by activity id 777 ..
	//> psd -> ok so at $updated_at we only have 4011, so we are missing 11 points, so lets insert hincrby += 11 for $updated_at

	buckets, err := self.bucketsForJob(updated_ati)

	r := psdcontext.Ctx.RedisPool.Get()
	r.Send("MULTI")

	if err != nil {
		return err
	} else {
		//		fmt.Println("\t[Buckets] ->")
		for idx, bucket := range buckets {
			// figure out when this is

			set_timestamp := time.Unix(updated_ati, int64(0)).UTC().Format("20060102150405")
			if len(buckets) > idx+1 {
				set_timestamp = buckets[idx+1]
			}

			// "hz:1:2016" 201603:3:3" 123

			key := fmt.Sprintf("hz:%s:%s:%s", thing, uid, bucket)

			r.Send("HINCRBY", key, fmt.Sprintf("%s:%s:%s", set_timestamp, atid, aid), valuei)
			r.Send("EXPIRE", key, psdcontext.Ctx.Config.RedisConfig.Expire)
		}
	}

	_, err = r.Do("EXEC")
	if err != nil {
		return err
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

func (self RedisHZ) IncrementCount(thing string, ts, value int64) error {

	if ts <= 0 {
		return errors.New("invalid timestamp")
	}
	if len(thing) < 2 {
		return errors.New("all things must be 2 characters or longer")
	}
	if len(thing) > 64 {
		return errors.New("all things must be 64 characters or less")
	}

	buckets, err := self.bucketsForJob(ts)
	if err != nil {
		return err
	} else {
		r := psdcontext.Ctx.RedisPool.Get()
		defer r.Close()
		for idx, bucket := range buckets {
			set_timestamp := time.Unix(ts, int64(0)).UTC().Format("20060102150405")
			if len(buckets) > idx+1 {
				set_timestamp = buckets[idx+1]
			}
			key := fmt.Sprintf("c:%s:%s", thing, bucket)
			r.Send("HINCRBY", key, set_timestamp, value)
			r.Send("EXPIRE", key, (86400 * 30))
			r.Flush()
			r.Receive()
			r.Receive()
		}
	}
	return nil
}

func (self RedisHZ) ReadCount(thing string, start_ts, end_ts int64) int64 {
	requests := self.requestsForRange(start_ts, end_ts)
	sum := int64(0)
	r := psdcontext.Ctx.RedisPool.Get()
	defer r.Close()

	for _, request := range requests {
		//		fmt.Println("REQ", request)
		key := fmt.Sprintf("c:%s:%s", thing, request.TimeBucket)
		qmin, _ := strconv.ParseInt(request.QueryMin(), 10, 64)
		qmax, _ := strconv.ParseInt(request.QueryMax(), 10, 64)
		//		fmt.Println("QMIN", qmin)
		//		fmt.Println("QMAX", qmax)
		//		fmt.Println("KEY", key)
		r.Send("HGETALL", key)

		r.Flush()
		reply, _ := redis.MultiBulk(r.Receive())
		lastBucket := int64(0)
		for i, x := range reply {
			if i%2 == 0 {
				bytes := x.([]byte)
				lastBucketStr := string(bytes)
				lastBucket, _ = strconv.ParseInt(lastBucketStr, 10, 64)
				//				fmt.Println("LB", lastBucket)
			} else {
				bytes := x.([]byte)
				str := string(bytes)
				val, _ := strconv.ParseInt(str, 10, 64)
				if lastBucket > qmax || lastBucket < qmin {
					continue
				} else {
					//					fmt.Println("VAL ACCEPT", lastBucket)
					//					fmt.Println("VAL", val)
				}
				sum += val
			}
		}
	}

	return sum
}
