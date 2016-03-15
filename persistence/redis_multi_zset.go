package persistence

import "time"
import "fmt"
import "strconv"
import "errors"
import "pointspaced/psdcontext"
import "github.com/garyburd/redigo/redis"

type RedisMZ struct {
	from time.Time
}

func (self RedisMZ) ReadBuckets(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64, debug string) QueryResponse {
	buckets := self.bucketsForRange(start_ts, end_ts)
	fmt.Println("[Read] START", start_ts, "END", end_ts)
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
