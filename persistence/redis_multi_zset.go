package persistence

import "time"
import "fmt"
import "strconv"
import "errors"
import "pointspaced/psdcontext"

type RedisWriter struct{}

func (self RedisWriter) WritePoint(flavor string, userId int64, value int64, activityTypeId int64, timestamp int64) error {

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
		for _, bucket := range buckets {

			// figure out when this is

			ts := bucket
			for {
				if len(ts) >= 10 {
					break
				}
				ts += "0"
			}

			tsx, _ := strconv.ParseInt(ts, 10, 64)
			tst := time.Unix(tsx, int64(0))

			strUserId := strconv.FormatInt(userId, 10)

			for _, aType := range []int64{0, activityTypeId} {

				strAType := strconv.FormatInt(aType, 10)

				key := "psd:"
				key = key + strUserId + ":" + strAType + ":" + flavor + ":" + bucket

				fmt.Println("\t\t=>", bucket, tst, "ZADD", key, timestamp, value)

				// TODO LOCKING
				// NOTE we store the value as the "SCORE"
				r.Do("ZINCRBY", key, value, timestamp)
			}
		}
	}

	r.Close()
	return nil
}

func (self *RedisWriter) bucketsForJob(ts int64) ([]string, error) {

	out := []string{}

	if ts <= 0 {
		return out, errors.New("timestamp was invalid")
	}

	for idx, n := range []int64{600, 7200, 14400, 86400, 86400 * 30} {
		bucket := strconv.FormatInt((ts/n)*n, 10)
		bucket = bucket[0 : len(bucket)-idx]
		out = append(out, bucket)
	}

	return out, nil
}
