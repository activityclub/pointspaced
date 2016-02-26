package jobs

import "fmt"
import "errors"
import "strconv"
import "time"

type MetricJob struct {
	UserId           int64 `json:"user_id"`
	Steps            int64 `json:"steps"`
	Points           int64 `json:"points"`
	Distance         int64 `json:"distance"`
	CaloriesBurned   int64 `json:"calories_burned"`
	CaloriesConsumed int64 `json:"calories_consumed"`
	ActivityTypeId   int64 `json:"activity_type_id"`
	StartTs          int64 `json:"start_ts"`
	EndTs            int64 `json:"end_ts"`
}

func ProcessMetricJob(metric MetricJob) {
	fmt.Println("-> [MetricJob] Processing Job, STEPS=", metric.Steps)

	// what buckets are we going to put this into

	fmt.Println("\tStartTs=", metric.StartTs)

	buckets, err := metric.bucketsForJob()

	if err != nil {
		fmt.Println("\t[Error] ->", err.Error())
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

			fmt.Println("\t\t=>", bucket, tst)
		}
	}

}

func (self *MetricJob) bucketsForJob() ([]string, error) {

	out := []string{}

	if self.StartTs <= 0 {
		return out, errors.New("start timestamp was invalid")
	}

	for idx, n := range []int64{60, 600, 3600, 7200, 14400, 86400, 86400 * 14, 86400 * 30} {
		bucket := strconv.FormatInt((self.StartTs/n)*n, 10)
		bucket = bucket[0 : len(bucket)-idx]
		out = append(out, bucket)
	}

	return out, nil
}
