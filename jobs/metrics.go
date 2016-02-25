package jobs

import "fmt"

type MetricJob struct {
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
}
