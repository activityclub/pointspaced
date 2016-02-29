package jobs

import "fmt"
import "pointspaced/persistence"

type MetricJob struct {
	UserId           int64 `json:"user_id"`
	Steps            int64 `json:"steps"`
	Points           int64 `json:"points"`
	Distance         int64 `json:"distance"`
	CaloriesBurned   int64 `json:"calories_burned"`
	CaloriesConsumed int64 `json:"calories_consumed"`
	ActivityTypeId   int64 `json:"activity_type_id"`
	Timestamp        int64 `json:"timestamp"`
}

type MectricWriter interface {
	WritePoint(flavor string, userId int64, value int64, timestamp int64)
}

func ProcessMetricJob(metric MetricJob) {
	fmt.Println("-> [MetricJob] Processing Job, STEPS=", metric.Steps)
	fmt.Println("\tTs=", metric.Timestamp)
	persistence.WritePoint("steps", metric.UserId, metric.Steps, metric.ActivityTypeId, metric.Timestamp)
	persistence.WritePoint("points", metric.UserId, metric.Points, metric.ActivityTypeId, metric.Timestamp)
}
