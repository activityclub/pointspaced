package jobs

import "fmt"
import "pointspaced/persistence"

type CountJob struct {
	Thing string `json:"thing"`
	Value int64  `json:"value"`
	Ts    int64  `json:"ts"`
}

var countManager *persistence.CountManager = nil

func ProcessCountJob(metric CountJob) {
	fmt.Println("-> [CountJob] Processing Job, THING="+metric.Thing+" V=", metric.Value)
	if countManager == nil {
		countManager = persistence.NewCountManagerHZ()
	}
	err := countManager.IncrementCount(metric.Thing, metric.Ts, metric.Value)
	if err != nil {
		fmt.Println("trying to write increment count: " + err.Error())
	}
}
