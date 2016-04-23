package jobs

import "fmt"
import "pointspaced/persistence"

type MetricJob struct {
	Thing string `json:"thing"`
	Uid   int64  `json:"uid"`
	Aid   int64  `json:"aid"`
	Value int64  `json:"value"`
	Ts    int64  `json:"ts"`
}

var curManager *persistence.MetricManager = nil

func ProcessMetricJob(metric MetricJob) {
	fmt.Println("-> [MetricJob] Processing Job, THING=", metric.Thing)
	fmt.Println("\tTs=", metric.Ts)
	if curManager == nil {
		curManager = persistence.NewMetricManagerHZ()
	}
	opts := make(map[string]string)
	opts["thing"] = metric.Thing
	if metric.Uid > 0 {
		opts["uid"] = fmt.Sprintf("%d", metric.Uid)
	}
	if metric.Aid > 0 {
		opts["aid"] = fmt.Sprintf("%d", metric.Aid)
	}

	opts["value"] = fmt.Sprintf("%d", metric.Value)

	if metric.Ts > 0 {
		opts["ts"] = fmt.Sprintf("%d", metric.Ts)
	}

	err := curManager.WritePoint(opts)
	if err != nil {
		fmt.Println("trying to write a point and " + err.Error())
	}
}
