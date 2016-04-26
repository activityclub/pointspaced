package jobs

import "fmt"
import "pointspaced/persistence"

type MetricJob struct {
	Thing string `json:"thing"`
	Uid   int64  `json:"uid"`
	Atid  int64  `json:"atid"`
	Aid   int64  `json:"aid"`
	Value int64  `json:"value"`
	Ts1   int64  `json:"ts1"`
	Ts2   int64  `json:"ts2"`
}

var curManager *persistence.MetricManager = nil

func ProcessMetricJob(metric MetricJob) {
	fmt.Println("-> [MetricJob] Processing Job, THING=", metric.Thing)
	fmt.Println("\tTs2=", metric.Ts2)
	if curManager == nil {
		curManager = persistence.NewMetricManagerHZ()
	}
	opts := make(map[string]string)
	opts["thing"] = metric.Thing
	opts["uid"] = fmt.Sprintf("%d", metric.Uid)
	opts["atid"] = fmt.Sprintf("%d", metric.Atid)
	opts["aid"] = fmt.Sprintf("%d", metric.Aid)
	opts["value"] = fmt.Sprintf("%d", metric.Value)
	opts["ts1"] = fmt.Sprintf("%d", metric.Ts1)
	opts["ts2"] = fmt.Sprintf("%d", metric.Ts2)

	err := curManager.WritePoint(opts)
	if err != nil {
		fmt.Println("trying to write a point and " + err.Error())
	}
}
