package jobs

import "fmt"
import "pointspaced/persistence"

type MetricJob struct {
	Thing string `json:"thing"`
	Uid   int64  `json:"uid"`
	Atid  int64  `json:"atid"`
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
	opts["uid"] = fmt.Sprintf("%d", metric.Uid)
	opts["atid"] = fmt.Sprintf("%d", metric.Atid)
	opts["aid"] = fmt.Sprintf("%d", metric.Aid)
	opts["value"] = fmt.Sprintf("%d", metric.Value)
	opts["ts"] = fmt.Sprintf("%d", metric.Ts)

	err := curManager.WritePoint(opts)
	if err != nil {
		fmt.Println("trying to write a point and " + err.Error())
	}
}
