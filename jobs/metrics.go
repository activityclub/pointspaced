package jobs

import "fmt"
import "pointspaced/persistence"

type MetricJob struct {
	Thing string `json:"thing"`
	Tz    int64  `json:"tz"`
	Uid   int64  `json:"uid"`
	Aid   int64  `json:"aid"`
	Value int64  `json:"value"`
	Ts    int64  `json:"ts"`
	Sid   int64  `json:"sid"`
	Did   int64  `json:"did"`
	Gid   int64  `json:"gid"`
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
	opts["tz"] = fmt.Sprintf("%d", metric.Tz)
	opts["uid"] = fmt.Sprintf("%d", metric.Uid)
	opts["aid"] = fmt.Sprintf("%d", metric.Aid)
	opts["value"] = fmt.Sprintf("%d", metric.Value)
	opts["ts"] = fmt.Sprintf("%d", metric.Ts)
	opts["sid"] = fmt.Sprintf("%d", metric.Sid)
	opts["did"] = fmt.Sprintf("%d", metric.Did)
	opts["gid"] = fmt.Sprintf("%d", metric.Gid)

	err := curManager.WritePoint(opts)
	if err != nil {
		fmt.Println("trying to write a point and " + err.Error())
	}
}
