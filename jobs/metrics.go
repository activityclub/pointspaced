package jobs

import "fmt"
import "pointspaced/persistence"

type MetricJob struct {
	thing string `json:"thing"`
	tz    int64  `json:"tz"`
	uid   int64  `json:"uid"`
	aid   int64  `json:"aid"`
	value int64  `json:"value"`
	ts    int64  `json:"ts"`
	sid   int64  `json:"sid"`
	did   int64  `json:"did"`
	gid   int64  `json:"gid"`
}

var curManager *persistence.MetricManager = nil

func ProcessMetricJob(metric MetricJob) {
	fmt.Println("-> [MetricJob] Processing Job, THING=", metric.thing)
	fmt.Println("\tTs=", metric.ts)
	if curManager == nil {
		curManager = persistence.NewMetricManagerHZ()
	}
	opts := make(map[string]string)
	opts["thing"] = metric.thing
	opts["tz"] = fmt.Sprintf("%d", metric.tz)
	opts["uid"] = fmt.Sprintf("%d", metric.uid)
	opts["aid"] = fmt.Sprintf("%d", metric.aid)
	opts["value"] = fmt.Sprintf("%d", metric.value)
	opts["ts"] = fmt.Sprintf("%d", metric.ts)
	opts["sid"] = fmt.Sprintf("%d", metric.sid)
	opts["did"] = fmt.Sprintf("%d", metric.did)
	opts["gid"] = fmt.Sprintf("%d", metric.gid)

	err := curManager.WritePoint(opts)
	if err != nil {
		fmt.Println("trying to write a point and " + err.Error())
	}
}
