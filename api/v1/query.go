package v1

import (
	"github.com/gin-gonic/gin"
	"pointspaced/persistence"
	"strconv"
	"strings"
)

// query takes
//  activity_types [48,4]
//  metric "points"
//  uids [1,2,3]
func Query(c *gin.Context) {

	metric := c.Param("metric")
	uidStr := c.Param("uids")
	activity_types := c.Param("activity_types")
	start_ts := c.Param("start_ts")
	end_ts := c.Param("end_ts")
	debug := c.Query("debug")

	var uids = []int64{}

	for _, i := range strings.Split(uidStr, ",") {
		j, _ := strconv.ParseInt(i, 10, 64)
		uids = append(uids, j)
	}

	var atypes = []int64{0}
	for _, i := range strings.Split(activity_types, ",") {
		j, _ := strconv.ParseInt(i, 10, 64)
		atypes = append(atypes, j)
	}

	start_ts_int, _ := strconv.ParseInt(start_ts, 10, 64)
	end_ts_int, _ := strconv.ParseInt(end_ts, 10, 64)

	mm := persistence.NewMetricManager()
	qr := mm.ReadBuckets(uids, metric, atypes, start_ts_int, end_ts_int, debug)

	c.JSON(200, qr)
}
