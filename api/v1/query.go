package v1

import (
	"github.com/gin-gonic/gin"
	"pointspaced/persistence"
	"strconv"
	"strings"
)

type QueryResponse struct {
	Sum int64 `json:"sum"`
}

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

	// todo - split these by comma; cast to int; and then send
	// to persistence to fetch results, or similar

	var uids = []int64{}

	for _, i := range strings.Split(uidStr, ",") {
		j, _ := strconv.Atoi(i)
		uids = append(uids, int64(j))
	}

	var atypes = []int64{0}
	if activity_types == "" {
	}

	start_ts_int, _ := strconv.Atoi(start_ts)
	end_ts_int, _ := strconv.Atoi(end_ts)

	qr := buildResults(uids, metric, atypes, int64(start_ts_int), int64(end_ts_int))

	c.JSON(200, qr)
}

func buildResults(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64) QueryResponse {

	persistence.ReadBuckets()

	qr := QueryResponse{}
	qr.Sum = 123
	return qr
}
