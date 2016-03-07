package v1

import (
	"github.com/gin-gonic/gin"
	"pointspaced/persistence"
)

//import "strings"

// query takes
//  activity_types [48,4]
//  metric "points"
//  uids [1,2,3]
func Query(c *gin.Context) {

	metric := c.Param("metric")
	uids := c.Param("uids")
	activity_types := c.Param("activity_types")
	start_ts := c.Param("start_ts")
	end_ts := c.Param("end_ts")

	// todo - split these by comma; cast to int; and then send
	// to persistence to fetch results, or similar

	c.JSON(200, gin.H{
		"metric":         metric,
		"uids":           uids,
		"activity_types": activity_types,
		"start_ts":       start_ts,
		"end_ts":         end_ts,
	})
}

func buildResults(uids []int64, metric string, aTypes []int64, start_ts int64, end_ts int64) {

	persistence.ReadBuckets()
}
