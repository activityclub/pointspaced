package v1

import "github.com/gin-gonic/gin"
import "pointspaced/persistence"
import "strconv"
import "fmt"

//import "strings"

func Query(c *gin.Context) {

	thing := c.Param("thing")
	//uidStr := c.Param("uids")
	//activity_types := c.Param("activity_types")
	start_ts := c.Param("start_ts")
	end_ts := c.Param("end_ts")
	uids := []int64{0}

	/*
		var uids = []int64{}

		for _, i := range strings.Split(uidStr, ",") {
			j, _ := strconv.ParseInt(i, 10, 64)
			uids = append(uids, j)
		}

		var atypes = []int64{}
		for _, i := range strings.Split(activity_types, ",") {
			j, _ := strconv.ParseInt(i, 10, 64)
			atypes = append(atypes, j)
		}
	*/

	start_ts_int, _ := strconv.ParseInt(start_ts, 10, 64)
	end_ts_int, _ := strconv.ParseInt(end_ts, 10, 64)

	mm := persistence.NewMetricManagerHZ()
	qr := mm.ReadBuckets(uids, thing, uids, start_ts_int, end_ts_int)
	fmt.Println(qr)

	c.JSON(200, qr)
}
