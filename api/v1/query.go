package v1

import "github.com/gin-gonic/gin"
import "pointspaced/persistence"
import "strconv"
import "fmt"

//import "strings"

func Query(c *gin.Context) {

	// r.GET("/v1/query/:dids/:tzs/:uids/:gids/:aids/:sids/:thing/:start_ts/:end_ts", v1.Query)

	/*
		dids := c.Param("dids")
		tzs := c.Param("tzs")
		uids := c.Param("uids")
		gids := c.Param("gids")
		aids := c.Param("aids")
		sids := c.Param("sids") */
	thing := c.Param("thing")
	start_ts := c.Param("start_ts")
	end_ts := c.Param("end_ts")

	group := c.Query("group")
	if group == "" {
		group = "uids"
	}

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
	temp := []int64{0}
	qr := mm.ReadBuckets(temp, thing, temp, start_ts_int, end_ts_int)
	fmt.Println(qr)

	c.JSON(200, qr)
}
