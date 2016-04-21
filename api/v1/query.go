package v1

import "github.com/gin-gonic/gin"
import "pointspaced/persistence"
import "strconv"
import "strings"

func Query(c *gin.Context) {

	// r.GET("/v1/query/:dids/:tzs/:uids/:gids/:aids/:sids/:thing/:start_ts/:end_ts", v1.Query)

	mm := persistence.NewMetricManagerHZ()
	opts := make(map[string][]string)
	opts["dids"] = strings.Split(c.Param("dids"), ",")
	opts["tzs"] = strings.Split(c.Param("tzs"), ",")
	opts["uids"] = strings.Split(c.Param("uids"), ",")
	opts["gids"] = strings.Split(c.Param("gids"), ",")
	opts["aids"] = strings.Split(c.Param("aids"), ",")
	opts["sids"] = strings.Split(c.Param("sids"), ",")

	thing := c.Param("thing")
	start_ts := c.Param("start_ts")
	end_ts := c.Param("end_ts")

	start_ts_int, _ := strconv.ParseInt(start_ts, 10, 64)
	end_ts_int, _ := strconv.ParseInt(end_ts, 10, 64)

	qr := mm.QueryBuckets(thing, c.Query("group"), opts, start_ts_int, end_ts_int)

	c.JSON(200, qr)
}
