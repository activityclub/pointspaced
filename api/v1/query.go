package v1

import "github.com/gin-gonic/gin"
import "pointspaced/persistence"
import "strconv"
import "fmt"
import "strings"
import "encoding/json"

func Query(c *gin.Context) {

	// r.GET("/v1/query/:uid/:thing/:atid/:start_ts/:end_ts", v1.Query)

	mm := persistence.NewMetricManagerHZ()

	thing := c.Param("thing")
	start_ts := c.Param("start_ts")
	end_ts := c.Param("end_ts")
	uid := c.Param("uid")
	atid := c.Param("atid")

	start_ts_int, _ := strconv.ParseInt(start_ts, 10, 64)
	end_ts_int, _ := strconv.ParseInt(end_ts, 10, 64)

	sum := mm.QueryBuckets(uid, thing, "all", atid, start_ts_int, end_ts_int)

	c.String(200, fmt.Sprintf("%d", sum))
}

func ReadCount(c *gin.Context) {
	cm := persistence.NewCountManagerHZ()
	thing := c.Param("thing")
	start_ts := c.Param("start_ts")
	end_ts := c.Param("end_ts")

	start_ts_int, _ := strconv.ParseInt(start_ts, 10, 64)
	end_ts_int, _ := strconv.ParseInt(end_ts, 10, 64)
	sum := cm.ReadCount(thing, start_ts_int, end_ts_int)
	c.String(200, fmt.Sprintf("%d", sum))
}

func ReadCountWithIds(c *gin.Context) {
	cm := persistence.NewCountManagerHZ()
	start_ts := c.Param("start_ts")
	end_ts := c.Param("end_ts")
	prefix := c.Param("prefix")
	ids := strings.Split(c.PostForm("ids"), ",")

	start_ts_int, _ := strconv.ParseInt(start_ts, 10, 64)
	end_ts_int, _ := strconv.ParseInt(end_ts, 10, 64)

	data := make(map[string]int64)
	for _, id := range ids {
		data[id] = cm.ReadCount(prefix+"_"+id, start_ts_int, end_ts_int)
	}

	bytes, _ := json.Marshal(data)

	c.String(200, string(bytes))
}
