package v1

import "github.com/gin-gonic/gin"
import "pointspaced/persistence"

//import "strconv"
//import "strings"

func Write(c *gin.Context) {
	mm := persistence.NewMetricManagerHZ()

	opts := make(map[string]string)
	opts["thing"] = c.PostForm("thing")
	opts["tz"] = c.PostForm("tz")
	opts["uid"] = c.PostForm("uid")
	opts["aid"] = c.PostForm("aid")
	opts["value"] = c.PostForm("value")
	opts["ts"] = c.PostForm("ts")
	opts["sid"] = c.PostForm("sid")
	opts["did"] = c.PostForm("did")
	opts["gid"] = c.PostForm("gid")

	err := mm.WritePoint(opts)
	if err != nil {
		c.String(406, err.Error())
		return
	}
	c.String(200, "ok")
}
