package v2

import "github.com/gin-gonic/gin"
import "pointspaced/persistence"
import "strconv"

//import "fmt"
import "strings"

//import "encoding/json"

func MultiUserMultiThingQuery(c *gin.Context) {
	mm := persistence.NewMetricManagerHZ()
	start_ts := c.Param("start_ts")
	end_ts := c.Param("end_ts")
	atid := c.Param("atid")
	things := strings.Split(c.PostForm("things"), ",")
	uids := strings.Split(c.PostForm("ids"), ",")
	offsets := strings.Split(c.PostForm("offsets"), ",")
	ts1, _ := strconv.ParseInt(start_ts, 10, 64)
	ts2, _ := strconv.ParseInt(end_ts, 10, 64)

	//mumtresp, err := mm.MultiUserMultiThingQuery(uids, things, atid, ts1, ts2)
	mumtresp, err := mm.MultiQueryBucketsWithOffsets(uids, offsets, things, atid, ts1, ts2)
	if err != nil {
		c.JSON(422, gin.H{"error": err.Error()})
		return
	}

	c.JSON(200, mumtresp)
}
