package v1

import "github.com/gin-gonic/gin"
import "pointspaced/jobs"

func Write(c *gin.Context) {
	var metric jobs.MetricJob
	c.BindJSON(&metric)
	err := jobs.ProcessMetricJob(metric)
	if err != nil {
		c.String(406, err.Error())
		return
	}
	c.String(200, "ok")
}
