package router

import "github.com/gin-gonic/gin"
import "pointspaced/api/v1"

func ConfigureRoutes(r *gin.Engine) {
	r.GET("/health/ping", v1.Ping)
	r.GET("/v1/query/:uid/:thing/:atid/:start_ts/:end_ts", v1.Query)
	r.POST("/v1/write", v1.Write)
}
