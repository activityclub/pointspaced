package router

import "github.com/gin-gonic/gin"
import "github.com/activityclub/pointspaced/api/v1"
import "github.com/activityclub/pointspaced/api/v2"

func ConfigureRoutes(r *gin.Engine) {
	r.GET("/health/ping", v1.Ping)
	r.GET("/v1/query/:uid/:thing/:atid/:start_ts/:end_ts", v1.Query)
	r.POST("/v1/query_ids/:thing/:atid/:start_ts/:end_ts", v1.QueryWithIds)
	r.POST("/v1/mu_query/:thing/:atid/:start_ts/:end_ts", v1.MultiUserQuery)
	r.POST("/v1/mumt_query/:atid/:start_ts/:end_ts", v1.MultiUserMultiThingQuery)
	r.POST("/v2/mumt_query/:atid/:start_ts/:end_ts", v2.MultiUserMultiThingQuery)
	r.GET("/v1/readcount/:thing/:start_ts/:end_ts", v1.ReadCount)
	r.POST("/v1/readcount_with_ids/:prefix/:start_ts/:end_ts", v1.ReadCountWithIds)
	r.POST("/v1/write", v1.Write)
}
