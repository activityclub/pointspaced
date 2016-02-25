package router

import "github.com/gin-gonic/gin"
import "pointspaced/api/v1"

func ConfigureRoutes(r *gin.Engine) {
	r.GET("/health/ping", v1.Ping)
}
