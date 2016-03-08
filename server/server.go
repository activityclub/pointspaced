package server

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"pointspaced/persistence"
	"pointspaced/psdcontext"
	"pointspaced/router"
)

func Run() {
	psdcontext.Ctx.RedisPool = persistence.NewRedisPool(":6379")
	fmt.Println("-> PSD, starting http server on port: ", psdcontext.Ctx.Config.HttpConfig.Bind)
	r := gin.Default()
	router.ConfigureRoutes(r)
	r.Run(psdcontext.Ctx.Config.HttpConfig.Bind)
}
