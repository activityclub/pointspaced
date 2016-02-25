package server

import "pointspaced/router"
import "pointspaced/psdcontext"
import "fmt"

import "github.com/gin-gonic/gin"

func Run() {
	fmt.Println("-> PSD, starting http server on port: ", psdcontext.Ctx.Config.HttpConfig.Bind)
	r := gin.Default()
	router.ConfigureRoutes(r)
	r.Run(psdcontext.Ctx.Config.HttpConfig.Bind)
}
