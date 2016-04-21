package server

import "fmt"
import "github.com/gin-gonic/gin"
import "pointspaced/persistence"
import "pointspaced/psdcontext"
import "pointspaced/router"
import "github.com/garyburd/redigo/redis"

func Run() {
	psdcontext.Ctx.RedisPool = persistence.NewRedisPool(psdcontext.Ctx.Config.RedisConfig.Dsn)
	fmt.Println("-> PSD, starting http server on port: ", psdcontext.Ctx.Config.HttpConfig.Bind)

	rx := psdcontext.Ctx.RedisPool.Get()

	psdcontext.Ctx.AgScript = redis.NewScript(-1, `local sum = 0
for _, packed in ipairs(ARGV) do
  local unpacked = cmsgpack.unpack(packed)
  local cscore
  for i, v in ipairs(redis.call('HGETALL', unpacked[1])) do
    if i % 2 == 1 then
    cscore = tonumber(v)
    else
      if cscore >= unpacked[2] and cscore <= unpacked[3] then
        sum = sum + v
      end
    end
  end
end
return sum`)

	err := psdcontext.Ctx.AgScript.Load(rx)
	if err != nil {
		panic(err)
	}
	rx.Close()

	r := gin.Default()
	router.ConfigureRoutes(r)
	r.Run(psdcontext.Ctx.Config.HttpConfig.Bind)
}
