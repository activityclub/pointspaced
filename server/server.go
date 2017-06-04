package server

import "fmt"
import "github.com/gin-gonic/gin"
import "github.com/activityclub/pointspaced/persistence"
import "github.com/activityclub/pointspaced/psdcontext"
import "github.com/activityclub/pointspaced/router"
import "github.com/garyburd/redigo/redis"

func Run() {
	psdcontext.Ctx.RedisPool = persistence.NewRedisPool(psdcontext.Ctx.Config.RedisConfig.Dsn)
	fmt.Println("-> PSD, starting http server on port: ", psdcontext.Ctx.Config.HttpConfig.Bind)

	rx := psdcontext.Ctx.RedisPool.Get()

	psdcontext.Ctx.AgScript = redis.NewScript(-1, `local sum = 0
local atids = {}
local aids = {}
for _, packed in ipairs(ARGV) do
        local unpacked = cmsgpack.unpack(packed)
        local res = redis.call('HGETALL', unpacked[1])
        for i, v in ipairs(res) do
                if i % 2 == 0 then
                        local cscore = tonumber(v)
                        local idx = 0
                        local tsv = 0
                        local aid = "all"
                        local atid = "all"
                        for word in string.gmatch(res[i-1], '[^:]+') do
                                if idx == 0 then
                                   tsv = tonumber(word)
                                elseif idx == 1 then
                                   atid = word
                                elseif idx == 2 then
                                   aid = word
                                end
                                idx = idx + 1
                        end
                        if tsv >= unpacked[2] and tsv <= unpacked[3] then
                                if unpacked[4] == "all" or unpacked[4] == atid then
                                        if unpacked[5] == "all" or unpacked[5] == aid then
                                                sum = sum + cscore
                                                if aids[aid] then
                                                else
                                                  atids[#atids+1] = tonumber(atid)
                                                  aids[aid] = true
                                                end
                                        end
                                end
                        end
                end
        end
end
return {sum, atids}`)

	err := psdcontext.Ctx.AgScript.Load(rx)
	if err != nil {
		panic(err)
	}
	rx.Close()

	r := gin.Default()
	router.ConfigureRoutes(r)
	r.Run(psdcontext.Ctx.Config.HttpConfig.Bind)
}
