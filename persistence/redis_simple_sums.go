package persistence

import (
	"fmt"
	"pointspaced/psdcontext"
)

func ReadBuckets() int {
	psdcontext.Ctx.RedisPool = NewRedisPool(":6379")
	r := psdcontext.Ctx.RedisPool.Get()
	fmt.Println("hi ", r)

	return 1
}
