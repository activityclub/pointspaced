package persistence

import (
	"fmt"
	"pointspaced/psdcontext"
)

func ReadBuckets() int {
	r := psdcontext.Ctx.RedisPool.Get()
	fmt.Println(r)

	return 1
}
