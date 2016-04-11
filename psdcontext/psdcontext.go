package psdcontext

import "log"
import "os"
import "github.com/BurntSushi/toml"
import "github.com/garyburd/redigo/redis"

type HttpConfig struct {
	Bind string
}

type NSQConfig struct {
	NSQLookupds []string
}

type RedisConfig struct {
	Expire int64
}

type Config struct {
	NSQConfig  NSQConfig
	HttpConfig HttpConfig
	Redis      RedisConfig
}

type Context struct {
	Config    Config
	RedisPool *redis.Pool
	AgScript  *redis.Script
}

var Ctx Context

func PrepareContext(cfg_file string) {
	Ctx = Context{}

	_, err := os.Stat(cfg_file)
	if err != nil {
		log.Fatal("Config file is missing: ", cfg_file)
	}

	if _, err := toml.DecodeFile(cfg_file, &Ctx.Config); err != nil {
		log.Fatal(err)
	}
}
