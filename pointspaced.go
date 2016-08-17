package main

import "os"
import "github.com/codegangsta/cli"
import "fmt"
import "pointspaced/psdcontext"
import "pointspaced/server"
import "pointspaced/worker"
import "pointspaced/persistence"
import "strings"

func main() {
	psdcontext.PrepareContext("/Users/aa/dev/ac/go/src/pointspaced/conf/settings.toml")
	psdcontext.Ctx.RedisPool = persistence.NewRedisPool(psdcontext.Ctx.Config.RedisConfig.Dsn)

	fmt.Println("hi")
	mm := persistence.NewMetricManagerHZ()
	atid := "all"
	things := strings.Split("points,steps", ",")
	uids := strings.Split("54,101,107,48", ",")
	offsets := strings.Split("-28800,-28800,-28800,0", ",")
	ts1 := int64(1471281987)
	ts2 := int64(1471466799)

	mumtresp, err := mm.MultiQueryBucketsWithOffsets(uids, offsets, things, atid, ts1, ts2)
	fmt.Println("hi2 ", mumtresp, err)
}

func main2() {
	app := cli.NewApp()
	app.Name = "pointspaced"
	app.Usage = "yo dawg i heard you like points"
	app.Version = "0.0.1"
	app.Author = "0x7a69"
	app.Email = "team@0x7a69.net"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config",
			Value: "conf/settings.toml",
			Usage: "path to settings.toml file",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:      "server",
			ShortName: "server",
			Usage:     "start pointspaced http server",
			Action: func(c *cli.Context) {
				cfg_file := c.GlobalString("config")
				psdcontext.PrepareContext(cfg_file)
				server.Run()
			},
		},
		{
			Name:      "worker",
			ShortName: "worker",
			Usage:     "start pointspaced nsq worker",
			Action: func(c *cli.Context) {
				cfg_file := c.GlobalString("config")
				psdcontext.PrepareContext(cfg_file)
				psdcontext.Ctx.RedisPool = persistence.NewRedisPool(psdcontext.Ctx.Config.RedisConfig.Dsn)
				fmt.Println("-> PSD, starting worker with nsqlookupds: ", psdcontext.Ctx.Config.NSQConfig.NSQLookupds)
				worker.Run()
			},
		},
	}

	app.Run(os.Args)
}
