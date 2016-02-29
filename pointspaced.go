package main

import "os"
import "github.com/codegangsta/cli"
import "fmt"
import "pointspaced/psdcontext"
import "pointspaced/server"
import "pointspaced/worker"
import "pointspaced/persistence"

func main() {
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
				psdcontext.Ctx.RedisPool = persistence.NewRedisPool(":6379")
				fmt.Println("-> PSD, starting worker with nsqlookupds: ", psdcontext.Ctx.Config.NSQConfig.NSQLookupds)
				worker.Run()
			},
		},
	}

	app.Run(os.Args)
}
