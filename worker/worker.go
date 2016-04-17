package worker

import "pointspaced/psdcontext"
import "pointspaced/jobs"
import "github.com/bitly/go-nsq"
import "encoding/json"
import "sync"
import "log"
import "fmt"

type WorkerMessage struct {
	Command   string         `json:"command"`
	MetricJob jobs.MetricJob `json:"metric,omitempty"`
}

func Run() {

	wg := &sync.WaitGroup{}

	// how many at a time (TODO configurable)
	wg.Add(1)

	config := nsq.NewConfig()

	// todo pull the topic and channel names out of config
	q, _ := nsq.NewConsumer("psd", "psd", config)
	q.AddHandler(nsq.HandlerFunc(func(nsqMsg *nsq.Message) error {

		fmt.Println(string(nsqMsg.Body))
		message := WorkerMessage{}

		err := json.Unmarshal(nsqMsg.Body, &message)

		if err != nil {
			panic(err)
		}
		log.Println("got a parsed message", message)
		log.Println("-> C=", message.Command)

		if message.Command == "metric" {
			jobs.ProcessMetricJob(message.MetricJob)
		} else {
			log.Println("-> COMMAND UNSUPPORTED")
		}
		// TODO process message
		// TODO ack or reject etc

		//		wg.Done()
		return nil
	}))

	// TODO multiple nsqlookupds
	err := q.ConnectToNSQLookupd(psdcontext.Ctx.Config.NSQConfig.NSQLookupds[0])
	if err != nil {
		log.Panic("Could not connect")
	}
	wg.Wait()

}
