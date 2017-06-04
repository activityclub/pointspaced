package worker

import "github.com/activityclub/pointspaced/psdcontext"
import "github.com/activityclub/pointspaced/jobs"
import "github.com/bitly/go-nsq"
import "encoding/json"
import "sync"
import "log"
import "fmt"

type WorkerMessage struct {
	Command   string         `json:"command"`
	MetricJob jobs.MetricJob `json:"metric,omitempty"`
	CountJob  jobs.CountJob  `json:"count,omitempty"`
}

func Run() {
	wg := &sync.WaitGroup{}
	wg.Add(1) // just means .. uhh dont quit?

	// TODO - enhanced configuration

	config := nsq.NewConfig()
	config.UserAgent = fmt.Sprintf("PSD/0.1")
	config.MaxInFlight = 4

	q, _ := nsq.NewConsumer("psd", "psd", config)

	q.AddConcurrentHandlers(nsq.HandlerFunc(func(nsqMsg *nsq.Message) error {

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
		} else if message.Command == "count" {
			jobs.ProcessCountJob(message.CountJob)
		} else {
			log.Println("-> COMMAND UNSUPPORTED")
		}
		// TODO process message
		// TODO ack or reject etc

		//		wg.Done()
		return nil
	}), 4)

	// TODO multiple nsqlookupds
	err := q.ConnectToNSQLookupd(psdcontext.Ctx.Config.NSQConfig.NSQLookupds[0])
	if err != nil {
		log.Panic("Could not connect")
	}
	wg.Wait()

}
