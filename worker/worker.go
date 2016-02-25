package worker

import "pointspaced/psdcontext"
import "github.com/bitly/go-nsq"
import "sync"
import "log"

func Run() {

	wg := &sync.WaitGroup{}

	// how many at a time (TODO configurable)
	wg.Add(1)

	config := nsq.NewConfig()

	// todo pull the topic and channel names out of config
	q, _ := nsq.NewConsumer("write_test", "ch", config)
	q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		log.Printf("Got a message: %v", message)

		// TODO parse message
		// TODO process message
		// TODO ack or reject etc

		wg.Done()
		return nil
	}))

	// TODO multiple nsqlookupds
	err := q.ConnectToNSQLookupd(psdcontext.Ctx.Config.NSQConfig.NSQLookupds[0])
	if err != nil {
		log.Panic("Could not connect")
	}
	wg.Wait()

}
