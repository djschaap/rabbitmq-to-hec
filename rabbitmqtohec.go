package rabbitmqtohec

// RabbitMQ code modeled after simple-consumer example
// https://github.com/streadway/amqp/blob/master/_examples/simple-consumer/consumer.go

import (
	"crypto/tls"
	"fmt"
	"github.com/NeowayLabs/wabbit"
	"github.com/NeowayLabs/wabbit/amqp"
	"github.com/djschaap/rabbitmq-to-hec/sendhec"
	hec "github.com/fuyufjh/splunk-hec-go"
	"github.com/kr/pretty"
	streadway_amqp "github.com/streadway/amqp"
	"log"
	"net/http"
	"os"
	"regexp"
	"time"
)

const (
	deliverySuccess = 1
	deliveryFailure = 0
)

var trace bool

type HecConfig struct {
	Token string
	Url   string
}

type MqConfig struct {
	Queue string
	Url   string
}

type mqConsumer struct {
	conn    wabbit.Conn
	channel wabbit.Channel
	tag     string
	done    chan error
}

type sess struct {
	HecConfig         *HecConfig
	MqConfig          *MqConfig
	amqpPrefetchCount int
	hostname          string
	pid               int
	product           string
}

func (c *mqConsumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer fmt.Println("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}

func (self *sess) RunOnce(runDuration time.Duration) error {
	var err error

	c, err := self.connectToRabbitMq(
		"rabbitmq-to-hec",
	)
	if err != nil {
		return fmt.Errorf("connectToRabbitMq: %s", err)
	}

	if runDuration > 0 {
		fmt.Printf("running for %s\n", runDuration)
		time.Sleep(runDuration)
	} else {
		fmt.Println("running forever")
		select {}
	}

	if err := c.Shutdown(); err != nil {
		return fmt.Errorf("Shutdown: %s", err)
	}

	return nil
}

func (self *sess) connectToRabbitMq(
	ctag string, // AMQP consumer tag
) (*mqConsumer, error) {
	var err error
	amqpConfig := streadway_amqp.Config{
		Properties: streadway_amqp.Table{
			"hostname": self.hostname,
			"pid":      self.pid,
			"product":  self.product,
			//"version": "0.0.0",
		},
	}
	c := &mqConsumer{
		conn:    nil,
		channel: nil,
		tag:     ctag,
		done:    make(chan error),
	}
	c.conn, err = amqp.DialConfig(self.MqConfig.Url, amqpConfig)
	if err != nil {
		return nil, fmt.Errorf("connectToRabbitMq DialConfig: %s", err)
	}
	return self.reconnectToRabbitMq(c)
}

func (self *sess) reconnectToRabbitMq(
	c *mqConsumer,
) (*mqConsumer, error) {
	var err error
	go func() {
		fmt.Printf("connectToRabbitMq: closing connection: %s\n", <-c.conn.NotifyClose(make(chan wabbit.Error)))
	}()

	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("connectToRabbitMq Channel: %s", err)
	}
	err = c.channel.Qos(
		self.amqpPrefetchCount, // prefetch count
		0,                      // prefetch size
		false,                  // global
	)
	if err != nil {
		return nil, fmt.Errorf("channel.QoS: %s", err)
	}

	queue, err := c.channel.QueueDeclare(
		self.MqConfig.Queue, // name of the queue
		wabbit.Option{
			"durable":    true,
			"autoDelete": false,
			"exclusive":  false,
			"noWait":     false,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("connectToRabbitMq Queue Declare: %s", err)
	}

	deliveries, err := c.channel.Consume(
		queue.Name(), // name
		c.tag,        // consumerTag,
		wabbit.Option{
			"noAck":     false,
			"exclusive": false,
			"noLocal":   false,
			"noWait":    false,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("connectToRabbitMq Queue Consume: %s", err)
	}

	hasHecUrl, _ := regexp.MatchString(`\S`, self.HecConfig.Url)
	hasHecToken, _ := regexp.MatchString(`\S`, self.HecConfig.Token)
	if hasHecUrl && hasHecToken {
		hecClient := hec.NewCluster(
			[]string{self.HecConfig.Url},
			self.HecConfig.Token,
		)
		if true { // TODO set InsecureSkipVerify from env var
			hecClient.SetHTTPClient(&http.Client{Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}})
		}
	}

	go handleMessages(deliveries, c.done, *self.HecConfig)

	return c, nil
}

func New(
	hecConfig HecConfig,
	mqConfig MqConfig,
) sess {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("level=FATAL os.Hostname() failed: %s", err)
	}
	s := sess{
		HecConfig:         &hecConfig,
		MqConfig:          &mqConfig,
		amqpPrefetchCount: 100,
		hostname:          hostname,
		pid:               os.Getpid(),
		product:           "rabbitmq-to-hec",
	}
	return s
}

func (_ sess) SetTrace(v bool) {
	trace = v
}

func handleDelivery(d wabbit.Delivery, hecConfig HecConfig) (int, error) {
	if d == nil {
		return deliveryFailure, fmt.Errorf("inbound delivery is nil")
	}

	hasHecUrl, _ := regexp.MatchString(`\S`, hecConfig.Url)
	hasHecToken, _ := regexp.MatchString(`\S`, hecConfig.Token)
	tracePrintf(
		"TRACE got %dB delivery: [%v] %q",
		len(d.Body()),
		d.DeliveryTag(),
		d.Body(),
	)

	hecMessagePtr := sendhec.FormatForHEC(d)
	if hecMessagePtr != nil {
		hecMessages := []hec.Event{
			*hecMessagePtr,
		}
		if !hasHecUrl || !hasHecToken {
			// HEC not configured, so dump and ack
			pretty.Logln("HEC-NOT-AVAILABLE:", *hecMessagePtr)
			return deliverySuccess, nil
		} else {
			err := sendhec.SendToHEC(hecConfig.Url, hecConfig.Token, hecMessages)
			if err != nil {
				// HEC failed; retry, then fail with soft nack+requeue
				log.Println("ERROR from SendToHEC:", err)
				return deliveryFailure, nil
				// TODO if HEC overload, print warning, nack w/requeue, and sleep for a few seconds
			} else {
				// HEC successful
				tracePretty("TRACE HEC SUCCESS:", *hecMessagePtr)
				return deliverySuccess, nil
			}
		}
	}

	// invalid HEC message
	return deliveryFailure, fmt.Errorf("REJECTING unparsable delivery: %#v", d)
}

func handleMessages(
	deliveries <-chan wabbit.Delivery,
	done chan error,
	hecConfig HecConfig,
) {
	for d := range deliveries {
		success, err := handleDelivery(d, hecConfig)
		if err != nil {
			// reject with requeue (to allow another consumer
			//   to attempt, before sending to dead-letter
			//   queue) would be ideal here, but doesn't work
			//   as expected:
			//   https://www.rabbitmq.com/blog/2010/08/03/well-ill-let-you-go-basicreject-in-rabbitmq/
			d.Reject(false)
		} else if success == deliverySuccess {
			// success
			d.Ack(false)
		} else {
			// transient failure, so nack with requeue
			d.Nack(false, true)
			time.Sleep(1 * time.Second)
		}
	}
	fmt.Printf("handleMessages: deliveries channel closed")
	done <- nil
}

func tracePretty(args ...interface{}) {
	if trace {
		pretty.Log(args...)
	}
}

func tracePrintf(s string, args ...interface{}) {
	if trace {
		log.Printf(s, args...)
	}
}

func tracePrintln(args ...interface{}) {
	if trace {
		log.Println(args...)
	}
}
