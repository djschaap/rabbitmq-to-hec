package rabbitmqtohec

import (
	"github.com/NeowayLabs/wabbit"
	"github.com/NeowayLabs/wabbit/amqptest"
	amqp_server "github.com/NeowayLabs/wabbit/amqptest/server"
	"regexp"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	t.Run("with minimal args",
		func(t *testing.T) {
			obj := New(HecConfig{}, MqConfig{})
			if *obj.HecConfig != *new(HecConfig) {
				t.Errorf("expected empty HecConfig, got: %#v", obj.HecConfig)
			}
			if *obj.MqConfig != *new(MqConfig) {
				t.Errorf("expected empty MqConfig, got: %#v", obj.MqConfig)
			}
		},
	)
}

func TestHandleDelivery(t *testing.T) {
	emptyHecConfig := new(HecConfig)

	t.Run("with delivery=nil",
		func(t *testing.T) {
			deliveryPtr := new(wabbit.Delivery)
			handleDelivery(*deliveryPtr, *emptyHecConfig)
		},
	)

	t.Run("with empty delivery",
		func(t *testing.T) {
			delivery := new(amqp_server.Delivery)
			success, err := handleDelivery(delivery, *emptyHecConfig)
			if err == nil {
				t.Errorf("expected error, got success=%d", success)
			}
			gotError, _ := regexp.MatchString(`^REJECTING unparsable delivery`, err.Error())
			if !gotError {
				t.Error("expected REJECTING unparsable delivery, got:", err)
			}
			if success != 0 {
				t.Errorf("expected success=0, got: %#v", success)
			}
		},
	)

	t.Run("with bare HEC event",
		func(t *testing.T) {
			delivery := amqp_server.NewDelivery(
				new(amqp_server.Channel),
				[]byte("{\"event\":{}}"), // data
				0,                        // tag
				"",                       // messageId
				nil,                      // hdrs
			)
			success, err := handleDelivery(delivery, *emptyHecConfig)
			if err != nil {
				t.Errorf("expected success, got err: %s", err)
			}
			if success != 1 {
				t.Errorf("expected success=1, got: %#v", success)
			}
		},
	)

	t.Run("with complete HEC event",
		func(t *testing.T) {
			delivery := amqp_server.NewDelivery(
				new(amqp_server.Channel),
				[]byte("{\"host\":\"h\",\"index\":\"i\",\"source\":\"s\",\"sourcetype\":\"t\",\"time\":\"1577836800\",\"event\":{\"e\":\"v\"},\"fields\":{\"f1\":\"v1\"}}"),
				0,   // tag
				"",  // messageId
				nil, // hdrs
			)
			success, err := handleDelivery(delivery, *emptyHecConfig)
			if err != nil {
				t.Errorf("expected success, got err: %s", err)
			}
			if success != 1 {
				t.Errorf("expected success=1, got: %#v", success)
			}
		},
	)
}

func TestHandleMessages(t *testing.T) {
	fakeServer := amqp_server.NewServer("amqp://guest:guest@localhost:5672/mock_one")
	fakeServer.Start()

	mockConn, err := amqptest.Dial("amqp://guest:guest@localhost:5672/mock_one")
	if err != nil {
		t.Error(err)
	}
	channel, err := mockConn.Channel()
	if err != nil {
		t.Error(err)
	}
	defer channel.Close()
	queue, err := declareQueue(channel, "q1")
	if err != nil {
		t.Error(err)
	}
	err = publishMessage(channel, "", queue, "expect-reject-due-to-parse-failure")
	if err != nil {
		t.Error(err)
	}

	obj := New(
		HecConfig{},
		MqConfig{
			Queue: "q1",
			Url:   "(ignored for mock)",
		},
	)
	c := &mqConsumer{
		conn:    mockConn,
		channel: nil,
		tag:     "ctag", // ctag,
		done:    make(chan error),
	}
	obj.reconnectToRabbitMq(c)

	t.Logf("sleeping for 1s")
	time.Sleep(time.Duration(1) * time.Second)
}

func declareQueue(channel wabbit.Channel, queueName string) (wabbit.Queue, error) {
	return channel.QueueDeclare(
		queueName,
		wabbit.Option{
			"durable":    true,
			"autoDelete": false,
			"exclusive":  false,
			"noWait":     false,
		},
	)
}

func publishMessage(channel wabbit.Channel, exchange string, queue wabbit.Queue, body string) error {
	return channel.Publish(
		exchange,     // exchange
		queue.Name(), // routing key
		[]byte(body),
		wabbit.Option{
			"deliveryMode": 2,
			"contentType":  "text/plain",
		})
}
