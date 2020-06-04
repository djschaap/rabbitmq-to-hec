package main

// modeled after https://github.com/rabbitmq/rabbitmq-tutorials/blob/master/go/receive.go

import (
	"flag"
	"fmt"
	//"github.com/djschaap/rabbitmq-to-hec"
	//"github.com/joho/godotenv"
	"github.com/streadway/amqp"
	"log"
	"os"
	"regexp"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	var runSeconds int
	flag.IntVar(&runSeconds, "lifetime", 5,
		"max runtime in seconds (0 = run forever)")

	flag.Parse()
	sourceQueue := os.Getenv("SRC_QUEUE")
	mqUrl := os.Getenv("MQ_URL")
	hasMqUrl, _ := regexp.MatchString(`^amqp://`, mqUrl)
	if !hasMqUrl {
		fmt.Println("ERROR: MQ_URL must be set")
		os.Exit(1)
	}

	conn, err := amqp.Dial(mqUrl)
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	/*
	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")
	*/

	msgs, err := ch.Consume(
		//q.Name, // queue
		sourceQueue, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	//log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

	runDuration := time.Duration(runSeconds) * time.Second
	if runDuration > 0 {
		fmt.Printf("waiting for %s\n", runDuration)
		time.Sleep(runDuration)
	} else {
		fmt.Println("running forever; hit Control-C to exit")
		//select {}
		forever := make(chan bool)
		<-forever
	}
}
