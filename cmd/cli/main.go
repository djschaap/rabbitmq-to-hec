package main

import (
	"flag"
	"fmt"
	"github.com/djschaap/rabbitmq-to-hec"
	"github.com/joho/godotenv"
	"log"
	"os"
	"regexp"
	"time"
)

var (
	buildDt string
	commit  string
	version string
)

func buildAmqpUrl() string {
	var amqpUrl string
	amqpUrl = os.Getenv("MQ_URL")
	if len(amqpUrl) > 0 {
		return amqpUrl
	}

	amqpHost := os.Getenv("AMQP_HOST")
	amqpPassword := os.Getenv("AMQP_PASSWORD")
	amqpPort := os.Getenv("AMQP_PORT")
	amqpProtocol := os.Getenv("AMQP_PROTOCOL")
	amqpUsername := os.Getenv("AMQP_USERNAME")
	amqpVhost := os.Getenv("AMQP_VHOST")

	var userPass string
	if len(amqpPassword) > 0 {
		userPass = fmt.Sprintf("%s:%s@", amqpUsername, amqpPassword)
	} else {
		userPass = fmt.Sprintf("%s@", amqpUsername)
	}

	var hostPort string
	if len(amqpPort) > 0 {
		hostPort = fmt.Sprintf("%s:%s", amqpHost, amqpPort)
	} else {
		hostPort = amqpHost
	}
	amqpUrl = fmt.Sprintf("%s://%s%s", amqpProtocol, userPass, hostPort)
	if len(amqpVhost) > 0 {
		amqpUrl = amqpUrl + "/" + amqpVhost
	}

	return amqpUrl
}

func main() {
	printVersion()

	var runSeconds int
	flag.IntVar(&runSeconds, "lifetime", 5,
		"max runtime in seconds (0 = run forever)")
	flag.Parse()

	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %s", err)
	}

	sourceQueue := os.Getenv("SRC_QUEUE")
	amqpUrl := buildAmqpUrl()
	if ok, err := regexp.MatchString(`^amqps?://`, amqpUrl); !ok {
		if err != nil {
			log.Fatalf("INTERNAL ERROR parsing AMQP_URL: %s", err)
		} else {
			log.Fatal("MQ_URL must begin with amqp:// or amqps://")
		}
	}

	hecUrl := os.Getenv("HEC_URL")
	hecToken := os.Getenv("HEC_TOKEN")

	hecConfig := rabbitmqtohec.HecConfig{
		Token: hecToken,
		Url:   hecUrl,
	}
	mqConfig := rabbitmqtohec.MqConfig{
		Queue: sourceQueue,
		Url:   amqpUrl,
	}

	app := rabbitmqtohec.New(
		hecConfig,
		mqConfig,
	)
	if len(os.Getenv("TRACE")) > 0 {
		fmt.Println("*** TRACE is enabled ***")
		app.SetTrace(true)
	}

	err = app.RunOnce(time.Duration(runSeconds) * time.Second)
	if err != nil {
		log.Fatalf("Error from RunOnce: %s", err)
	}
}

func printVersion() {
	fmt.Println("rabbitmq-to-hec  Version:", version,
		" Commit:", commit,
		" Built at:", buildDt)
}
