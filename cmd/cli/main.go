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

func main() {
	printVersion()

	var runSeconds int
	flag.IntVar(&runSeconds, "lifetime", 5,
		"max runtime in seconds (0 = run forever)")
	flag.Parse()

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	sourceQueue := os.Getenv("SRC_QUEUE")
	mqUrl := os.Getenv("MQ_URL")
	hasMqUrl, _ := regexp.MatchString(`^amqp://`, mqUrl)
	if !hasMqUrl {
		fmt.Println("ERROR: MQ_URL must be set")
		os.Exit(1)
	}

	hecUrl := os.Getenv("HEC_URL")
	hecToken := os.Getenv("HEC_TOKEN")

	hecConfig := rabbitmqtohec.HecConfig{
		Token: hecToken,
		Url:   hecUrl,
	}
	mqConfig := rabbitmqtohec.MqConfig{
		Queue: sourceQueue,
		Url:   mqUrl,
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
