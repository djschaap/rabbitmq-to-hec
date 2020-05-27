package sendhec

import (
	"crypto/tls"
	"encoding/json"
	"github.com/NeowayLabs/wabbit"
	hec "github.com/fuyufjh/splunk-hec-go"
	"net/http"
	"strconv"
	"time"
)

var trace_mq bool

func FormatForHEC(m wabbit.Delivery) *hec.Event {
	var mq_message_body map[string]interface{}
	json.Unmarshal(m.Body(), &mq_message_body)

	var hecMessagePtr *hec.Event
	if _, ok := mq_message_body["event"]; ok {
		hecMessagePtr = hec.NewEvent(mq_message_body["event"])
		if mq_message_body["host"] != nil {
			v := mq_message_body["host"]
			hecMessagePtr.SetHost(v.(string))
		}
		if mq_message_body["index"] != nil {
			v := mq_message_body["index"]
			hecMessagePtr.SetIndex(v.(string))
		}
		if mq_message_body["source"] != nil {
			v := mq_message_body["source"]
			hecMessagePtr.SetSource(v.(string))
		}
		if mq_message_body["sourcetype"] != nil {
			v := mq_message_body["sourcetype"]
			hecMessagePtr.SetSourceType(v.(string))
		}
		if mq_message_body["fields"] != nil {
			v := mq_message_body["fields"]
			hecMessagePtr.SetFields(v.(map[string]interface{}))
		}
		if mq_message_body["time"] != nil {
			f, err := strconv.ParseFloat(mq_message_body["time"].(string), 64)
			if err == nil {
				time_s := int64(f)
				time_ns := int64((f - float64(time_s)) * 1000000000)
				v := time.Unix(time_s, time_ns)
				hecMessagePtr.SetTime(v)
			}
		}
	}

	return hecMessagePtr
}

func SendToHEC(hecUrl string, hecToken string, hecMessages []hec.Event) error {
	hec_client := hec.NewCluster(
		[]string{hecUrl},
		hecToken,
	)
	if true {
		hec_client.SetHTTPClient(&http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}})
	}

	for _, e := range hecMessages {
		err := hec_client.WriteBatch([]*hec.Event{&e})
		if err != nil {
			return err
		}
	}

	return nil
}
