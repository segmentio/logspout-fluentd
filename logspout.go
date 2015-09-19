package fluentd

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"time"

	"github.com/gliderlabs/logspout/router"
)

// FluentdAdapter is an adapter for streaming JSON to a fluentd collector.
type FluentdAdapter struct {
	conn  net.Conn
	route *router.Route
}

// Stream handles a stream of messages from Logspout. Implements router.logAdapter.
func (adapter *FluentdAdapter) Stream(logstream chan *router.Message) {
	for message := range logstream {
		timestamp := int32(time.Now().Unix())
		tag := "docker." + message.Container.Config.Hostname
		record := make(map[string]string)
		record["message"] = message.Data
		record["docker.hostname"] = message.Container.Config.Hostname
		record["docker.id"] = message.Container.ID
		record["docker.image"] = message.Container.Config.Image
		record["docker.name"] = message.Container.Name
		for label, value := range message.Container.Config.Labels {
			record["docker.label."+label] = value
		}
		data := []interface{}{tag, timestamp, record}

		json, err := json.Marshal(data)
		if err != nil {
			log.Println("fluentd-adapter: ", err)
			continue
		}

		_, err = adapter.conn.Write(json)
		if err != nil {
			log.Println("fluentd-adapter: ", err)
			continue
		}
	}
}

// NewFluentdAdapter creates a Logspout fluentd adapter instance.
func NewFluentdAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("tcp"))

	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	return &FluentdAdapter{
		conn:  conn,
		route: route,
	}, nil
}

func init() {
	router.AdapterFactories.Register(NewFluentdAdapter, "fluentd-tcp")
}
