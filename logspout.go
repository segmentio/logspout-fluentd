package fluentd

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"time"
	"strconv"
	"os"

	"github.com/gliderlabs/logspout/router"
)

const defaultRetryCount = 10

var (
	retryCount uint
	econnResetErrStr string
)

func setRetryCount() {
	if count, err := strconv.Atoi(getopt("RETRY_COUNT", strconv.Itoa(defaultRetryCount))); err != nil {
		retryCount = uint(defaultRetryCount)
	} else {
		retryCount = uint(count)
	}
	debug("setting retryCount to:", retryCount)
}

func getopt(name, dfault string) string {
	value := os.Getenv(name)
	if value == "" {
		value = dfault
	}
	return value
}

func debug(v ...interface{}) {
	if os.Getenv("DEBUG") != "" {
		log.Println(v...)
	}
}

// FluentdAdapter is an adapter for streaming JSON to a fluentd collector.
type FluentdAdapter struct {
	conn  net.Conn
	route *router.Route
	transport router.AdapterTransport
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
			adapter.retry(json, err)
		}
	}
}

func (adapter *FluentdAdapter) retry(json []uint8, err error) error {
	if opError, ok := err.(*net.OpError); ok {
		if (opError.Temporary() && opError.Err.Error() != econnResetErrStr) || opError.Timeout() {
			retryErr := adapter.retryTemporary(json)
			if retryErr == nil {
				return nil
			}
		}
	}
	if reconnErr := adapter.reconnect(); reconnErr != nil {
		return reconnErr
	}
	if _, err = adapter.conn.Write(json); err != nil {
		log.Println("fluentd: reconnect failed")
		return err
	}
	log.Println("fluentd: reconnect successful")
	return nil
}

func (adapter *FluentdAdapter) retryTemporary(json []uint8) error {
	log.Printf("fluentd: retrying tcp up to %v times\n", retryCount)
	err := retryExp(func() error {
		_, err := adapter.conn.Write(json)
		if err == nil {
			log.Println("fluentd: retry successful")
			return nil
		}

		return err
	}, retryCount)

	if err != nil {
		log.Println("fluentd: retry failed")
		return err
	}

	return nil
}

func (adapter *FluentdAdapter) reconnect() error {
	log.Printf("fluentd: reconnecting up to %v times\n", retryCount)
	err := retryExp(func() error {
		conn, err := adapter.transport.Dial(adapter.route.Address, adapter.route.Options)
		if err != nil {
			return err
		}
		adapter.conn = conn
		return nil
	}, retryCount)

	if err != nil {
		return err
	}
	return nil
}

func retryExp(fun func() error, tries uint) error {
	try := uint(0)
	for {
		err := fun()
		if err == nil {
			return nil
		}

		try++
		if try > tries {
			return err
		}

		time.Sleep((1 << try) * 10 * time.Millisecond)
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
		transport: transport,
	}, nil
}

func init() {
	router.AdapterFactories.Register(NewFluentdAdapter, "fluentd-tcp")
	setRetryCount()
}
