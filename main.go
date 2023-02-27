package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gopcua/opcua"
	opcua_debug "github.com/gopcua/opcua/debug"
	"github.com/gopcua/opcua/monitor"
	"github.com/gopcua/opcua/ua"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v2"
)

const exporterNamespace = "opcua_exporter"

var port = flag.Int("port", 9686, "Port to publish metrics on.")
// var endpoint = flag.String("endpoint", "opc.tcp://localhost:4096", "OPC UA Endpoint to connect to.")
var promPrefix = flag.String("prom-prefix", "", "Prefix will be appended to emitted prometheus metrics")
var nodeListFile = flag.String("config", "", "Path to a file from which to read the list of OPC UA nodes to monitor")
var configB64 = flag.String("config-b64", "", "Base64-encoded config JSON. Overrides -config")
var debug = flag.Bool("debug", false, "Enable debug logging")
var readTimeout = flag.Duration("read-timeout", 5*time.Second, "Timeout when waiting for OPCUA subscription messages")
var maxTimeouts = flag.Int("max-timeouts", 0, "The exporter will quit trying after this many read timeouts (0 to disable).")
var bufferSize = flag.Int("buffer-size", 64, "Maximum number of messages in the receive buffer")
var summaryInterval = flag.Duration("summary-interval", 5*time.Minute, "How frequently to print an event count summary")
var forcedReadInterval = flag.Duration("force-read", 0*time.Second, "How frequently to force a read from Server")

var securityMode = flag.String("security-mode", "None", "Available security modes: None,Sign")
var securityPolicy = flag.String("security-policy", "None", "Available security policies: None,Basic256")
var username = flag.String("username", "", "User authentication for a session with user.")
var password = flag.String("password", "", "User authentication for a session with password.")

// NodeConfig : Structure for representing OPCUA nodes to monitor.
type NodeConfig struct {
	NodeName     string            `yaml:"nodeName"`   // OPC UA node identifier
	MetricName   string            `yaml:"metricName"` // Prometheus metric name to emit
	MetricHelp   string            `yaml:"metricHelp"` // Prometheus metric help to emit
	MetricLables map[string]string `yaml:"metricLables"`
	ExtractBit   interface{}       `yaml:"extractBit,omitempty"` // Optional numeric value. If present and positive, extract just this bit and emit it as a boolean metric
}
// GlobalConf : Structure representing the yaml file
type GlobalConf struct {
	EndPoint string `yaml:"endPoint"` // OPC UA Endpoint to connect to.
	Nodes []NodeConfig `yaml:"nodes"` // List of OPCUA nodes
}

// MsgHandler interface can convert OPC UA Variant objects
// and emit prometheus metrics
type MsgHandler interface {
	Handle(v ua.Variant) error // compute the metric value and publish it
}

// HandlerMap maps OPC UA channel names to MsgHandlers
type HandlerMap map[string][]handlerMapRecord

type handlerMapRecord struct {
	config  NodeConfig
	handler MsgHandler
}

var startTime = time.Now()
var uptimeGauge prometheus.Gauge
var messageCounter prometheus.Counter
var eventSummaryCounter *EventSummaryCounter

var registry = prometheus.NewRegistry()

func init() {
	uptimeGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: exporterNamespace,
		Name:      "uptime_seconds",
		Help:      "Time in seconds since the OPCUA exporter started",
	})
	uptimeGauge.Set(time.Since(startTime).Seconds())
	registry.MustRegister(uptimeGauge)

	messageCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: exporterNamespace,
		Name:      "message_count",
		Help:      "Total number of OPCUA channel updates received by the exporter",
	})
	registry.MustRegister(messageCounter)

	eventSummaryCounter = NewEventSummaryCounter(*summaryInterval)
}

func main() {
	log.Print("Starting up.")
	flag.Parse()
	opcua_debug.Enable = *debug

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventSummaryCounter.Start(ctx)

	var nodes []NodeConfig
	var endpoint string
	var readError error
	if *configB64 != "" {
		log.Print("Using base64-encoded config")
		nodes, endpoint, readError = readConfigBase64(configB64)
	} else if *nodeListFile != "" {
		log.Printf("Reading config from %s", *nodeListFile)
		nodes, endpoint, readError = readConfigFile(*nodeListFile)
	} else {
		log.Fatal("Requires -config or -config-b64")
	}

	if readError != nil {
		log.Fatalf("Error reading config JSON: %v", readError)
	}

	client := getClient(&endpoint)
	log.Printf("Connecting to OPCUA server at %s", endpoint)
	if err := client.Connect(ctx); err != nil {
		log.Fatalf("Error connecting to OPC UA client: %v", err)
	} else {
		log.Print("Connected successfully")
	}
	defer client.Close()

	metricMap := createMetrics(&nodes)

	go forcedRead(ctx, client, metricMap)
	go setupMonitor(ctx, client, metricMap, *bufferSize)

	http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}))
	var listenOn = fmt.Sprintf(":%d", *port)
	log.Printf("Serving metrics on %s", listenOn)
	log.Fatal(http.ListenAndServe(listenOn, nil))
}

func getClient(endpoint *string) *opcua.Client {
	endpoints, err := opcua.GetEndpoints(*endpoint)
	if err != nil {
		return nil
	}

	ep := opcua.SelectEndpoint(endpoints, "None", ua.MessageSecurityModeFromString("None"))
	if ep == nil {
		return nil
	}

	opts := []opcua.Option{
		opcua.SecurityModeString(*securityMode),
		opcua.SecurityPolicy(*securityPolicy),
	}

	if *username == "" && *password == "" {
		opts = append(opts,
			opcua.AuthAnonymous(),
			opcua.SecurityFromEndpoint(ep, ua.UserTokenTypeAnonymous),
		)
	} else {
		opts = append(opts,
			opcua.AuthUsername(*username, *password),
			opcua.SecurityFromEndpoint(ep, ua.UserTokenTypeUserName),
		)
	}

	client := opcua.NewClient(*endpoint, opts...)
	return client
}

func forcedRead(ctx context.Context, client *opcua.Client, handlerMap HandlerMap){
	
	// Build lists of request nodes and node names
	var nameList []string
	var idList []*ua.ReadValueID
	for nodeName ,_ := range handlerMap {
		id, err := ua.ParseNodeID(nodeName)
		if err != nil {
			log.Fatalf("Invalid node id: %v", err)
		}
		nameList = append(nameList, nodeName)
		idList = append(idList, &ua.ReadValueID{NodeID: id})
	}
	// Mount request object
	req := &ua.ReadRequest{
		MaxAge: 0,
		NodesToRead: idList,
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	}
	// Read data
	resp, err := client.Read(req)
	if err != nil {
		log.Fatalf("Read failed: %s", err)
	}
	// Loop in results
	for i, val := range resp.Results {
		key := nameList[i]
		// Check if it is valid
		if val.Status != ua.StatusOK {
			log.Fatalf("Status not OK: %v", val.Status)
		}
		
		// Open handler to write value to it
		for _, handlerMapRec := range handlerMap[key] {
			err := handlerMapRec.handler.Handle(*val.Value)
			if err != nil {
				log.Printf("Error handling opcua value: %s (%s)\n", err, handlerMapRec.config.MetricName)
			}
			if *debug {
				log.Printf("Forced read of %s = %v", handlerMapRec.config.MetricName, val.Value.Value())
			}
		}
	}
}

// Subscribe to all the nodes and update the appropriate prometheus metrics on change
func setupMonitor(ctx context.Context, client *opcua.Client, handlerMap HandlerMap, bufferSize int) {
	m, err := monitor.NewNodeMonitor(client)
	if err != nil {
		log.Fatal(err)
	}

	var nodeList []string
	for nodeName := range handlerMap { // Node names are keys of handlerMap
		nodeList = append(nodeList, nodeName)
	}

	msgChan := make(chan *monitor.DataChangeMessage, bufferSize)
	params := opcua.SubscriptionParameters{Interval: time.Second}
	sub, err := m.ChanSubscribe(ctx, &params, msgChan, nodeList...)
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup(sub)

	lag := time.Millisecond * 10
	timeoutCount := 0
	var lastRead = time.Now()
	for {
		uptimeGauge.Set(time.Since(startTime).Seconds())
		select {
		case <-ctx.Done():
			return
		case msg := <-msgChan:
			if msg.Error != nil {
				log.Printf("[error ] sub=%d error=%s", sub.SubscriptionID(), msg.Error)
			} else if msg.Value == nil {
				log.Printf("nil value received for node %s", msg.NodeID)
			} else {
				if *debug {
					log.Printf("[message ] sub=%d ts=%s node=%s value=%v", sub.SubscriptionID(), msg.SourceTimestamp.UTC().Format(time.RFC3339), msg.NodeID, msg.Value.Value())
				}

				messageCounter.Inc()
				nodeID := msg.NodeID.String()
				eventSummaryCounter.Inc(nodeID)

				handleMessage(msg, handlerMap)
			}
			time.Sleep(lag)
		case <-time.After(*readTimeout):
			timeoutCount++
			log.Printf("Timeout %d wating for subscription messages", timeoutCount)
			if *maxTimeouts > 0 && timeoutCount >= *maxTimeouts {
				log.Fatalf("Max timeouts (%d) exceeded. Quitting.", *maxTimeouts)
			}
		}

		timeDiff := time.Now().Sub(lastRead)
		if *forcedReadInterval > lag {
			if timeDiff > *forcedReadInterval {
				lastRead = time.Now()
				go forcedRead(ctx, client, handlerMap)
			}
		}
	}

}

func cleanup(sub *monitor.Subscription) {
	log.Printf("stats: sub=%d delivered=%d dropped=%d", sub.SubscriptionID(), sub.Delivered(), sub.Dropped())
	sub.Unsubscribe()
}

func handleMessage(msg *monitor.DataChangeMessage, handlerMap HandlerMap) {
	nodeID := msg.NodeID.String()
	for _, handlerMapRec := range handlerMap[nodeID] {
		handler := handlerMapRec.handler
		value := msg.Value
		if *debug {
			log.Printf("Handling %s --> %s", nodeID, handlerMapRec.config.MetricName)
		}
		err := handler.Handle(*value)
		if err != nil {
			log.Printf("Error handling opcua value: %s (%s)\n", err, handlerMapRec.config.MetricName)
		}
	}
}

// Initialize a Prometheus gauge for each node. Return them as a map.
func createMetrics(nodeConfigs *[]NodeConfig) HandlerMap {
	handlerMap := make(HandlerMap)
	for _, nodeConfig := range *nodeConfigs {
		nodeName := nodeConfig.NodeName
		metricName := nodeConfig.MetricName
		mapRecord := handlerMapRecord{nodeConfig, createHandler(nodeConfig)}
		handlerMap[nodeName] = append(handlerMap[nodeName], mapRecord)
		log.Printf("Created prom metric %s for OPC UA node %s", metricName, nodeName)
	}

	return handlerMap
}

func createHandler(nodeConfig NodeConfig) MsgHandler {
	metricName := nodeConfig.MetricName
	metricHelp := nodeConfig.MetricHelp
	metricLables := nodeConfig.MetricLables
	if *promPrefix != "" {
		metricName = fmt.Sprintf("%s_%s", *promPrefix, metricName)
	}
	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        metricName,
		Help:        metricHelp,
		ConstLabels: metricLables,
	})
	registry.MustRegister(g)

	var handler MsgHandler
	if nodeConfig.ExtractBit != nil {
		extractBit := nodeConfig.ExtractBit.(int) // coerce interface to an integer
		handler = OpcuaBitVectorHandler{g, extractBit, *debug}
	} else {
		handler = OpcValueHandler{g}
	}
	return handler
}

func readConfigFile(path string) ([]NodeConfig, string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, "", err
	}

	f, err := os.Open(absPath)
	if err != nil {
		return nil, "", err
	}

	return parseConfigYAML(f)
}

func readConfigBase64(encodedConfig *string) ([]NodeConfig, string, error) {
	config, decodeErr := base64.StdEncoding.DecodeString(*encodedConfig)
	if decodeErr != nil {
		log.Fatal(decodeErr)
	}
	return parseConfigYAML(bytes.NewReader(config))
}

func parseConfigYAML(config io.Reader) ([]NodeConfig, string, error) {
	content, err := ioutil.ReadAll(config)
	if err != nil {
		return nil, "", err
	}
	var gconf GlobalConf
	err = yaml.Unmarshal(content, &gconf)
	log.Printf("Found %d nodes in config file.", len(gconf.Nodes))
	return gconf.Nodes, gconf.EndPoint, err
}
