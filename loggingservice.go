package appwrap

import (
	"cloud.google.com/go/logging"
	"context"
	"fmt"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	"net/http"
	"os"
	"strings"
	"sync"
)

type LogName string

type LoggerClientInterface interface {
	Logger(logID string, opts ...logging.LoggerOption) LoggerInterface
	Close() error
	SetUpOnError()
	Ping(ctx context.Context) error
}

type LoggerInterface interface {
	Log(logging.Entry)
	Flush() error
}

type LoggingClient struct {
	mtx    sync.Mutex
	client *logging.Client
}

var globalLoggingClient LoggingClient

func (lc *LoggingClient) checkClientInitialization() error {
	if lc.client == nil {
		return fmt.Errorf("logging client is not initialized")
	}
	return nil
}

func (lc *LoggingClient) Logger(logID string, opts ...logging.LoggerOption) LoggerInterface {
	if err := lc.checkClientInitialization(); err != nil {
		panic(err)
	}
	return lc.client.Logger(logID, opts...)
}

func (lc *LoggingClient) SetUpOnError() {
	if err := lc.checkClientInitialization(); err != nil {
		panic(err)
	}
	lc.client.OnError = func(e error) {
		fmt.Fprintf(os.Stderr, "logging error: %v", e)
	}
}

func (lc *LoggingClient) Ping(ctx context.Context) error {
	if err := lc.checkClientInitialization(); err != nil {
		return err
	}
	return lc.client.Ping(ctx)
}

func (lc *LoggingClient) Close() error {
	lc.mtx.Lock()
	defer lc.mtx.Unlock()
	err := lc.client.Close()
	lc.client = nil
	return err
}

func CloseGlobalLoggingClient() error {
	return globalLoggingClient.Close()
}

func GetOrCreateLoggingClient() *logging.Client {
	if globalLoggingClient.client != nil {
		return globalLoggingClient.client
	}

	globalLoggingClient.mtx.Lock()
	defer globalLoggingClient.mtx.Unlock()
	if globalLoggingClient.client == nil {
		c := context.Background()
		aeInfo := NewAppengineInfoFromContext(c)
		client, err := logging.NewClient(c, fmt.Sprintf("projects/%s", aeInfo.NativeProjectID()))
		if err != nil {
			panic(fmt.Sprintf("failed to create logging client %s", err.Error()))
		}
		globalLoggingClient.client = client
	}
	return globalLoggingClient.client
}

func newLoggingClient() LoggerClientInterface {
	client := GetOrCreateLoggingClient()
	return &LoggingClient{client: client}
}

type LoggingServiceInterface interface {
	CreateLog(labels map[string]string, logName LogName, r *http.Request, traceId string) Logging
	Close()
}

func newStandardLoggingService(log Logging) LoggingServiceInterface {
	return &StandardLoggingService{
		log: log,
	}
}

type StandardLoggingService struct {
	log Logging
}

// CreateLog simply returns the App Engine logger for legacy standard environments
func (sls StandardLoggingService) CreateLog(labels map[string]string, logName LogName, r *http.Request, traceId string) Logging {
	return sls.log
}

// Close is not implemented since there is nothing to shut down on App Engine's standard logger
func (sls StandardLoggingService) Close() {

}

// StackdriverLogging is a logger set up to work with an App Engine flexible environment
type StackdriverLoggingService struct {
	appInfo         AppengineInfo
	client          LoggerClientInterface
	log             Logging
	resourceOptions logging.LoggerOption
}

// NewStackdriverLogging will return a new logger set up to work with an App Engine flexible environment
func newStackdriverLoggingService(client LoggerClientInterface, appInfo AppengineInfo, log Logging) LoggingServiceInterface {
	client.SetUpOnError()

	projectId := appInfo.NativeProjectID()
	versionSplit := strings.Split(appInfo.VersionID(), ".")
	versionId := versionSplit[0]
	moduleName := appInfo.ModuleName()

	return &StackdriverLoggingService{
		appInfo:         appInfo,
		client:          client,
		log:             log,
		resourceOptions: basicAppEngineOptions(moduleName, projectId, versionId),
	}
}

// Close will close the logging service and flush the logs.  This will close off the logging service from being able to receive logs
func (sl *StackdriverLoggingService) Close() {
	if err := sl.client.Close(); err != nil {
		sl.log.Errorf("Unable to close stackdriver logging client: %+v", err)
	}
}

// CreateLog will return a new logger to use throughout a single request
//
// Every Request on AppEngine includes a header X-Cloud-Trace-Context which contains a traceId.  This id can be
// used to correlate various logs together from a request.  Stackdriver will leverage this field when producing
// a child/parent relationship in the Stackdriver UI. However, not all logs include a request object.  In that
// instance we will fallback to the provided traceId
func (sl *StackdriverLoggingService) CreateLog(labels map[string]string, logName LogName, r *http.Request, traceId string) Logging {
	return sl.newStackdriverLogging(labels, logName, r, traceId)
}

// basicAppEngineOptions creates labels that will map the correct app engine instance to Stackdriver
func basicAppEngineOptions(moduleName, projectId, versionId string) logging.LoggerOption {
	return logging.CommonResource(&mrpb.MonitoredResource{
		Type: monitoredType(),
		Labels: map[string]string{
			"module_id":  moduleName,
			"project_id": projectId,
			"version_id": versionId,
		},
	})
}

func monitoredType() string {
	if InKubernetes() {
		return "k8s_pod"
	} else {
		return "gae_app"
	}
}
