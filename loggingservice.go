package appwrap

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/logging"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
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

type StackdriverClient struct {
	client *logging.Client
}

func newStackdriverClient(client *logging.Client) LoggerClientInterface {
	return &StackdriverClient{
		client: client,
	}
}

// Logger creates a new logger to upload log entries
func (c StackdriverClient) Logger(logID string, opts ...logging.LoggerOption) LoggerInterface {
	return c.client.Logger(logID, opts...)
}

// SetUpOnError sets up stackdriver logging to print errors to the Stderr whenever an internal error occurs on the logger
func (c StackdriverClient) SetUpOnError() {
	c.client.OnError = func(e error) {
		fmt.Fprintf(os.Stderr, "logging error: %v", e)
	}
}

// Ping will ping the stackdriver log
func (c StackdriverClient) Ping(ctx context.Context) error {
	return c.client.Ping(ctx)
}

// Close will close the logger and flush any pending log entries up to the stackdriver system
func (c StackdriverClient) Close() error {
	return c.client.Close()
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

	projectId := appInfo.AppID()
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
		Type: "gae_app",
		Labels: map[string]string{
			"module_id":  moduleName,
			"project_id": projectId,
			"version_id": versionId,
		},
	})
}
