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

type LogMessage struct {
	LogName      LogName
	CommonLabels logging.LoggerOption
	Entry        logging.Entry
}

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

func NewStackdriverClient(client *logging.Client) LoggerClientInterface {
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
	ProcessLogEntries()
	CreateLog(r *http.Request, labels map[string]string, logName LogName) Logging
	Close()
}

func NewStandardLoggingService(log Logging) LoggingServiceInterface {
	return &StandardLoggingService{
		log: log,
	}
}

type StandardLoggingService struct {
	log Logging
}

func (sls StandardLoggingService) ProcessLogEntries() {

}

func (sls StandardLoggingService) CreateLog(r *http.Request, labels map[string]string, logName LogName) Logging {
	return sls.log
}

func (sls StandardLoggingService) Close() {

}

// StackdriverLogging is a logger set up to work with an App Engine flexible environment
type StackdriverLoggingService struct {
	appInfo         AppengineInfo
	client          LoggerClientInterface
	log             Logging
	logCh           chan LogMessage
	resourceOptions logging.LoggerOption
}

// NewStackdriverLogging will return a new logger set up to work with an App Engine flexible environment
func NewStackdriverLoggingService(client LoggerClientInterface, appInfo AppengineInfo, logCh chan LogMessage, log Logging) LoggingServiceInterface {
	client.SetUpOnError()

	projectId := appInfo.AppID()
	versionSplit := strings.Split(appInfo.VersionID(), ".")
	versionId := versionSplit[0]
	moduleName := appInfo.ModuleName()

	return &StackdriverLoggingService{
		appInfo:         appInfo,
		client:          client,
		log:             log,
		logCh:           logCh,
		resourceOptions: basicAppEngineOptions(moduleName, projectId, versionId),
	}
}

// ProcessLogEntries will create a new log entry with a logger
func (sl *StackdriverLoggingService) ProcessLogEntries() {
	for {
		select {
		case logMessage, ok := <-sl.logCh:
			if ok {
				logger := sl.client.Logger(string(logMessage.LogName), logMessage.CommonLabels, sl.resourceOptions)
				logger.Log(logMessage.Entry)
			} else {
				break
			}

		}
	}
}

// Close will close the logging service and flush the logs.  This will close off the logging service from being able to receive logs
func (sl *StackdriverLoggingService) Close() {
	sl.client.Close()
	close(sl.logCh)
}

// CreateLog will return a new logger to use throughout a single request
func (sl *StackdriverLoggingService) CreateLog(r *http.Request, labels map[string]string, logName LogName) Logging {
	return NewStackdriverLogging(labels, logName, sl.logCh, r)
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
