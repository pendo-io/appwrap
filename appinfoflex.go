// +build appenginevm

package appwrap

import (
	"context"
	"errors"
	"net/http"
	"os"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	appengine "google.golang.org/api/appengine/v1"
)

var googleScopes = []string{appengine.CloudPlatformScope}

type AppengineInfoFlex struct {
	c context.Context
}

func NewAppengineInfoFromContext(c context.Context) AppengineInfo {
	return AppengineInfoFlex{c: c}
}

func (ai AppengineInfoFlex) AppID() string {
	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}

func (ai AppengineInfoFlex) InstanceID() string {
	return os.Getenv("GAE_INSTANCE")
}

func (ai AppengineInfoFlex) ModuleHostname(version, module, app string) (string, error) {
	return "", errors.New("ModuleHostname() isn't supported on flex")
}

func (ai AppengineInfoFlex) ModuleName() string {
	return os.Getenv("GAE_SERVICE")
}

func (ai AppengineInfoFlex) VersionID() string {
	return os.Getenv("GAE_VERSION")
}

func (ai AppengineInfoFlex) ModuleDefaultVersionID(moduleName string) (string, error) {
	ae, err := appengine.New(webClient(ai.c))
	if err != nil {
		return "", err
	}

	svc := appengine.NewAppsServicesService(ae)
	call := svc.Get(ai.AppID(), moduleName)
	if resp, err := call.Do(); err != nil {
		return "", err
	} else {
		for version, allocation := range resp.Split.Allocations {
			if allocation == 1 {
				return version, nil
			}
		}

		return "", errors.New("no default traffic split found")
	}
}

func (ai AppengineInfoFlex) NumInstances(moduleName, version string) (int, error) {
	ae, err := appengine.New(webClient(ai.c))
	if err != nil {
		return -1, err
	}

	svc := appengine.NewAppsServicesVersionsInstancesService(ae)
	call := svc.List(ai.AppID(), moduleName, version).PageSize(100)
	if resp, err := call.Do(); err != nil {
		return -1, err
	} else {
		return len(resp.Instances), nil
	}
}

func webClient(c context.Context) *http.Client {
	return &http.Client{
		Transport: &oauth2.Transport{
			Source: google.AppEngineTokenSource(c, googleScopes...),
		},
	}

}
