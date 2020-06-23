package appwrap

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"

	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	appengine "google.golang.org/api/appengine/v1"
)

var googleScopes = []string{appengine.CloudPlatformScope}

type AppengineInfoFlex struct {
	c context.Context
}

// Don't call this.  It exists to make NewAppengineInfoFromContext mockable
func InternalNewAppengineInfoFromContext(c context.Context) AppengineInfo {
	return AppengineInfoFlex{c: c}
}

var NewAppengineInfoFromContext = InternalNewAppengineInfoFromContext

func (ai AppengineInfoFlex) AppID() string {
	return os.Getenv("GOOGLE_CLOUD_PROJECT_OVERRIDE")
}

func (ai AppengineInfoFlex) InstanceID() string {
	return os.Getenv("GAE_INSTANCE")
}

func (ai AppengineInfoFlex) ModuleHostname(version, module, app string) (string, error) {
	if module == "" {
		module = ai.ModuleName()
	}
	if app == "" {
		app = ai.AppID()
	}
	if version == "" {
		return fmt.Sprintf("%s-dot-%s.appspot.com", module, app), nil
	} else {
		return fmt.Sprintf("%s-dot-%s-dot-%s.appspot.com", version, module, app), nil
	}
}

func (ai AppengineInfoFlex) ModuleName() string {
	return os.Getenv("GAE_SERVICE")
}

func (ai AppengineInfoFlex) VersionID() string {
	return os.Getenv("GAE_VERSION")
}

var (
	zone    string
	zoneMtx sync.Mutex
)

func (ai AppengineInfoFlex) Zone() string {
	zoneMtx.Lock()
	defer zoneMtx.Unlock()

	if zone == "" {
		z, err := metadata.Zone()
		if err != nil {
			panic(err)
		}
		zone = z
	}

	return zone
}

func (ai AppengineInfoFlex) ModuleHasTraffic(moduleName, moduleVersion string) (bool, error) {

	ae, err := appengine.New(webClient(ai.c))
	if err != nil {
		return false, err
	}

	svc := appengine.NewAppsServicesService(ae)
	call := svc.Get(ai.AppID(), moduleName)
	if resp, err := call.Do(); err != nil {
		return false, err
	} else {
		for version, allocation := range resp.Split.Allocations {
			if version == moduleVersion && allocation > 0 {
				return true, nil
			}
		}
	}

	return false, nil
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
	src, err := google.DefaultTokenSource(c, googleScopes...)
	if err != nil {
		panic("failed to create token source: " + err.Error())
	}

	return &http.Client{
		Transport: &oauth2.Transport{
			Source: src,
		},
	}

}
