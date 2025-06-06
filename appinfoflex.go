package appwrap

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/appengine/v1"
)

var googleScopes = []string{appengine.CloudPlatformScope}

type AppengineInfoFlex struct {
	c context.Context
}

var NewAppengineInfoFromContext = InternalNewAppengineInfoFromContext

func (ai AppengineInfoFlex) DataProjectID() string {
	if project := os.Getenv("GOOGLE_CLOUD_DATA_PROJECT"); project != "" {
		return project
	}

	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}

func (ai AppengineInfoFlex) NativeProjectID() string {
	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}

func (ai AppengineInfoFlex) InstanceID() string {
	return os.Getenv("GAE_INSTANCE")
}

func (ai AppengineInfoFlex) ModuleHostname(version, module, app string) (string, error) {
	if module == "" {
		module = ai.ModuleName()
	}
	if app == "" {
		app = ai.NativeProjectID()
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

func (ai AppengineInfoFlex) Zone() string {
	return getZone(ai.c)
}

func (ai AppengineInfoFlex) DataProjectNum() string {
	return getProjectNumber(ai.DataProjectID())
}

func (ai AppengineInfoFlex) NativeProjectNum() string {
	return getProjectNumber(ai.NativeProjectID())
}

func (ai AppengineInfoFlex) NodeName() string { return "" }

func (ai AppengineInfoFlex) ClusterName() string { return "" }

func (ai AppengineInfoFlex) ModuleHasTraffic(moduleName, moduleVersion string) (bool, error) {

	ae, err := appengine.New(webClient(ai.c))
	if err != nil {
		return false, err
	}

	svc := appengine.NewAppsServicesService(ae)
	call := svc.Get(ai.NativeProjectID(), moduleName)
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

func (ai AppengineInfoFlex) NumInstances(moduleName, version string) (int, error) {
	ae, err := appengine.New(webClient(ai.c))
	if err != nil {
		return -1, err
	}

	svc := appengine.NewAppsServicesVersionsInstancesService(ae)
	call := svc.List(ai.NativeProjectID(), moduleName, version).PageSize(100)
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
