package appwrap

import (
	"context"
	"fmt"
	"os"
)

type AppengineInfoK8s struct {
	c context.Context
}

func (ai AppengineInfoK8s) AppID() string {
	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}

func (ai AppengineInfoK8s) InstanceID() string {
	return os.Getenv("K8S_POD")
}

// ModuleHostname in K8s doesn't yet handle versions
func (ai AppengineInfoK8s) ModuleHostname(version, module, app string) (string, error) {
	if module == "" {
		module = ai.ModuleName()
	}
	if app == "" {
		app = ai.AppID()
	}

	domain := os.Getenv("K8S_DOMAIN")

	return fmt.Sprintf("%s-dot-%s.%s", module, app, domain), nil
}

func (ai AppengineInfoK8s) ModuleName() string {
	return os.Getenv("K8S_SERVICE")
}

func (ai AppengineInfoK8s) VersionID() string {
	return os.Getenv("K8S_VERSION")
}

func (ai AppengineInfoK8s) Zone() string {
	return os.Getenv("K8S_ZONE")
}

// ModuleHasTraffic not implemented in K8s
func (ai AppengineInfoK8s) ModuleHasTraffic(moduleName, moduleVersion string) (bool, error) {
	return false, nil
}

// NumInstances not implemented in K8s
func (ai AppengineInfoK8s) NumInstances(moduleName, version string) (int, error) {
	return 0, nil
}
