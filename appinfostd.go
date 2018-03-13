// +build appengine !appenginevm

package appwrap

import (
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/module"
)

type AppengineInfoStandard struct {
	c context.Context
}

func NewAppengineInfoFromContext(c context.Context) AppengineInfo {
	return AppengineInfoStandard{c}
}

func (ai AppengineInfoStandard) InstanceID() string {
	return appengine.InstanceID()
}

func (ai AppengineInfoStandard) ModuleHostname(version, module, app string) (string, error) {
	// return fmt.Sprintf("%s-dot-%s-dot-%s.appspot.com", strings.Split(aeInfo.VersionID(), ".")[0], aeInfo.ModuleName(), aeInfo.AppID()) // for later
	return appengine.ModuleHostname(ai.c, version, module, app)
}

func (ai AppengineInfoStandard) ModuleName() string {
	return appengine.ModuleName(ai.c)
}

func (ai AppengineInfoStandard) NumInstances(moduleName, version string) (int, error) {
	return module.NumInstances(ai.c, moduleName, version)
}

func (ai AppengineInfoStandard) VersionID() string {
	return appengine.VersionID(ai.c)
}

func (ai AppengineInfoStandard) ModuleDefaultVersionID(moduleName string) (string, error) {
	return module.DefaultVersion(ai.c, moduleName)
}

func (ai AppengineInfoStandard) AppID() string {
	return appengine.AppID(ai.c)
}
