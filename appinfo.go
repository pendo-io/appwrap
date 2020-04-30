package appwrap

import "google.golang.org/appengine"

type AppengineInfo interface {
	AppID() string
	InstanceID() string
	ModuleDefaultVersionID(moduleName string) (string, error)
	ModuleHasTraffic(moduleName, moduleVersion string) (bool, error)
	ModuleHostname(version, module, app string) (string, error)
	ModuleName() string
	NumInstances(moduleName, version string) (int, error)
	VersionID() string
	Zone() string
}
var (
	IsDevAppServer = false
	IsFlex      = appengine.IsFlex
	IsSecondGen = appengine.IsSecondGen
	IsStandard  = appengine.IsStandard
)
