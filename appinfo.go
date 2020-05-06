package appwrap

import "google.golang.org/appengine"

type AppengineInfo interface {
	AppID() string
	InstanceID() string
	ModuleDefaultVersionID(moduleName string) (string, error)
	ModuleHasTraffic(moduleName, moduleVersion string) (bool, error)
	// ModuleHostname returns the HTTP hostname to route to the given version, module, and app (project).
	// If version is "", the *live* version is used.
	// If module is "", the *caller's* module is used.
	// If app is "", the *caller's* project is used.
	// Example: ("", "", "") called on service = aggregations, project = pendo-io returns "aggregations-dot-pendo-io.appspot.com"
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
