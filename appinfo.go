package appwrap

type AppengineInfo interface {
	AppID() string
	InstanceID() string
	ModuleDefaultVersionID(moduleName string) (string, error)
	ModuleHostname(version, module, app string) (string, error)
	ModuleName() string
	NumInstances(moduleName, version string) (int, error)
	VersionID() string
}
