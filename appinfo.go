// +build appengine appenginevm

package appwrap

import (
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/module"
)

type AppengineInfo interface {
	AppID() string
	InstanceID() string
	ModuleDefaultVersionID(moduleName string) (string, error)
	ModuleName() string
	NumInstances(moduleName, version string) (int, error)
	VersionID() string
}

type AppengineInfoFromContext struct {
	c context.Context
}

func NewAppengineInfoFromContext(c context.Context) AppengineInfo {
	return AppengineInfoFromContext{c}
}

func (ai AppengineInfoFromContext) InstanceID() string {
	return appengine.InstanceID()
}

func (ai AppengineInfoFromContext) ModuleName() string {
	return appengine.ModuleName(ai.c)
}

func (ai AppengineInfoFromContext) NumInstances(moduleName, version string) (int, error) {
	return module.NumInstances(ai.c, moduleName, version)
}

func (ai AppengineInfoFromContext) VersionID() string {
	return appengine.VersionID(ai.c)
}

func (ai AppengineInfoFromContext) ModuleDefaultVersionID(moduleName string) (string, error) {
	return module.DefaultVersion(ai.c, moduleName)
}

func (ai AppengineInfoFromContext) AppID() string {
	return appengine.AppID(ai.c)
}
