// +build appengine appenginevm

package appwrap

import (
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/module"
)

type AppengineInfo interface {
	ModuleName() string
	VersionID() string
	ModuleDefaultVersionID(moduleName string) (string, error)
	AppID() string
}

type AppengineInfoFromContext struct {
	c context.Context
}

func NewAppengineInfoFromContext(c context.Context) AppengineInfo {
	return AppengineInfoFromContext{c}
}

func (ai AppengineInfoFromContext) ModuleName() string {
	return appengine.ModuleName(ai.c)
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
