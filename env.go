package appwrap

import "google.golang.org/appengine"

// CloudPlatformScope is the scope for general operations in GCP
const CloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

var (
	IsDevAppServer = appengine.IsDevAppServer

	IsStandard  = appengine.IsStandard
	IsFlex      = appengine.IsFlex
	IsSecondGen = appengine.IsSecondGen
)
