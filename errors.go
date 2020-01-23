package appwrap

import "google.golang.org/appengine"

type MultiError = appengine.MultiError

var IsTimeoutError = appengine.IsTimeoutError
