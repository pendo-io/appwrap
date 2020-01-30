package appwrap

import (
	"context"

	"google.golang.org/appengine/urlfetch"
)

func UrlfetchTransport(c context.Context) *urlfetch.Transport {
	return &urlfetch.Transport{
		Context:                       c,
		AllowInvalidServerCertificate: true,
	}
}
