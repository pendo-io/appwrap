// +build !appengine,!appenginevm

package appwrap

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

func (s *AppengineInterfacesTest) newDatastore() Datastore {
	cds, err := NewCloudDatastore(context.Background())
	if err != nil {
		panic(err)
	}

	return cds.Namespace(fmt.Sprintf("%s_%d", time.Now().Format("Jan__2_15_04_05_000000000"), rand.Uint64()))
}
