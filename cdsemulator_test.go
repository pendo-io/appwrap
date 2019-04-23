// +build cdsemulator

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

func (s *AppengineInterfacesTest) TestNewCloudDatastoreThreadSafety(c *C) {
	dsClient = nil

	wg := &sync.WaitGroup{}
	startingLine := make(chan struct{})
	numGoroutines := 20000
	clients := make([]CloudDatastore, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			// Start all goroutines at once
			<-startingLine
			client, err := NewCloudDatastore(context.Background())
			c.Check(err, IsNil)
			clients[i] = client.(CloudDatastore)
		}()
	}
	close(startingLine)
	wg.Wait()

	authoritativeClient := clients[0].client
	for i := 0; i < numGoroutines; i++ {
		// should be the exact same pointer
		c.Assert(clients[i].client, Equals, authoritativeClient)
	}
}
