// +build !cdsemulator

package appwrap

func (s *AppengineInterfacesTest) newDatastore() Datastore {
	return NewLocalDatastore(false, nil)
}
