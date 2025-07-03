# appwrap
Interfaces to abstract appengine datastore and memache for Golang.

## DEPRECATED

This package is deprecated and no longer maintained. It was originally created to bridge the
gap between Appengine Standard and MVM (Managed VMs), later Appengine Flex, environments. 
If you use it, please consider migrating to using the official Google Cloud libraries for Go
directly.

## Original README

This is designed to work with the new appengine interfaces available as "google.golang.org/appengine" but it should be easy
to port to classic appengine.

Test with

```gcloud --project pendo-io beta emulators datastore start --host-port 127.0.0.1:8090 --consistency=1 --no-store-on-disk```

and set

```export DATASTORE_EMULATOR_HOST=127.0.0.1:8090
export DATASTORE_PROJECT_ID=pendo-io```
