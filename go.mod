module github.com/pendo-io/appwrap

go 1.12

require (
	cloud.google.com/go v0.57.0
	cloud.google.com/go/datastore v1.2.0
	cloud.google.com/go/logging v1.0.0
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/codegangsta/inject v0.0.0-20140425184007-37d7f8432a3e // indirect
	github.com/go-martini/martini v0.0.0-20140425183230-de6438617700
	github.com/go-redis/redis/v8 v8.5.0
	github.com/golang/protobuf v1.4.2
	github.com/googleapis/gax-go/v2 v2.0.5
	github.com/masukomi/check v0.0.0-20150227023654-e0a72205c0f3 // indirect
	github.com/stretchr/testify v1.6.1
	go.opencensus.io v0.22.4
	golang.org/x/net v0.0.0-20201202161906-c7110b5ffcbb
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	google.golang.org/api v0.29.0
	google.golang.org/appengine v1.6.6
	google.golang.org/genproto v0.0.0-20200904004341-0bd0a958aa1d
	google.golang.org/grpc v1.30.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15
	gopkg.in/yaml.v2 v2.3.0
)

replace gopkg.in/check.v1 => github.com/pendo-io/check v0.0.0-20150330131248-af5907e7f8ac
