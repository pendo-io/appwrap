module github.com/pendo-io/appwrap

go 1.12

require (
	cloud.google.com/go v0.43.0
	github.com/cespare/xxhash v1.1.0
	github.com/codegangsta/inject v0.0.0-20150114235600-33e0aa1cb7c0 // indirect
	github.com/go-martini/martini v0.0.0-20170121215854-22fa46961aab
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/golang/groupcache v0.0.0-20191227052852-215e87163ea7 // indirect
	github.com/golang/protobuf v1.3.3
	github.com/google/go-cmp v0.4.0 // indirect
	github.com/googleapis/gax-go/v2 v2.0.5
	github.com/masukomi/check v0.0.0-20150227023654-e0a72205c0f3 // indirect
	github.com/onsi/ginkgo v1.11.0 // indirect
	github.com/onsi/gomega v1.8.1 // indirect
	github.com/stretchr/testify v1.4.0
	go.opencensus.io v0.22.2 // indirect
	golang.org/x/net v0.0.0-20200114155413-6afb5195e5aa
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e // indirect
	golang.org/x/sys v0.0.0-20200113162924-86b910548bc1 // indirect
	google.golang.org/api v0.7.0
	google.golang.org/appengine v1.6.5
	google.golang.org/genproto v0.0.0-20200128133413-58ce757ed39b
	google.golang.org/grpc v1.27.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15
	gopkg.in/yaml.v2 v2.2.8
)

replace gopkg.in/check.v1 => github.com/pendo-io/check v0.0.0-20150330131248-4a18b4639312

replace cloud.google.com/go => cloud.google.com/go v0.42.0

replace google.golang.org/grpc => google.golang.org/grpc v1.22.0

replace google.golang.org/api => google.golang.org/api v0.7.0
