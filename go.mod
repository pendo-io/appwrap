module github.com/pendo-io/appwrap

go 1.12

require (
	cloud.google.com/go v0.57.0
	cloud.google.com/go/datastore v1.1.0
	cloud.google.com/go/logging v1.0.0
	github.com/cespare/xxhash/v2 v2.1.0
	github.com/codegangsta/inject v0.0.0-20140425184007-37d7f8432a3e // indirect
	github.com/fsnotify/fsnotify v1.4.7 // indirect
	github.com/go-martini/martini v0.0.0-20140425183230-de6438617700
	github.com/go-redis/redis v6.15.2+incompatible
	github.com/golang/protobuf v1.4.0
	github.com/googleapis/gax-go/v2 v2.0.5
	github.com/hpcloud/tail v1.0.0 // indirect
	github.com/masukomi/check v0.0.0-20150227023654-e0a72205c0f3 // indirect
	github.com/onsi/ginkgo v1.11.0 // indirect
	github.com/onsi/gomega v1.4.1 // indirect
	github.com/stretchr/testify v1.4.0
	go.opencensus.io v0.22.3
	golang.org/x/net v0.0.0-20200501053045-e0ff5e5a1de5
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	google.golang.org/api v0.22.0
	google.golang.org/appengine v1.6.6
	google.golang.org/genproto v0.0.0-20200430143042-b979b6f78d84
	google.golang.org/grpc v1.29.1
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127
	gopkg.in/fsnotify.v1 v1.4.7 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v2 v2.2.2
)

replace gopkg.in/check.v1 => github.com/pendo-io/check v0.0.0-20150330131248-af5907e7f8ac
