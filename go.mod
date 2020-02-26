module github.com/pendo-io/appwrap

go 1.12

require (
	cloud.google.com/go v0.38.0
	github.com/cespare/xxhash v1.1.0
	github.com/codegangsta/inject v0.0.0-20140425184007-37d7f8432a3e // indirect
	github.com/fsnotify/fsnotify v1.4.7 // indirect
	github.com/go-martini/martini v0.0.0-20140425183230-de6438617700
	github.com/go-redis/redis v6.15.2+incompatible
	github.com/golang/protobuf v1.3.3
	github.com/google/go-cmp v0.4.0 // indirect
	github.com/googleapis/gax-go/v2 v2.0.5
	github.com/hpcloud/tail v1.0.0 // indirect
	github.com/masukomi/check v0.0.0-20150227023654-e0a72205c0f3 // indirect
	github.com/onsi/ginkgo v1.11.0 // indirect
	github.com/onsi/gomega v1.4.1 // indirect
	github.com/stretchr/testify v1.3.0
	golang.org/x/net v0.0.0-20200202094626-16171245cfb2
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	google.golang.org/api v0.7.0
	google.golang.org/appengine v1.6.1
	google.golang.org/genproto v0.0.0-20191115194625-c23dd37a84c9
	google.golang.org/grpc v1.27.0
	gopkg.in/check.v1 v1.0.0-00010101000000-000000000000
	gopkg.in/fsnotify.v1 v1.4.7 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v2 v2.0.0
)

replace gopkg.in/check.v1 => github.com/pendo-io/check v0.0.0-20150330131248-4a18b4639312

replace cloud.google.com/go => cloud.google.com/go v0.42.0

replace google.golang.org/grpc => google.golang.org/grpc v1.22.0

replace google.golang.org/api => google.golang.org/api v0.7.0
