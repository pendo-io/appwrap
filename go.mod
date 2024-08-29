module github.com/pendo-io/appwrap

go 1.21

require (
	cloud.google.com/go/cloudtasks v1.12.8
	cloud.google.com/go/compute/metadata v0.3.0
	cloud.google.com/go/datastore v1.17.1
	cloud.google.com/go/errorreporting v0.3.0
	cloud.google.com/go/logging v1.10.0
	cloud.google.com/go/memcache v1.10.7
	cloud.google.com/go/redis v1.15.0
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/go-martini/martini v0.0.0-20140425183230-de6438617700
	github.com/go-redis/redis/v8 v8.11.5
	github.com/googleapis/gax-go/v2 v2.12.4
	github.com/pendo-io/gomemcache v0.0.0-20220318155316-448bc05805ee
	github.com/stretchr/testify v1.9.0
	go.opencensus.io v0.24.0
	go.opentelemetry.io/otel v1.24.0
	go.opentelemetry.io/otel/trace v1.24.0
	golang.org/x/oauth2 v0.21.0
	google.golang.org/api v0.183.0
	google.golang.org/appengine v1.6.8
	google.golang.org/genproto v0.0.0-20240528184218-531527333157
	google.golang.org/genproto/googleapis/api v0.0.0-20240604185151-ef581f913117
	google.golang.org/grpc v1.64.0
	google.golang.org/protobuf v1.34.1
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f
	gopkg.in/yaml.v2 v2.4.0
	istio.io/api v0.0.0-20220817194812-9ed7a41198c8
	istio.io/client-go v1.13.9
	k8s.io/api v0.23.1
	k8s.io/apimachinery v0.23.1
	k8s.io/client-go v0.23.1
)

require (
	cloud.google.com/go v0.114.0 // indirect
	cloud.google.com/go/auth v0.5.1 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.2 // indirect
	cloud.google.com/go/iam v1.1.8 // indirect
	cloud.google.com/go/longrunning v0.5.7 // indirect
	github.com/codegangsta/inject v0.0.0-20140425184007-37d7f8432a3e // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/evanphx/json-patch v4.12.0+incompatible // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gnostic v0.5.5 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/masukomi/check v0.0.0-20150227023654-e0a72205c0f3 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	golang.org/x/crypto v0.23.0 // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/sync v0.7.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/term v0.20.0 // indirect
	golang.org/x/text v0.15.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240528184218-531527333157 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	istio.io/gogo-genproto v0.0.0-20211208193508-5ab4acc9eb1e // indirect
	k8s.io/klog/v2 v2.30.0 // indirect
	k8s.io/kube-openapi v0.0.0-20211115234752-e816edb12b65 // indirect
	k8s.io/utils v0.0.0-20210930125809-cb0fa318a74b // indirect
	sigs.k8s.io/json v0.0.0-20211020170558-c049b76a60c6 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.1.2 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
)

replace gopkg.in/check.v1 => github.com/pendo-io/check v0.0.0-20180220194651-af5907e7f8ac

replace cloud.google.com/go/datastore v1.17.1 => github.com/pendo-io/google-cloud-go/datastore v1.0.1-0.20240829131339-abf77c09d0ae
