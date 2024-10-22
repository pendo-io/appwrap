package appwrap

import (
	"context"
	"fmt"
	"os"
	"sync"

	"cloud.google.com/go/compute/metadata"
	"google.golang.org/appengine"
	istio "istio.io/client-go/pkg/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type AppengineInfo interface {
	DataProjectID() string
	NativeProjectID() string
	InstanceID() string
	ModuleHasTraffic(moduleName, moduleVersion string) (bool, error)
	// ModuleHostname returns the HTTP hostname to route to the given version, module, and app (project).
	// If version is "", the *live* version is used.
	// If module is "", the *caller's* module is used.
	// If app is "", the *caller's* project is used.
	// Example: ("", "", "") called on service = aggregations, project = pendo-io returns "aggregations-dot-pendo-io.appspot.com"
	ModuleHostname(version, module, app string) (string, error)
	ModuleName() string
	NumInstances(moduleName, version string) (int, error)
	VersionID() string
	Zone() string
}

var (
	zone           string
	zoneMtx        sync.Mutex
	config         *rest.Config
	k8sClientSet   *kubernetes.Clientset
	istioClientSet *istio.Clientset
)

func init() {
	var err error
	if InKubernetes() {
		config, err = rest.InClusterConfig()
		if err != nil {
			panic(fmt.Sprintf("Cannot get K8s config: %s", err.Error()))
		}
		k8sClientSet, err = kubernetes.NewForConfig(config)
		if err != nil {
			panic(fmt.Sprintf("Cannot create K8s client: %s", err.Error()))
		}
		istioClientSet, err = istio.NewForConfig(config)
		if err != nil {
			panic(fmt.Sprintf("Cannot create Istio k8s client: %s", err.Error()))
		}
	}
}

func getZone() string {
	zoneMtx.Lock()
	defer zoneMtx.Unlock()

	if zone == "" {
		z, err := metadata.Zone()
		if err != nil {
			panic(err)
		}
		zone = z
	}

	return zone
}

func InKubernetes() bool {
	// if running in K8s, the following environment variable will always be set
	return os.Getenv("KUBERNETES_SERVICE_HOST") != ""
}

func K8sDomain() string {
	if InKubernetes() {
		return os.Getenv("K8S_DOMAIN")
	}
	return ""
}

var (
	LocalDebug     = os.Getenv("LOCAL_DEBUG") == "true"
	IsDevAppServer = false
	IsFlex         = appengine.IsFlex
	IsSecondGen    = appengine.IsSecondGen
	IsStandard     = appengine.IsStandard
)

// Don't call this.  It exists to make NewAppengineInfoFromContext mockable
func InternalNewAppengineInfoFromContext(c context.Context) AppengineInfo {
	if InKubernetes() {
		return AppengineInfoK8s{
			c:         c,
			clientset: k8sClientSet,
			istioset:  istioClientSet,
		}
	}

	return AppengineInfoFlex{c: c}
}
