package appwrap

import (
	"context"
	"fmt"
	"os"
	"sync"

	"cloud.google.com/go/compute/metadata"
	"google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/appengine"
	istio "istio.io/client-go/pkg/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type AppengineInfo interface {
	DataProjectID() string
	NativeProjectID() string
	DataProjectNum() string
	NativeProjectNum() string
	InstanceID() string
	NodeName() string
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
	ClusterName() string
}

var (
	zone                        string
	zoneMtx                     sync.Mutex
	config                      *rest.Config
	k8sClientSet                *kubernetes.Clientset
	istioClientSet              *istio.Clientset
	cloudResourceManagerService *cloudresourcemanager.Service
	syncMap                     sync.Map
	instanceAttributes          sync.Map
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
		cloudResourceManagerService, err = cloudresourcemanager.NewService(context.Background())
		if err != nil {
			panic(fmt.Sprintf("Cannot create cloudresourcemanager client: %s", err.Error()))
		}
	}
}

func getZone(ctx context.Context) string {
	zoneMtx.Lock()
	defer zoneMtx.Unlock()

	if zone == "" {
		z, err := metadata.ZoneWithContext(ctx)
		if err != nil {
			panic(err)
		}
		zone = z
	}

	return zone
}

func getInstanceAttribute(ctx context.Context, attr string) string {
	if instanceAttribute, ok := instanceAttributes.Load(attr); ok {
		return instanceAttribute.(string)
	} else if attribute, err := metadata.InstanceAttributeValueWithContext(ctx, attr); err == nil {
		instanceAttributes.Store(attr, attribute)
		return attribute
	} else {
		return ""
	}
}

func getProjectNumber(projectID string) string {
	result, ok := syncMap.Load(projectID)
	if ok {
		return result.(string)
	}

	project, err := cloudResourceManagerService.Projects.Get(projectID).Do()
	if err != nil {
		panic(err)
	}

	projectNumber := fmt.Sprintf("%v", project.ProjectNumber)
	syncMap.Store(projectID, projectNumber)

	return projectNumber
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
