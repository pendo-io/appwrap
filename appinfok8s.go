package appwrap

import (
	"context"
	"fmt"
	"os"

	istio "istio.io/client-go/pkg/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

type AppengineInfoK8s struct {
	c         context.Context
	clientset kubernetes.Interface
	istioset  istio.Interface
}

func (ai AppengineInfoK8s) DataProjectID() string {
	if project := os.Getenv("GOOGLE_CLOUD_DATA_PROJECT"); project != "" {
		return project
	}

	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}

func (ai AppengineInfoK8s) NativeProjectID() string {
	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}

func (ai AppengineInfoK8s) InstanceID() string {
	return os.Getenv("K8S_POD")
}

func (ai AppengineInfoK8s) NodeName() string {
	return os.Getenv("K8S_NODE_NAME")
}

func (ai AppengineInfoK8s) ClusterName() string {
	// https://cloud.google.com/kubernetes-engine/docs/concepts/workload-identity#instance_attributes
	return getInstanceAttribute(ai.c, "cluster-name")
}

// ModuleHostname in K8s doesn't yet handle versions
func (ai AppengineInfoK8s) ModuleHostname(version, module, app string) (string, error) {
	if module == "" {
		module = ai.ModuleName()
	}
	if app == "" {
		app = ai.NativeProjectID()
	}

	domain := os.Getenv("K8S_DOMAIN")

	host := fmt.Sprintf("%s-dot-%s.%s", module, app, domain)
	if version == "" {
		return host, nil
	} else {
		return fmt.Sprintf("%s-dot-%s", version, host), nil
	}
}

func (ai AppengineInfoK8s) ModuleName() string {
	return os.Getenv("K8S_SERVICE")
}

func (ai AppengineInfoK8s) VersionID() string {
	return os.Getenv("K8S_VERSION")
}

func (ai AppengineInfoK8s) Zone() string {
	//This uses GCE metadata service, which is available on the nodes of this pod
	return getZone(ai.c)
}

func (ai AppengineInfoK8s) DataProjectNum() string {
	return getProjectNumber(ai.DataProjectID())
}

func (ai AppengineInfoK8s) NativeProjectNum() string {
	return getProjectNumber(ai.NativeProjectID())
}

// There is no one way to achieve traffic management in k8s. This implementation assumes using Istio.
//
// This implementation will attempt to find a corresponding virtual service resource labeled app=<module_name>.
// We will attempt to find a destination rule will a subset named moduleVersion, and derive traffic weight from this
func (ai AppengineInfoK8s) ModuleHasTraffic(moduleName, moduleVersion string) (bool, error) {
	namespace := ai.NativeProjectID()
	labelSet := labels.Set{
		"app": moduleName,
	}
	if vsl, err := ai.istioset.NetworkingV1beta1().VirtualServices(namespace).List(ai.c, metav1.ListOptions{
		LabelSelector: labelSet.String(),
	}); err != nil {
		return false, fmt.Errorf("could not get virtual service for %s in namespace %s, error was: %s", moduleName, namespace, err.Error())
	} else {
		for _, svc := range vsl.Items {
			httpRoutes := svc.Spec.Http
			tlsRoutes := svc.Spec.Tls
			//Find a tls or http route. Although same named attributes, different structures, so we loop through independently
			for _, route := range tlsRoutes {
				for _, tlsDest := range route.Route {
					if tlsDest.Destination.Subset == moduleVersion && tlsDest.Weight > 0 {
						return true, nil
					}
				}
			}
			for _, route := range httpRoutes {
				for _, httpDest := range route.Route {
					if httpDest.Destination.Subset == moduleVersion && httpDest.Weight > 0 {
						return true, nil
					}
				}
			}
		}
	}
	return false, nil
}

func (ai AppengineInfoK8s) NumInstances(moduleName, version string) (int, error) {
	namespace := ai.NativeProjectID()
	labelSet := labels.Set{
		"version": version,
		"app":     moduleName,
	}
	if rs, err := ai.clientset.AppsV1().ReplicaSets(namespace).List(ai.c, metav1.ListOptions{
		LabelSelector: labelSet.String(),
	}); err != nil {
		return -1, fmt.Errorf("could not get number of instances for module %s version %s in namespace %s, error was: %s", moduleName, version, namespace, err.Error())
	} else {
		instances := 0
		for _, set := range rs.Items {
			instances += int(set.Status.ReadyReplicas)
		}
		return instances, nil
	}
}
