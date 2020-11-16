package appwrap

import (
	"context"
	"fmt"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type AppengineInfoK8s struct {
	c         context.Context
	clientset *kubernetes.Clientset
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

// ModuleHostname in K8s doesn't yet handle versions
func (ai AppengineInfoK8s) ModuleHostname(version, module, app string) (string, error) {
	if module == "" {
		module = ai.ModuleName()
	}
	if app == "" {
		app = ai.NativeProjectID()
	}

	domain := os.Getenv("K8S_DOMAIN")

	return fmt.Sprintf("%s-dot-%s.%s", module, app, domain), nil
}

func (ai AppengineInfoK8s) ModuleName() string {
	return os.Getenv("K8S_SERVICE")
}

func (ai AppengineInfoK8s) VersionID() string {
	return os.Getenv("K8S_VERSION")
}

func (ai AppengineInfoK8s) Zone() string {
	return os.Getenv("K8S_ZONE")
}

// ModuleHasTraffic not implemented in K8s
func (ai AppengineInfoK8s) ModuleHasTraffic(moduleName, moduleVersion string) (bool, error) {
	return false, nil
}

func (ai AppengineInfoK8s) NumInstances(moduleName, version string) (int, error) {
	namespace := ai.DataProjectID()
	deploymentName := ai.ModuleName()
	d, err := ai.clientset.AppsV1().Deployments(namespace).Get(deploymentName, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("Error getting deployment: %s", err)
	}

	return int(d.Status.AvailableReplicas), nil
}
