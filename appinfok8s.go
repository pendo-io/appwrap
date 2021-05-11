package appwrap

import (
	"context"
	"fmt"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

type AppengineInfoK8s struct {
	c         context.Context
	clientset kubernetes.Interface
	datastore Datastore
}

type TrafficRecord struct {
	Service string
	Version string
	Weight int32
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
	//This uses GCE metadata service, which is available on the nodes of this pod
	return getZone()
}

//There is no one way to achieve traffic management in k8s. This implementation assumes using Istio.
//
//This implementation will attempt to read TrafficRecords from the datastore put there by traffic-monitor
//https://github.com/pendo-io/traffic-monitor
//keys must be moduleNmae-moduleVersion. We also double check the rest of the record for due diligence
func (ai AppengineInfoK8s) ModuleHasTraffic(moduleName, moduleVersion string) (bool, error) {
	//We're unable to set this in the constructor of AppengineInfoK8s since NewCloudDatastore makes a call to
	//NewAppengineInfoFromContext which will be an endless loop. Will only update if the datastore field is empty
	if ai.datastore == nil {
		datastore, err := NewCloudDatastore(ai.c)
		if err != nil {
			panic(fmt.Sprintf("Cannot create Traffic Monitor datastore client: %s", err.Error()))
		}
		ai.datastore = datastore.Namespace(dsNamespace)
	}

	var trafficRecord TrafficRecord
	dsKey := ai.datastore.NewKey("TrafficRecord", fmt.Sprintf("%s-%s", moduleName, moduleVersion), 0, nil)
	if err := ai.datastore.Get(dsKey, &trafficRecord); err != nil {
		if err == ErrNoSuchEntity {
			return false, nil
		} else {
			return false, err
		}
	}
	if trafficRecord.Service == moduleName && trafficRecord.Version == moduleVersion && trafficRecord.Weight > 0 {
		return true, nil
	} else {
		return false, nil
	}
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
