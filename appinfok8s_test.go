package appwrap

import (
	"os"

	"context"
	. "gopkg.in/check.v1"
	v1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func (t *AppengineInterfacesTest) TestModuleHostnameK8s(c *C) {
	ai := AppengineInfoK8s{}
	ck := func(v, s, a, expect string) {
		actual, err := ai.ModuleHostname(v, s, a)
		c.Assert(err, IsNil)
		c.Assert(actual, Equals, expect)
	}

	// GKE
	os.Setenv("K8S_SERVICE", "theservice")
	os.Setenv("K8S_DOMAIN", "domain.com")
	ck("", "", "", "theservice-dot-theapp.domain.com")
	ck("v", "", "", "theservice-dot-theapp.domain.com")
	ck("", "s", "", "s-dot-theapp.domain.com")
	ck("", "", "a", "theservice-dot-a.domain.com")
	ck("v", "s", "a", "s-dot-a.domain.com")
	os.Setenv("K8S_SERVICE", "")
	os.Setenv("K8S_DOMAIN", "")
}

func (t *AppengineInterfacesTest) TestNumInstancesK8s(c *C) {
	clientset := fake.NewSimpleClientset(&v1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "agentlogs-1",
			Namespace: "theapp",
			Labels: map[string]string{
				"app":     "agentlogs",
				"version": "jb-version",
			},
		},
		Status: v1.ReplicaSetStatus{
			ReadyReplicas: 2,
		},
	},
		&v1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "agentlogs-2",
				Namespace: "theapp",
				Labels: map[string]string{
					"app":     "agentlogs",
					"version": "jb-version-2",
				},
			},
			Status: v1.ReplicaSetStatus{
				ReadyReplicas: 3,
			},
		},
	)
	ai := AppengineInfoK8s{
		c:         context.Background(),
		clientset: clientset,
	}
	num, err := ai.NumInstances("agentlogs", "jb-version-2")

	c.Assert(err, IsNil)
	c.Assert(num, Equals, 3)
}

func (t *AppengineInterfacesTest) TestNumInstancesK8s_InstancesReportedWhenReady(c *C) {
	clientset := fake.NewSimpleClientset(&v1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "agentlogs-1",
			Namespace: "theapp",
			Labels: map[string]string{
				"app":     "jobs-large",
				"version": "cool-version",
			},
		},
		Status: v1.ReplicaSetStatus{
			ReadyReplicas:     2,
			AvailableReplicas: 3,
		},
	},
	)
	ai := AppengineInfoK8s{
		c:         context.Background(),
		clientset: clientset,
	}
	num, err := ai.NumInstances("jobs-large", "cool-version")

	c.Assert(err, IsNil)
	c.Assert(num, Equals, 2)
}

func (t *AppengineInterfacesTest) TestNumInstancesK8s_InstancesFromDifferentRSFound(c *C) {
	clientset := fake.NewSimpleClientset(&v1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "agentlogs-1",
			Namespace: "theapp",
			Labels: map[string]string{
				"app":     "jobs-large",
				"version": "liveversion",
			},
		},
		Status: v1.ReplicaSetStatus{
			ReadyReplicas:     2,
			AvailableReplicas: 3,
		},
	},
		&v1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "agentlogs-experiment",
				Namespace: "theapp",
				Labels: map[string]string{
					"app":     "jobs-large",
					"version": "liveversion",
				},
			},
			Status: v1.ReplicaSetStatus{
				ReadyReplicas:     2,
				AvailableReplicas: 3,
			},
		},
	)
	ai := AppengineInfoK8s{
		c:         context.Background(),
		clientset: clientset,
	}
	num, err := ai.NumInstances("jobs-large", "liveversion")

	c.Assert(err, IsNil)
	c.Assert(num, Equals, 4)
}

func (t *AppengineInterfacesTest) TestModuleHasTraffic(c *C) {
	mem := t.newDatastore()
	key1 := mem.NewKey("TrafficRecord", "default-version3", 0, nil)
	_, _ = mem.Put(key1, &TrafficRecord{
		Service: "default",
		Version: "version3",
		Weight:  80,
	})
	key2 := mem.NewKey("TrafficRecord", "default-version2", 0, nil)
	_, _ = mem.Put(key2, &TrafficRecord{
		Service: "default",
		Version: "version2",
		Weight:  20,
	})
	key3 := mem.NewKey("TrafficRecord", "default-version1", 0, nil)
	_, _ = mem.Put(key3, &TrafficRecord{
		Service: "default",
		Version: "version1",
		Weight:  0,
	})
	ai := AppengineInfoK8s{
		c:        context.Background(),
		datastore: mem,
	}

	hasTraffic, err := ai.ModuleHasTraffic("default", "version1")
	c.Assert(err, IsNil)
	c.Assert(hasTraffic, IsFalse)

	hasTraffic, err = ai.ModuleHasTraffic("default", "version2")
	c.Assert(err, IsNil)
	c.Assert(hasTraffic, IsTrue)

	hasTraffic, err = ai.ModuleHasTraffic("default", "version3")
	c.Assert(err, IsNil)
	c.Assert(hasTraffic, IsTrue)

	hasTraffic, err = ai.ModuleHasTraffic("default", "version4")
	c.Assert(err, IsNil)
	c.Assert(hasTraffic, IsFalse)
}
