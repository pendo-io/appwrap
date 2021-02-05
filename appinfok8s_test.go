package appwrap

import (
	"os"

	"context"
	. "gopkg.in/check.v1"
	networkingv1beta1 "istio.io/api/networking/v1beta1"
	"istio.io/client-go/pkg/apis/networking/v1beta1"
	istiofake "istio.io/client-go/pkg/clientset/versioned/fake"
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
	clientset := istiofake.NewSimpleClientset(&v1beta1.VirtualService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default-vs",
			Namespace: "theapp",
			Labels: map[string]string{
				"app": "default",
			},
		},
		Spec: networkingv1beta1.VirtualService{
			Tls: []*networkingv1beta1.TLSRoute{
				{
					Route: []*networkingv1beta1.RouteDestination{
						{
							Weight: int32(80),
							Destination: &networkingv1beta1.Destination{
								Subset: "version3",
							},
						},
						{
							Weight: int32(20),
							Destination: &networkingv1beta1.Destination{
								Subset: "version2",
							},
						},
						{
							Weight: int32(0),
							Destination: &networkingv1beta1.Destination{
								Subset: "version1",
							},
						},
					},
				},
			},
		},
	})
	ai := AppengineInfoK8s{
		c:        context.Background(),
		istioset: clientset,
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
}

func (t *AppengineInterfacesTest) TestModuleHasTraffic_Http(c *C) {
	clientset := istiofake.NewSimpleClientset(&v1beta1.VirtualService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default-vs",
			Namespace: "theapp",
			Labels: map[string]string{
				"app": "default",
			},
		},
		Spec: networkingv1beta1.VirtualService{
			Http: []*networkingv1beta1.HTTPRoute{
				{
					Route: []*networkingv1beta1.HTTPRouteDestination{
						{
							Weight: int32(80),
							Destination: &networkingv1beta1.Destination{
								Subset: "version3",
							},
						},
						{
							Weight: int32(20),
							Destination: &networkingv1beta1.Destination{
								Subset: "version2",
							},
						},
						{
							Weight: int32(0),
							Destination: &networkingv1beta1.Destination{
								Subset: "version1",
							},
						},
					},
				},
			},
		},
	})
	ai := AppengineInfoK8s{
		c:        context.Background(),
		istioset: clientset,
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
}
