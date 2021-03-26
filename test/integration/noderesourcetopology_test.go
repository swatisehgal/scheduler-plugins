/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"

	topologyclientset "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/generated/clientset/versioned"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler"
	schedapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	fwkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	testutils "k8s.io/kubernetes/test/integration/util"
	imageutils "k8s.io/kubernetes/test/utils/image"

	topologyv1alpha1 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha1"
	scheconfig "sigs.k8s.io/scheduler-plugins/pkg/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology"
	"sigs.k8s.io/scheduler-plugins/test/util"
)

func TestTopologyMatchPlugin(t *testing.T) {
	todo := context.TODO()
	ctx, cancelFunc := context.WithCancel(todo)
	testCtx := &testutils.TestContext{
		Ctx:      ctx,
		CancelFn: cancelFunc,
		CloseFn:  func() {},
	}
	registry := fwkruntime.Registry{noderesourcetopology.Name: noderesourcetopology.New}
	t.Log("create apiserver")
	_, config := util.StartApi(t, todo.Done())

	config.ContentType = "application/json"

	apiExtensionClient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	kubeConfigPath := util.BuildKubeConfigFile(config)
	if len(kubeConfigPath) == 0 {
		t.Fatal("Build KubeConfigFile failed")
	}
	defer os.RemoveAll(kubeConfigPath)

	t.Log("create crd")
	if _, err := apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, makeNodeResourceTopologyCRD(), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	cs := kubernetes.NewForConfigOrDie(config)

	topologyClient, err := topologyclientset.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	if err = wait.Poll(100*time.Millisecond, 3*time.Second, func() (done bool, err error) {
		groupList, _, err := cs.ServerGroupsAndResources()
		if err != nil {
			return false, nil
		}
		for _, group := range groupList {
			if group.Name == "topology.node.k8s.io" {
				return true, nil
			}
		}
		t.Log("waiting for crd api ready")
		return false, nil
	}); err != nil {
		t.Fatalf("Waiting for crd read time out: %v", err)
	}

	ns, err := cs.CoreV1().Namespaces().Create(ctx, &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("integration-test-%v", string(uuid.NewUUID()))}}, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		t.Fatalf("Failed to integration test ns: %v", err)
	}

	autoCreate := false
	t.Logf("namespaces %+v", ns.Name)
	_, err = cs.CoreV1().ServiceAccounts(ns.Name).Create(ctx, &v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: ns.Name}, AutomountServiceAccountToken: &autoCreate}, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		t.Fatalf("Failed to create ns default: %v", err)
	}

	testCtx.NS = ns
	testCtx.ClientSet = cs

	args := &scheconfig.NodeResourceTopologyMatchArgs{
		KubeConfigPath: kubeConfigPath,
		Namespaces:     []string{ns.Name},
	}

	profile := schedapi.KubeSchedulerProfile{
		SchedulerName: v1.DefaultSchedulerName,
		Plugins: &schedapi.Plugins{
			Filter: &schedapi.PluginSet{
				Enabled: []schedapi.Plugin{
					{Name: noderesourcetopology.Name},
				},
				Disabled: []schedapi.Plugin{
					{Name: "*"},
				},
			},
		},
		PluginConfig: []schedapi.PluginConfig{
			{
				Name: noderesourcetopology.Name,
				Args: args,
			},
		},
	}

	testCtx = util.InitTestSchedulerWithOptions(
		t,
		testCtx,
		true,
		scheduler.WithProfiles(profile),
		scheduler.WithFrameworkOutOfTreeRegistry(registry),
	)
	t.Log("init scheduler success")
	defer testutils.CleanupTest(t, testCtx)

	// Create a Node.
	nodeName1 := "fake-node-1"
	node1 := st.MakeNode().Name("fake-node-1").Label("node", nodeName1).Obj()
	node1.Status.Allocatable = v1.ResourceList{
		v1.ResourceCPU:  *resource.NewQuantity(4, resource.DecimalSI),
		v1.ResourcePods: *resource.NewQuantity(32, resource.DecimalSI),
	}
	node1.Status.Capacity = v1.ResourceList{
		v1.ResourceCPU:  *resource.NewQuantity(4, resource.DecimalSI),
		v1.ResourcePods: *resource.NewQuantity(32, resource.DecimalSI),
	}
	n1, err := cs.CoreV1().Nodes().Create(ctx, node1, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create Node %q: %v", nodeName1, err)
	}

	t.Logf(" Node 1 created: %v", n1)
	// Create another Node.
	nodeName2 := "fake-node-2"
	node2 := st.MakeNode().Name("fake-node-2").Label("node", nodeName2).Obj()
	node2.Status.Allocatable = v1.ResourceList{
		v1.ResourceCPU:  *resource.NewQuantity(4, resource.DecimalSI),
		v1.ResourcePods: *resource.NewQuantity(32, resource.DecimalSI),
	}
	node2.Status.Capacity = v1.ResourceList{
		v1.ResourceCPU:  *resource.NewQuantity(4, resource.DecimalSI),
		v1.ResourcePods: *resource.NewQuantity(32, resource.DecimalSI),
	}
	n2, err := cs.CoreV1().Nodes().Create(ctx, node2, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create Node %q: %v", nodeName2, err)
	}
	t.Logf(" Node 2 created: %v", n2)
	nodeList, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	t.Logf(" NodeList: %v", nodeList)
	pause := imageutils.GetPauseImageName()
	for _, tt := range []struct {
		name                   string
		pods                   []*v1.Pod
		nodeResourceTopologies []*topologyv1alpha1.NodeResourceTopology
		expectedNode           string
	}{
		{
			name: "Filtering out nodes that cannot fit resources on a single numa node ",
			pods: []*v1.Pod{
				withContainer(st.MakePod().Namespace(ns.Name).Name("topology-aware-scheduler-pod").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Obj(), pause),
			},
			nodeResourceTopologies: []*topologyv1alpha1.NodeResourceTopology{
				{
					ObjectMeta:       metav1.ObjectMeta{Name: "fake-node-1", Namespace: ns.Name},
					TopologyPolicies: []string{string(topologyv1alpha1.SingleNUMANodeContainerLevel)},
					Zones: topologyv1alpha1.ZoneList{
						topologyv1alpha1.Zone{
							Name: "node-0",
							Type: "Node",
							Resources: topologyv1alpha1.ResourceInfoList{
								topologyv1alpha1.ResourceInfo{
									Name:        "cpu",
									Allocatable: intstr.FromString("2"),
									Capacity:    intstr.FromString("2"),
								},
							},
						},
						topologyv1alpha1.Zone{
							Name: "node-1",
							Type: "Node",
							Resources: topologyv1alpha1.ResourceInfoList{
								topologyv1alpha1.ResourceInfo{
									Name:        "cpu",
									Allocatable: intstr.FromString("2"),
									Capacity:    intstr.FromString("2"),
								},
							},
						},
					},
				},
				{
					ObjectMeta:       metav1.ObjectMeta{Name: "fake-node-2", Namespace: ns.Name},
					TopologyPolicies: []string{string(topologyv1alpha1.SingleNUMANodeContainerLevel)},
					Zones: topologyv1alpha1.ZoneList{
						topologyv1alpha1.Zone{
							Name: "node-0",
							Type: "Node",
							Resources: topologyv1alpha1.ResourceInfoList{
								topologyv1alpha1.ResourceInfo{
									Name:        "cpu",
									Allocatable: intstr.FromString("4"),
									Capacity:    intstr.FromString("4"),
								},
							},
						},
						topologyv1alpha1.Zone{
							Name: "node-1",
							Type: "Node",
							Resources: topologyv1alpha1.ResourceInfoList{
								topologyv1alpha1.ResourceInfo{
									Name:        "cpu",
									Allocatable: intstr.FromString("0"),
									Capacity:    intstr.FromString("0"),
								},
							},
						},
					},
				},
			},
			expectedNode: "fake-node-2",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Start-topology-match-test %v", tt.name)

			defer cleanupNodeResourceTopologies(ctx, topologyClient, tt.nodeResourceTopologies)

			if err := createNodeResourceTopologies(ctx, topologyClient, tt.nodeResourceTopologies); err != nil {
				t.Fatal(err)
			}

			defer testutils.CleanupPods(cs, t, tt.pods)
			// Create Pods
			for i := range tt.pods {
				t.Logf("Creating Pod %q", tt.pods[i].Name)
				_, err := cs.CoreV1().Pods(ns.Name).Create(context.TODO(), tt.pods[i], metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("Failed to create Pod %q: %v", tt.pods[i].Name, err)
				}
			}

			for i, p := range tt.pods {

				// Wait for the pod to be scheduled.
				err = wait.Poll(1*time.Second, 20*time.Second, func() (bool, error) {
					return podScheduled(cs, ns.Name, p.Name), nil
				})
				if err != nil {
					t.Errorf("pod %q to be scheduled, error: %v", tt.pods[i].Name, err)
				}

				t.Logf(" p scheduled: %v", p)
				podList, err := cs.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{})
				if err != nil {
					t.Logf("Error listing pods %v", err)
				}
				t.Logf(" podList: %v", podList)

				// The other pods should be scheduled on the small nodes.
				if p.Spec.NodeName == tt.expectedNode {
					t.Logf("Pod %q is on a node as expected.", p.Name)
					continue
				} else {
					t.Errorf("Pod %s is expected on node %s, but found on node %s",
						tt.pods[i].Name, tt.expectedNode, p.Spec.NodeName)
				}

			}
			t.Logf("case %v finished", tt.name)
		})
	}
}

func makeNodeResourceTopologyCRD() *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: "noderesourcetopologies.topology.node.k8s.io",
			Annotations: map[string]string{
				"api-approved.kubernetes.io": "https://github.com/kubernetes/enhancements/pull/1870",
			},
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "topology.node.k8s.io",
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   "noderesourcetopologies",
				Singular: "noderesourcetopology",
				ShortNames: []string{
					"node-res-topo",
				},
				Kind: "NodeResourceTopology",
			},
			Scope: "Namespaced",
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name: "v1alpha1",
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							Type: "object",
							Properties: map[string]apiextensionsv1.JSONSchemaProps{
								"topologyPolicies": {
									Type: "array",
									Items: &apiextensionsv1.JSONSchemaPropsOrArray{
										Schema: &apiextensionsv1.JSONSchemaProps{
											Type: "string",
										},
									},
								},
								"zones": {
									Type: "array",
									Items: &apiextensionsv1.JSONSchemaPropsOrArray{
										Schema: &apiextensionsv1.JSONSchemaProps{
											Type: "object",
											Properties: map[string]apiextensionsv1.JSONSchemaProps{
												"name": {
													Type: "string",
												},
												"type": {
													Type: "string",
												},
												"parent": {
													Type: "string",
												},
												"resources": {
													Type: "array",
													Items: &apiextensionsv1.JSONSchemaPropsOrArray{
														Schema: &apiextensionsv1.JSONSchemaProps{
															Type: "object",
															Properties: map[string]apiextensionsv1.JSONSchemaProps{
																"name": {
																	Type: "string",
																},
																"capacity": {
																	XIntOrString: true,
																},
																"allocatable": {
																	XIntOrString: true,
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
					Served:  true,
					Storage: true,
				},
			},
		},
	}
}

func createNodeResourceTopologies(ctx context.Context, topologyClient *topologyclientset.Clientset, noderesourcetopologies []*topologyv1alpha1.NodeResourceTopology) error {
	for _, nrt := range noderesourcetopologies {
		_, err := topologyClient.TopologyV1alpha1().NodeResourceTopologies(nrt.Namespace).Create(ctx, nrt, metav1.CreateOptions{})
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		crdInstance, err := topologyClient.TopologyV1alpha1().NodeResourceTopologies(nrt.Namespace).Get(ctx, nrt.Name, metav1.GetOptions{})
		if err != nil {
			klog.Infof(" Error in createNodeResourceTopologies not able to Get the CRD instance %s %v", nrt.Name, err)
		}
		crdInstJson, err := json.Marshal(crdInstance)
		klog.Infof(string(crdInstJson))
		if err != nil {
			klog.Infof(" Error in createNodeResourceTopologies marshalling crdInstJson %v", err)
		}
	}
	return nil
}

func cleanupNodeResourceTopologies(ctx context.Context, topologyClient *topologyclientset.Clientset, noderesourcetopologies []*topologyv1alpha1.NodeResourceTopology) {
	for _, nrt := range noderesourcetopologies {
		err := topologyClient.TopologyV1alpha1().NodeResourceTopologies(nrt.Namespace).Delete(ctx, nrt.Name, metav1.DeleteOptions{})
		if err != nil {
			klog.Errorf("clean up NodeResourceTopologies (%v/%v) error %s", nrt.Namespace, nrt.Name, err.Error())
		}
	}
}

func withContainer(pod *v1.Pod, image string) *v1.Pod {
	pod.Spec.Containers[0].Name = "con0"
	pod.Spec.Containers[0].Image = image
	return pod
}
