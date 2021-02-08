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

package noderesourcetopology

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	topologyv1alpha1 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha1"
	listerv1alpha1 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/generated/listers/topology/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	intstr "k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"

	apiconfig "sigs.k8s.io/scheduler-plugins/pkg/apis/config"
)

const nicResourceName = "vendor/nic1"
const notExistingNICResourceName = "vendor/notexistingnic"
const containerName = "container1"

func makePodByResourceList(resources *v1.ResourceList) *v1.Pod {
	return &v1.Pod{Spec: v1.PodSpec{Containers: []v1.Container{{
		Resources: v1.ResourceRequirements{
			Requests: *resources,
			Limits:   *resources,
		},
	}},
	}}
}

// no no mock only indexer
type mockIndexer struct {
	nodeTopologies nodeTopologyMap
}

func (m mockIndexer) ByIndex(indexName, indexKey string) ([]interface{}, error)      { return nil, nil }
func (m mockIndexer) AddIndexers(cache.Indexers) error                               { return nil }
func (m mockIndexer) GetIndexers() cache.Indexers                                    { return nil }
func (m mockIndexer) ListIndexFuncValues(indexName string) []string                  { return nil }
func (m mockIndexer) Index(indexName string, obj interface{}) ([]interface{}, error) { return nil, nil }
func (m mockIndexer) IndexKeys(indexName, indexedValue string) ([]string, error)     { return nil, nil }

func (m mockIndexer) Add(interface{}) error               { return nil }
func (m mockIndexer) Update(obj interface{}) error        { return nil }
func (m mockIndexer) Delete(obj interface{}) error        { return nil }
func (m mockIndexer) List() []interface{}                 { return nil }
func (m mockIndexer) ListKeys() []string                  { return nil }
func (m mockIndexer) Replace([]interface{}, string) error { return nil }
func (m mockIndexer) Resync() error                       { return nil }
func (m mockIndexer) Get(obj interface{}) (item interface{}, exists bool, err error) {
	return nil, false, nil
}

func (m mockIndexer) GetByKey(key string) (item interface{}, exists bool, err error) {
	if v, ok := m.nodeTopologies[key]; ok {
		return &v, ok, nil
	}
	return nil, false, fmt.Errorf("Node topology is not found: %v", key)
}

func makeResourceListFromZones(zones topologyv1alpha1.ZoneList) v1.ResourceList {
	result := make(v1.ResourceList)
	for _, zone := range zones {
		for _, resInfo := range zone.Resources {
			resQuantity, err := resource.ParseQuantity(resInfo.Allocatable.String())
			if err != nil {
				klog.Errorf("Failed to parse %s", resInfo.Allocatable.String())
				continue
			}
			if quantity, ok := result[v1.ResourceName(resInfo.Name)]; ok {
				resQuantity.Add(quantity)
			}
			result[v1.ResourceName(resInfo.Name)] = resQuantity
		}
	}
	return result
}

func makePodByResourceListWithManyContainers(resources *v1.ResourceList, containerCount int) *v1.Pod {
	containers := []v1.Container{}

	for i := 0; i < containerCount; i++ {
		containers = append(containers, v1.Container{
			Resources: v1.ResourceRequirements{
				Requests: *resources,
				Limits:   *resources,
			},
		})
	}
	return &v1.Pod{Spec: v1.PodSpec{Containers: containers}}
}

func TestTopologyRequests(t *testing.T) {
	nodes := nodeTopologyMap{}
	nodes["default/node1"] = topologyv1alpha1.NodeResourceTopology{
		ObjectMeta:       metav1.ObjectMeta{Name: "node1"},
		TopologyPolicies: []string{string(apiconfig.SingleNUMANodeContainerLevel)},
		Zones: topologyv1alpha1.ZoneList{
			topologyv1alpha1.Zone{
				Name: "node-0",
				Type: "Node",
				Resources: topologyv1alpha1.ResourceInfoList{
					topologyv1alpha1.ResourceInfo{
						Name:        "cpu",
						Capacity:    intstr.Parse("20"),
						Allocatable: intstr.Parse("4"),
					}, topologyv1alpha1.ResourceInfo{
						Name:        "memory",
						Capacity:    intstr.Parse("8Gi"),
						Allocatable: intstr.Parse("8Gi"),
					}, topologyv1alpha1.ResourceInfo{
						Name:        nicResourceName,
						Capacity:    intstr.Parse("30"),
						Allocatable: intstr.Parse("10"),
					},
				},
			}, topologyv1alpha1.Zone{
				Name: "node-1",
				Type: "Node",
				Resources: topologyv1alpha1.ResourceInfoList{
					topologyv1alpha1.ResourceInfo{
						Name:        "cpu",
						Capacity:    intstr.Parse("30"),
						Allocatable: intstr.Parse("8"),
					}, topologyv1alpha1.ResourceInfo{
						Name:        "memory",
						Capacity:    intstr.Parse("8Gi"),
						Allocatable: intstr.Parse("8Gi"),
					}, topologyv1alpha1.ResourceInfo{
						Name:        nicResourceName,
						Capacity:    intstr.Parse("30"),
						Allocatable: intstr.Parse("10"),
					},
				},
			},
		},
	}

	nodes["default/node2"] = topologyv1alpha1.NodeResourceTopology{
		ObjectMeta:       metav1.ObjectMeta{Name: "node2"},
		TopologyPolicies: []string{string(apiconfig.SingleNUMANodeContainerLevel)},
		Zones: topologyv1alpha1.ZoneList{
			topologyv1alpha1.Zone{
				Name: "node-0",
				Type: "Node",
				Resources: topologyv1alpha1.ResourceInfoList{
					topologyv1alpha1.ResourceInfo{
						Name:        "cpu",
						Capacity:    intstr.Parse("20"),
						Allocatable: intstr.Parse("2"),
					}, topologyv1alpha1.ResourceInfo{
						Name:        "memory",
						Capacity:    intstr.Parse("8Gi"),
						Allocatable: intstr.Parse("4Gi"),
					}, topologyv1alpha1.ResourceInfo{
						Name:        nicResourceName,
						Capacity:    intstr.Parse("30"),
						Allocatable: intstr.Parse("5"),
					},
				},
			}, topologyv1alpha1.Zone{
				Name: "node-1",
				Type: "Node",
				Resources: topologyv1alpha1.ResourceInfoList{
					topologyv1alpha1.ResourceInfo{
						Name:        "cpu",
						Capacity:    intstr.Parse("30"),
						Allocatable: intstr.Parse("4"),
					}, topologyv1alpha1.ResourceInfo{
						Name:        "memory",
						Capacity:    intstr.Parse("8Gi"),
						Allocatable: intstr.Parse("4Gi"),
					}, topologyv1alpha1.ResourceInfo{
						Name:        nicResourceName,
						Capacity:    intstr.Parse("30"),
						Allocatable: intstr.Parse("2"),
					},
				},
			},
		},
	}

	nodes["default/node3"] = topologyv1alpha1.NodeResourceTopology{
		ObjectMeta:       metav1.ObjectMeta{Name: "node3"},
		TopologyPolicies: []string{string(apiconfig.SingleNUMANodePodLevel)},
		Zones: topologyv1alpha1.ZoneList{
			topologyv1alpha1.Zone{
				Name: "node-0",
				Type: "Node",
				Resources: topologyv1alpha1.ResourceInfoList{
					topologyv1alpha1.ResourceInfo{
						Name:        "cpu",
						Capacity:    intstr.Parse("20"),
						Allocatable: intstr.Parse("2"),
					}, topologyv1alpha1.ResourceInfo{
						Name:        "memory",
						Capacity:    intstr.Parse("8Gi"),
						Allocatable: intstr.Parse("4Gi"),
					}, topologyv1alpha1.ResourceInfo{
						Name:        nicResourceName,
						Capacity:    intstr.Parse("30"),
						Allocatable: intstr.Parse("5"),
					},
				},
			}, topologyv1alpha1.Zone{
				Name: "node-1",
				Type: "Node",
				Resources: topologyv1alpha1.ResourceInfoList{
					topologyv1alpha1.ResourceInfo{
						Name:        "cpu",
						Capacity:    intstr.Parse("30"),
						Allocatable: intstr.Parse("4"),
					}, topologyv1alpha1.ResourceInfo{
						Name:        "memory",
						Capacity:    intstr.Parse("8Gi"),
						Allocatable: intstr.Parse("4Gi"),
					}, topologyv1alpha1.ResourceInfo{
						Name:        nicResourceName,
						Capacity:    intstr.Parse("30"),
						Allocatable: intstr.Parse("2"),
					},
				},
			},
		},
	}
	node1Resources := makeResourceListFromZones(nodes["default/node1"].Zones)
	node1 := v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node1"},
		Status: v1.NodeStatus{
			Capacity:    node1Resources,
			Allocatable: node1Resources,
		},
	}

	node2Resources := makeResourceListFromZones(nodes["default/node2"].Zones)
	node2 := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node2"}, Status: v1.NodeStatus{
		Capacity:    node2Resources,
		Allocatable: node2Resources,
	},
	}

	node3Resources := makeResourceListFromZones(nodes["node3"].Zones)

	node3 := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node3"}, Status: v1.NodeStatus{
		Capacity:    node3Resources,
		Allocatable: node3Resources,
	},
	}

	// Test different QoS Guaranteed/Burstable/BestEffort
	topologyTests := []struct {
		pod            *v1.Pod
		nodeTopologies nodeTopologyMap
		name           string
		node           v1.Node
		wantStatus     *framework.Status
	}{
		{
			pod:            &v1.Pod{Spec: v1.PodSpec{Containers: []v1.Container{{}}}},
			nodeTopologies: nodes,
			name:           "Best effort QoS, pod fit",
			node:           node1,
			wantStatus:     nil,
		},
		{
			pod: makePodByResourceList(&v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(2, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse("2Gi"),
				nicResourceName:   *resource.NewQuantity(3, resource.DecimalSI)}),
			nodeTopologies: nodes,
			name:           "Guaranteed QoS, pod fit",
			node:           node2,
			wantStatus:     nil,
		},
		{
			pod: makePodByResourceList(&v1.ResourceList{
				v1.ResourceCPU:  *resource.NewQuantity(4, resource.DecimalSI),
				nicResourceName: *resource.NewQuantity(3, resource.DecimalSI)}),
			nodeTopologies: nodes,
			name:           "Burstable QoS, pod fit",
			node:           node2,
			wantStatus:     nil,
		},
		{
			pod: makePodByResourceList(&v1.ResourceList{
				v1.ResourceCPU:  *resource.NewQuantity(14, resource.DecimalSI),
				nicResourceName: *resource.NewQuantity(3, resource.DecimalSI)}),
			nodeTopologies: nodes,
			name:           "Burstable QoS, pod doesn't fit",
			node:           node2,
			wantStatus:     nil, // number of cpu is exceeded, but in case of burstable QoS for cpu resources we rely on fit.go
		},
		{
			pod: makePodByResourceList(&v1.ResourceList{
				v1.ResourceCPU:  *resource.NewQuantity(4, resource.DecimalSI),
				nicResourceName: *resource.NewQuantity(11, resource.DecimalSI)}),
			nodeTopologies: nodes,
			name:           "Burstable QoS, pod doesn't fit",
			node:           node2,
			wantStatus:     framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Cannot align container: %s", containerName)),
		},
		{
			pod: makePodByResourceList(&v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(9, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse("1Gi"),
				nicResourceName:   *resource.NewQuantity(3, resource.DecimalSI)}),
			nodeTopologies: nodes,
			name:           "Guaranteed QoS, pod doesn't fit",
			node:           node1,
			wantStatus:     framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Cannot align container: %s", containerName)),
		},
		{
			pod: makePodByResourceList(&v1.ResourceList{
				v1.ResourceCPU:             *resource.NewQuantity(2, resource.DecimalSI),
				v1.ResourceMemory:          resource.MustParse("1Gi"),
				notExistingNICResourceName: *resource.NewQuantity(0, resource.DecimalSI)}),
			nodeTopologies: nodes,
			name:           "Guaranteed QoS, pod fit",
			node:           node1,
			wantStatus:     nil,
		},
		{
			pod: makePodByResourceListWithManyContainers(&v1.ResourceList{
				v1.ResourceCPU:             *resource.NewQuantity(3, resource.DecimalSI),
				v1.ResourceMemory:          resource.MustParse("1Gi"),
				notExistingNICResourceName: *resource.NewQuantity(0, resource.DecimalSI)}, 3),
			nodeTopologies: nodes,
			name:           "Guaranteed QoS Topology Scope, pod doesn't fit",
			node:           node3,
			wantStatus:     framework.NewStatus(framework.Unschedulable, "Cannot align pod: "),
		},
		{
			pod: makePodByResourceListWithManyContainers(&v1.ResourceList{
				v1.ResourceCPU:             *resource.NewQuantity(1, resource.DecimalSI),
				v1.ResourceMemory:          resource.MustParse("1Gi"),
				notExistingNICResourceName: *resource.NewQuantity(0, resource.DecimalSI)}, 3),
			nodeTopologies: nodes,
			name:           "Guaranteed QoS Topology Scope, pod fit",
			node:           node3,
			wantStatus:     nil,
		},
	}

	nodeInfo := framework.NewNodeInfo()
	for _, test := range topologyTests {
		t.Run(test.name, func(t *testing.T) {
			tm := TopologyMatch{
				policyHandlers: PolicyHandlerMap{
					apiconfig.SingleNUMANodePodLevel:       SingleNUMAPodLevelHandler,
					apiconfig.SingleNUMANodeContainerLevel: SingleNUMAContainerLevelHandler,
				},
				namespace: metav1.NamespaceDefault,
				lister:    listerv1alpha1.NewNodeResourceTopologyLister(mockIndexer{nodeTopologies: test.nodeTopologies}),
			}
			nodeInfo.SetNode(&test.node)
			test.pod.Spec.Containers[0].Name = containerName
			gotStatus := tm.Filter(context.Background(), framework.NewCycleState(), test.pod, nodeInfo)

			fmt.Printf("test.Name: %v; status: %v\n", test.name, gotStatus)
			if !reflect.DeepEqual(gotStatus, test.wantStatus) {
				t.Errorf("status does not match: %v, want: %v\n", gotStatus, test.wantStatus)
			}
		})
	}

}
