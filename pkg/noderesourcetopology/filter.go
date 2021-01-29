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
	"strings"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	bm "k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	apiconfig "sigs.k8s.io/scheduler-plugins/pkg/apis/config"

	topologyv1alpha1 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha1"
	topoclientset "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/generated/clientset/versioned"
	topologyinformers "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/generated/informers/externalversions"
)

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = "NodeResourceTopologyMatch"
)

var _ framework.FilterPlugin = &NodeResourceTopologyMatch{}

type nodeTopologyMap map[string]topologyv1alpha1.NodeResourceTopology

type PolicyHandler interface {
	PolicyFilter(pod *v1.Pod, zoneMap topologyv1alpha1.ZoneList) *framework.Status
}

type PolicyHandlerMap map[apiconfig.TopologyManagerPolicy]PolicyHandler

// NodeResourceTopologyMatch plugin which run simplified version of TopologyManager's admit handler
type NodeResourceTopologyMatch struct {
	nodeTopologies    nodeTopologyMap
	nodeTopologyGuard sync.RWMutex
	policyHandlers    PolicyHandlerMap
}

type SingleNUMANodeHandler struct {
}

type PodLevelResourceHandler struct {
}

type NUMANode struct {
	NUMAID    int
	Resources v1.ResourceList
}

type NUMANodeList []NUMANode

// Name returns name of the plugin. It is used in logs, etc.
func (tm *NodeResourceTopologyMatch) Name() string {
	return Name
}

// getTopologyPolicies return true if we're working with such policy
func getTopologyPolicies(nodeTopologies nodeTopologyMap, nodeName string) []apiconfig.TopologyManagerPolicy {
	if nodeTopology, ok := nodeTopologies[nodeName]; ok {
		policies := make([]apiconfig.TopologyManagerPolicy, 0)
		for _, policy := range nodeTopology.TopologyPolicies {
			policies = append(policies, apiconfig.TopologyManagerPolicy(policy))
		}
		return policies
	}
	return nil
}

func extractResources(zone topologyv1alpha1.Zone) v1.ResourceList {
	res := make(v1.ResourceList)
	for _, resInfo := range zone.Resources {
		quantity, err := resource.ParseQuantity(resInfo.Allocatable.String())
		if err != nil {
			klog.Errorf("Failed to parse %s", resInfo.Allocatable.String())
			continue
		}
		res[v1.ResourceName(resInfo.Name)] = quantity
	}
	return res
}

func (sh SingleNUMANodeHandler) PolicyFilter(pod *v1.Pod, zones topologyv1alpha1.ZoneList) *framework.Status {
	klog.V(5).Infof("Single NUMA node handler")
	containers := []v1.Container{}
	containers = append(pod.Spec.InitContainers, pod.Spec.Containers...)

	// prepare NUMANodes list from zoneMap
	nodes := createNUMANodeList(zones)
	qos := v1qos.GetPodQOS(pod)
	for _, container := range containers {
		bitmask := bm.NewEmptyBitMask()
		bitmask.Fill()

		checkResourcesForNUMANodes(bitmask, nodes, container.Resources.Requests, qos)
		if bitmask.IsEmpty() {
			// definitly we can't align container, so we can't align a pod
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Cannot align container: %s", container.Name))
		}
	}
	return nil
}

func checkResourcesForNUMANodes(bitmask bm.BitMask, nodes NUMANodeList, resources v1.ResourceList, qos v1.PodQOSClass) {
	zeroQuantity := resource.MustParse("0")
	for resource, quantity := range resources {
		resourceBitmask := bm.NewEmptyBitMask()
		for _, numaNode := range nodes {
			numaQuantity, ok := numaNode.Resources[resource]
			// if can't find requested resource on the node - skip (don't set it as available NUMA node)
			// if unfound resource has 0 quantity probably this numa node can be considered
			if !ok && quantity.Cmp(zeroQuantity) != 0 {
				continue
			}
			// Check for the following:
			// 1. set numa node as possible node if resource is memory or Hugepages (until memory manager will not be merged and
			// memory will not be provided in CRD
			// 2. set numa node as possible node if resource is cpu and it's not guaranteed QoS, since cpu will flow
			// 3. set numa node as possible node if zero quantity for non existing resource was requested (TODO check topology manaager behaviour)
			// 4. otherwise check amount of resources
			if resource == v1.ResourceMemory ||
				strings.HasPrefix(string(resource), string(v1.ResourceHugePagesPrefix)) ||
				resource == v1.ResourceCPU && qos != v1.PodQOSGuaranteed ||
				quantity.Cmp(zeroQuantity) == 0 ||
				numaQuantity.Cmp(quantity) >= 0 {
				resourceBitmask.Add(numaNode.NUMAID)
			}
		}
		bitmask.And(resourceBitmask)
	}
}

func (ph PodLevelResourceHandler) PolicyFilter(pod *v1.Pod, zones topologyv1alpha1.ZoneList) *framework.Status {
	klog.V(5).Infof("Pod Level Resource handler")
	containers := []v1.Container{}
	containers = append(pod.Spec.InitContainers, pod.Spec.Containers...)

	resources := make(v1.ResourceList)

	for _, container := range containers {
		for resource, quantity := range container.Resources.Requests {
			if quan, ok := resources[resource]; ok {
				quantity.Add(quan)
			}
			resources[resource] = quantity
		}
	}

	nodes := createNUMANodeList(zones)
	bitmask := bm.NewEmptyBitMask()
	bitmask.Fill()
	checkResourcesForNUMANodes(bitmask, nodes, resources, v1qos.GetPodQOS(pod))

	if bitmask.IsEmpty() {
		// definitly we can't align container, so we can't align a pod
		return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Cannot align pod: %s", pod.Name))
	}
	return nil

}

func createNUMANodeList(zones topologyv1alpha1.ZoneList) NUMANodeList {
	nodes := make(NUMANodeList, 0)
	for _, zone := range zones {
		if zone.Type == "Node" {
			var numaID int
			fmt.Sscanf(zone.Name, "node-%d", &numaID)
			resources := extractResources(zone)
			nodes = append(nodes, NUMANode{NUMAID: numaID, Resources: resources})
		}
	}
	return nodes
}

// Filter Now only single-numa-node supported
func (tm *NodeResourceTopologyMatch) Filter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	if nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("Node is nil %s", nodeInfo.Node().Name))
	}
	if v1qos.GetPodQOS(pod) == v1.PodQOSBestEffort {
		return nil
	}

	nodeName := nodeInfo.Node().Name

	topologyPolicies := getTopologyPolicies(tm.nodeTopologies, nodeName)
	for _, policyName := range topologyPolicies {
		if handler, ok := tm.policyHandlers[policyName]; ok {
			tm.nodeTopologyGuard.RLock()
			zones := tm.nodeTopologies[nodeName].Zones
			tm.nodeTopologyGuard.RUnlock()
			if status := handler.PolicyFilter(pod, zones); status != nil {
				return status
			}
		} else {
			klog.V(5).Infof("Handler for policy %s not found", policyName)
		}
	}
	return nil
}

func (tm *NodeResourceTopologyMatch) onTopologyFromDelete(obj interface{}) {
	var nodeTopology *topologyv1alpha1.NodeResourceTopology
	switch t := obj.(type) {
	case *topologyv1alpha1.NodeResourceTopology:
		nodeTopology = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		nodeTopology, ok = t.Obj.(*topologyv1alpha1.NodeResourceTopology)
		if !ok {
			klog.Errorf("cannot convert to *v1alpha1.NodeResourceTopology: %v", t.Obj)
			return
		}
	default:
		klog.Errorf("cannot convert to *v1alpha1.NodeResourceTopology: %v", t)
		return
	}

	klog.V(5).Infof("delete event for scheduled NodeResourceTopology %s/%s ",
		nodeTopology.Namespace, nodeTopology.Name)

	tm.nodeTopologyGuard.Lock()
	defer tm.nodeTopologyGuard.Unlock()
	if _, ok := tm.nodeTopologies[nodeTopology.Name]; ok {
		delete(tm.nodeTopologies, nodeTopology.Name)
	}
}

func (tm *NodeResourceTopologyMatch) onTopologyUpdate(oldObj interface{}, newObj interface{}) {
	var nodeTopology *topologyv1alpha1.NodeResourceTopology
	switch t := newObj.(type) {
	case *topologyv1alpha1.NodeResourceTopology:
		nodeTopology = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		nodeTopology, ok = t.Obj.(*topologyv1alpha1.NodeResourceTopology)
		if !ok {
			klog.Errorf("cannot convert to *v1alpha1.NodeResourceTopology: %v", t.Obj)
			return
		}
	default:
		klog.Errorf("cannot convert to *v1alpha1.NodeResourceTopology: %v", t)
		return
	}
	klog.V(5).Infof("update event for scheduled NodeResourceTopology %s/%s ",
		nodeTopology.Namespace, nodeTopology.Name)

	tm.nodeTopologyGuard.Lock()
	defer tm.nodeTopologyGuard.Unlock()
	tm.nodeTopologies[nodeTopology.Name] = *nodeTopology
}

func (tm *NodeResourceTopologyMatch) onTopologyAdd(obj interface{}) {
	var nodeTopology *topologyv1alpha1.NodeResourceTopology
	switch t := obj.(type) {
	case *topologyv1alpha1.NodeResourceTopology:
		nodeTopology = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		nodeTopology, ok = t.Obj.(*topologyv1alpha1.NodeResourceTopology)
		if !ok {
			klog.Errorf("cannot convert to *v1alpha1.NodeResourceTopology: %v", t.Obj)
			return
		}
	default:
		klog.Errorf("cannot convert to *v1alpha1.NodeResourceTopology: %v", t)
		return
	}
	klog.V(5).Infof("add event for scheduled NodeResourceTopology %s/%s ",
		nodeTopology.Namespace, nodeTopology.Name)

	tm.nodeTopologyGuard.Lock()
	defer tm.nodeTopologyGuard.Unlock()
	tm.nodeTopologies[nodeTopology.Name] = *nodeTopology
}

// New initializes a new plugin and returns it.
func New(args runtime.Object, handle framework.FrameworkHandle) (framework.Plugin, error) {
	klog.V(5).Infof("creating new NodeResourceTopologyMatch plugin")
	tcfg, ok := args.(*apiconfig.NodeResourceTopologyMatchArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type NodeResourceTopologyMatchArgs, got %T", args)
	}

	topologyMatch := &NodeResourceTopologyMatch{
		policyHandlers: PolicyHandlerMap{
			apiconfig.SingleNUMANodeTopologyManagerPolicy: SingleNUMANodeHandler{},
			apiconfig.PodTopologyScope:                    PodLevelResourceHandler{},
		},
		nodeTopologies: nodeTopologyMap{},
	}

	kubeConfig, err := clientcmd.BuildConfigFromFlags(tcfg.MasterOverride, tcfg.KubeConfigPath)
	if err != nil {
		klog.Errorf("Cannot create kubeconfig based on: %s, %s, %v", tcfg.KubeConfigPath, tcfg.MasterOverride, err)
		return nil, err
	}

	topoClient, err := topoclientset.NewForConfig(kubeConfig)
	if err != nil {
		klog.Errorf("Cannot create clientset for NodeTopologyResource: %s, %s", kubeConfig, err)
		return nil, err
	}

	topologyInformerFactory := topologyinformers.NewSharedInformerFactory(topoClient, 0)
	nodeTopologyInformer := topologyInformerFactory.Topology().V1alpha1().NodeResourceTopologies()

	nodeTopologyInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    topologyMatch.onTopologyAdd,
			UpdateFunc: topologyMatch.onTopologyUpdate,
			DeleteFunc: topologyMatch.onTopologyFromDelete,
		},
	)

	ctx := context.Background()
	go nodeTopologyInformer.Informer().Run(ctx.Done())
	klog.V(5).Infof("start nodeTopologyInformer")
	topologyInformerFactory.Start(ctx.Done())
	topologyInformerFactory.WaitForCacheSync(ctx.Done())

	klog.V(5).Infof("WaitForCacheSync synchronyous")

	return topologyMatch, nil
}
