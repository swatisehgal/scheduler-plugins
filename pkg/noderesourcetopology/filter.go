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

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	bm "k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	apiconfig "sigs.k8s.io/scheduler-plugins/pkg/apis/config"

	topologyv1alpha1 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha1"
	topoclientset "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/generated/clientset/versioned"
	topologyinformers "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/generated/informers/externalversions"
	listerv1alpha1 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/generated/listers/topology/v1alpha1"
)

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = "NodeResourceTopologyMatch"
)

var _ framework.FilterPlugin = &TopologyMatch{}

type nodeTopologyMap map[string]topologyv1alpha1.NodeResourceTopology

type PolicyHandler func(pod *v1.Pod, zoneMap topologyv1alpha1.ZoneList) *framework.Status

type PolicyHandlerMap map[topologyv1alpha1.TopologyManagerPolicy]PolicyHandler

// TopologyMatch plugin which run simplified version of TopologyManager's admit handler
type TopologyMatch struct {
	policyHandlers PolicyHandlerMap
	lister         listerv1alpha1.NodeResourceTopologyLister
	namespaces     []string
}

type NUMANode struct {
	NUMAID    int
	Resources v1.ResourceList
}

type NUMANodeList []NUMANode

// Name returns name of the plugin. It is used in logs, etc.
func (tm *TopologyMatch) Name() string {
	return Name
}

// getTopologyPolicies return true if we're working with such policy
func getTopologyPolicies(nodeTopology *topologyv1alpha1.NodeResourceTopology, nodeName string) []topologyv1alpha1.TopologyManagerPolicy {
	policies := make([]topologyv1alpha1.TopologyManagerPolicy, 0)
	for _, policy := range nodeTopology.TopologyPolicies {
		policies = append(policies, topologyv1alpha1.TopologyManagerPolicy(policy))
	}
	return policies
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

func SingleNUMAContainerLevelHandler(pod *v1.Pod, zones topologyv1alpha1.ZoneList) *framework.Status {
	klog.V(5).Infof("Single NUMA node handler")

	// prepare NUMANodes list from zoneMap
	nodes := createNUMANodeList(zones)
	qos := v1qos.GetPodQOS(pod)
	for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		resBitmask := checkResourcesForNUMANodes(nodes, container.Resources.Requests, qos)
		if resBitmask.IsEmpty() {
			// definitely we can't align container, so we can't align a pod
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Cannot align container: %s", container.Name))
		}
	}
	return nil
}

func checkResourcesForNUMANodes(nodes NUMANodeList, resources v1.ResourceList, qos v1.PodQOSClass) bm.BitMask {
	bitmask := bm.NewEmptyBitMask()
	bitmask.Fill()

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
	return bitmask
}

func SingleNUMAPodLevelHandler(pod *v1.Pod, zones topologyv1alpha1.ZoneList) *framework.Status {
	klog.V(5).Infof("Pod Level Resource handler")
	resources := make(v1.ResourceList)

	for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		for resource, quantity := range container.Resources.Requests {
			if q, ok := resources[resource]; ok {
				quantity.Add(q)
			}
			resources[resource] = quantity
		}
	}

	nodes := createNUMANodeList(zones)
	resBitmask := checkResourcesForNUMANodes(nodes, resources, v1qos.GetPodQOS(pod))

	if resBitmask.IsEmpty() {
		// definitely we can't align container, so we can't align a pod
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

func getNodeTopology(nodes []*topologyv1alpha1.NodeResourceTopology, nodeName string) *topologyv1alpha1.NodeResourceTopology {
	for _, node := range nodes {
		if node.Name == nodeName {
			return node
		}
	}
	return nil
}

func (tm *TopologyMatch) findNodeTopology(nodeName string) *topologyv1alpha1.NodeResourceTopology {
	klog.V(5).Infof("tm.namespaces: %s", tm.namespaces)
	for _, namespace := range tm.namespaces {
		// NodeTopology couldn't be placed in several namespaces simultaneously
		nodeTopology, err := tm.lister.NodeResourceTopologies(namespace).Get(nodeName)
		if err != nil {
			klog.Errorf("Cannot get NodeTopologies from cache: %v", err)
			return nil
		}
		if nodeTopology != nil {
			return nodeTopology
		}
	}
	return nil
}

// Filter Now only single-numa-node supported
func (tm *TopologyMatch) Filter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	if nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("Node is nil %s", nodeInfo.Node().Name))
	}
	if v1qos.GetPodQOS(pod) == v1.PodQOSBestEffort {
		return nil
	}

	nodeName := nodeInfo.Node().Name
	nodeTopology := tm.findNodeTopology(nodeName)

	if nodeTopology == nil {
		return nil
	}

	klog.V(5).Infof("nodeTopology: %v", nodeTopology)
	topologyPolicies := getTopologyPolicies(nodeTopology, nodeName)
	for _, policyName := range topologyPolicies {
		if handler, ok := tm.policyHandlers[policyName]; ok {
			if status := handler(pod, nodeTopology.Zones); status != nil {
				return status
			}
		} else {
			klog.V(5).Infof("Handler for policy %s not found", policyName)
		}
	}
	return nil
}

// New initializes a new plugin and returns it.
func New(args runtime.Object, handle framework.FrameworkHandle) (framework.Plugin, error) {
	klog.V(5).Infof("creating new TopologyMatch plugin")
	tcfg, ok := args.(*apiconfig.NodeResourceTopologyMatchArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type NodeResourceTopologyMatchArgs, got %T", args)
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
	topologyMatch := &TopologyMatch{
		policyHandlers: PolicyHandlerMap{
			topologyv1alpha1.SingleNUMANodePodLevel:       SingleNUMAPodLevelHandler,
			topologyv1alpha1.SingleNUMANodeContainerLevel: SingleNUMAContainerLevelHandler,
		},
		lister:     nodeTopologyInformer.Lister(),
		namespaces: tcfg.Namespaces,
	}

	klog.V(5).Infof("start nodeTopologyInformer")
	ctx := context.Background()
	topologyInformerFactory.Start(ctx.Done())
	topologyInformerFactory.WaitForCacheSync(ctx.Done())

	return topologyMatch, nil
}
