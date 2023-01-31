package clustermanager

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/retry"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	kapi "k8s.io/api/core/v1"
	cache "k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// SecondaryLayer3NetworkClusterMgr is the object for managing the secondary
// layer3 network for all the nodes. Listens to the node events.
type SecondaryLayer3NetworkClusterMgr struct {
	networkClusterMgrBase
	nodeL3NetworkController *NodeNetworkController

	// per controller NAD/netconf name information
	util.NetInfo
	util.NetConfInfo

	// Node-specific syncMaps used by node event handler
	addNodeFailed sync.Map
}

func newSecondaryLayer3NetworkClusterMgr(ovnClient *util.OVNClientset, wf *factory.WatchFactory, stopChan chan struct{},
	wg *sync.WaitGroup, nInfo util.NetInfo, netConfInfo util.NetConfInfo, networkName string) *SecondaryLayer3NetworkClusterMgr {

	kube := &kube.Kube{
		KClient:              ovnClient.KubeClient,
		EIPClient:            ovnClient.EgressIPClient,
		EgressFirewallClient: ovnClient.EgressFirewallClient,
		CloudNetworkClient:   ovnClient.CloudNetworkClient,
	}

	ncm := &SecondaryLayer3NetworkClusterMgr{
		networkClusterMgrBase: networkClusterMgrBase{
			kube:         kube,
			watchFactory: wf,
			stopChan:     stopChan,
			wg:           wg,
		},
		nodeL3NetworkController: newNodeNetworkController(kube, wf, networkName),
		NetInfo:                 nInfo,
		NetConfInfo:             netConfInfo,
		addNodeFailed:           sync.Map{},
	}

	ncm.initRetryFramework()
	return ncm
}

func (ncm *SecondaryLayer3NetworkClusterMgr) initRetryFramework() {
	ncm.retryNodes = ncm.newRetryFramework(factory.NodeType)
}

// Start initializes the default network subnet allocator ranges
// and hybrid network subnet allocator ranges if hybrod overlay is enabled.
func (ncm *SecondaryLayer3NetworkClusterMgr) Start(ctx context.Context) error {
	klog.Infof("Start secondary %s network cluster manager for network %s", ncm.TopologyType(), ncm.GetNetworkName())
	klog.Infof("Allocating subnets")
	layer3NetConfInfo := ncm.NetConfInfo.(*util.Layer3NetConfInfo)
	if err := ncm.nodeL3NetworkController.InitSubnetAllocatorRanges(layer3NetConfInfo.ClusterSubnets); err != nil {
		klog.Errorf("Failed to initialize host subnet allocator ranges: %v", err)
		return err
	}

	return ncm.Run()
}

// Stop gracefully stops the controller, and delete all logical entities for this network if requested
func (ncm *SecondaryLayer3NetworkClusterMgr) Stop() {
	klog.Infof("Stop secondary %s network cluster manager for network %s", ncm.TopologyType(), ncm.GetNetworkName())
	close(ncm.stopChan)
	ncm.wg.Wait()

	if ncm.nodeHandler != nil {
		ncm.watchFactory.RemoveNodeHandler(ncm.nodeHandler)
	}
}

// Cleanup cleans up logical entities for the given network, called from net-attach-def routine
func (ncm *SecondaryLayer3NetworkClusterMgr) Cleanup(netName string) error {
	return ncm.nodeL3NetworkController.cleanup(netName)
}

// Run starts the actual watching.
func (ncm *SecondaryLayer3NetworkClusterMgr) Run() error {
	if err := ncm.WatchNodes(); err != nil {
		return err
	}

	return nil
}

// WatchNodes starts the watching of node resource and calls
// back the appropriate handler logic
func (ncm *SecondaryLayer3NetworkClusterMgr) WatchNodes() error {
	if ncm.nodeHandler != nil {
		return nil
	}
	handler, err := ncm.retryNodes.WatchResource()
	if err == nil {
		ncm.nodeHandler = handler
	}
	return err
}

func (ncm *SecondaryLayer3NetworkClusterMgr) addUpdateNodeEvent(node *kapi.Node, nSyncs *nodeSyncs) error {
	klog.Infof("Adding or Updating Node %q for network %s", node.Name, ncm.GetNetworkName())
	if nSyncs.syncNode {
		if err := ncm.addNode(node); err != nil {
			ncm.addNodeFailed.Store(node.Name, true)
			err = fmt.Errorf("nodeAdd: error adding node %q for network %s: %w", node.Name, ncm.GetNetworkName(), err)
			return err
		}
		ncm.addNodeFailed.Delete(node.Name)
	}

	return nil
}

func (ncm *SecondaryLayer3NetworkClusterMgr) addNode(node *kapi.Node) error {
	return ncm.nodeL3NetworkController.addUpdateNode(node)
}

func (ncm *SecondaryLayer3NetworkClusterMgr) deleteNodeEvent(node *kapi.Node) error {
	klog.V(5).Infof("Deleting Node %q for network %s. Removing the node from "+
		"various caches", node.Name, ncm.GetNetworkName())

	return ncm.nodeL3NetworkController.deleteNode(node)
}

func (ncm *SecondaryLayer3NetworkClusterMgr) syncNodes(nodes []interface{}) error {
	return ncm.nodeL3NetworkController.syncNodes(nodes)
}

type secondaryLayer3NetClusterMgrEventHandler struct {
	watchFactory *factory.WatchFactory
	objType      reflect.Type
	ncm          *SecondaryLayer3NetworkClusterMgr
	syncFunc     func([]interface{}) error
}

// newRetryFramework builds and returns a retry framework for the input resource type;
func (ncm *SecondaryLayer3NetworkClusterMgr) newRetryFramework(
	objectType reflect.Type) *retry.RetryFramework {
	eventHandler := &secondaryLayer3NetClusterMgrEventHandler{
		objType:      objectType,
		watchFactory: ncm.watchFactory,
		ncm:          ncm,
		syncFunc:     nil,
	}
	resourceHandler := &retry.ResourceHandler{
		HasUpdateFunc:          true,
		NeedsUpdateDuringRetry: false,
		ObjType:                objectType,
		EventHandler:           eventHandler,
	}
	return retry.NewRetryFramework(
		ncm.stopChan,
		ncm.wg,
		ncm.watchFactory,
		resourceHandler,
	)
}

func (h *secondaryLayer3NetClusterMgrEventHandler) AreResourcesEqual(obj1, obj2 interface{}) (bool, error) {
	node1, ok := obj1.(*kapi.Node)
	if !ok {
		return false, fmt.Errorf("could not cast obj1 of type %T to *kapi.Node", obj1)
	}
	node2, ok := obj2.(*kapi.Node)
	if !ok {
		return false, fmt.Errorf("could not cast obj2 of type %T to *kapi.Node", obj2)
	}

	// when shouldUpdateNode is false, the hostsubnet is not assigned by ovn-kubernetes
	shouldUpdate, err := util.ShouldUpdateNode(node2, node1)
	if err != nil {
		klog.Errorf(err.Error())
	}
	return !shouldUpdate, nil
}

// GetInternalCacheEntry returns the internal cache entry for this object, given an object and its type.
// This is now used only for pods, which will get their the logical port cache entry.
func (h *secondaryLayer3NetClusterMgrEventHandler) GetInternalCacheEntry(obj interface{}) interface{} {
	return nil
}

// GetResourceFromInformerCache returns the latest state of the object, given an object key and its type.
// from the informers cache.
func (h *secondaryLayer3NetClusterMgrEventHandler) GetResourceFromInformerCache(key string) (interface{}, error) {
	var obj interface{}
	var err error

	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to split key %s: %v", key, err)
	}

	if h.objType == factory.NodeType {
		obj, err = h.watchFactory.GetNode(name)
	}
	return obj, err
}

// RecordAddEvent records the add event on this given object.
func (h *secondaryLayer3NetClusterMgrEventHandler) RecordAddEvent(obj interface{}) {
}

// RecordUpdateEvent records the udpate event on this given object.
func (h *secondaryLayer3NetClusterMgrEventHandler) RecordUpdateEvent(obj interface{}) {
}

// RecordDeleteEvent records the delete event on this given object.
func (h *secondaryLayer3NetClusterMgrEventHandler) RecordDeleteEvent(obj interface{}) {
}

// RecordSuccessEvent records the success event on this given object.
func (h *secondaryLayer3NetClusterMgrEventHandler) RecordSuccessEvent(obj interface{}) {
}

// RecordErrorEvent records the error event on this given object.
func (h *secondaryLayer3NetClusterMgrEventHandler) RecordErrorEvent(obj interface{}, reason string, err error) {
}

func (h *secondaryLayer3NetClusterMgrEventHandler) IsResourceScheduled(obj interface{}) bool {
	return true
}

// IsObjectInTerminalState returns true if the object is in a terminal state.
func (h *secondaryLayer3NetClusterMgrEventHandler) IsObjectInTerminalState(bj interface{}) bool {
	return false
}

type nodeSyncs struct {
	syncNode bool
}

// AddResource adds the specified object to the cluster according to its type and returns the error,
// if any, yielded during object creation.
// Given an object to add and a boolean specifying if the function was executed from iterateRetryResources
func (h *secondaryLayer3NetClusterMgrEventHandler) AddResource(obj interface{}, fromRetryLoop bool) error {
	switch h.objType {
	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast %T object to *kapi.Node", obj)
		}
		var nodeParams *nodeSyncs
		if fromRetryLoop {
			_, nodeSync := h.ncm.addNodeFailed.Load(node.Name)
			nodeParams = &nodeSyncs{syncNode: nodeSync}
		} else {
			nodeParams = &nodeSyncs{syncNode: true}
		}

		if err := h.ncm.addUpdateNodeEvent(node, nodeParams); err != nil {
			klog.Errorf("Node add failed for %s, will try again later: %v",
				node.Name, err)
			return err
		}
	}
	return nil
}

// UpdateResource updates the specified object in the cluster to its version in newObj according to its
// type and returns the error, if any, yielded during the object update.
// Given an old and a new object; The inRetryCache boolean argument is to indicate if the given resource
// is in the retryCache or not.
func (h *secondaryLayer3NetClusterMgrEventHandler) UpdateResource(oldObj, newObj interface{}, inRetryCache bool) error {
	switch h.objType {
	case factory.NodeType:
		newNode, ok := newObj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast newObj of type %T to *kapi.Node", newObj)
		}
		_, ok = oldObj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast oldObj of type %T to *kapi.Node", oldObj)
		}
		// determine what actually changed in this update
		_, nodeSync := h.ncm.addNodeFailed.Load(newNode.Name)

		return h.ncm.addUpdateNodeEvent(newNode, &nodeSyncs{syncNode: nodeSync})
	}
	return nil
}

// DeleteResource deletes the object from the cluster according to the delete logic of its resource type.
// Given an object and optionally a cachedObj; cachedObj is the internal cache entry for this object,
// used for now for pods and network policies.
func (h *secondaryLayer3NetClusterMgrEventHandler) DeleteResource(obj, cachedObj interface{}) error {
	switch h.objType {
	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast obj of type %T to *knet.Node", obj)
		}
		return h.ncm.deleteNodeEvent(node)

	}
	return nil
}

func (h *secondaryLayer3NetClusterMgrEventHandler) SyncFunc(objs []interface{}) error {
	switch h.objType {
	case factory.NodeType:
		return h.ncm.syncNodes(objs)

	default:
		return fmt.Errorf("no sync function for object type %s", h.objType)
	}
}
