package clustermanager

import (
	"context"
	"fmt"
	"math/big"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	kapi "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	utilwait "k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	utilnet "k8s.io/utils/net"

	"github.com/ovn-org/ovn-kubernetes/go-controller/hybrid-overlay/pkg/types"
	houtil "github.com/ovn-org/ovn-kubernetes/go-controller/hybrid-overlay/pkg/util"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	bitmapallocator "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/ipallocator/allocator"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/subnetallocator"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

const (
	OvnNodeAnnotationRetryInterval = 100 * time.Millisecond
	OvnNodeAnnotationRetryTimeout  = 1 * time.Second

	transitSwitchv4Cidr = "169.254.0.0/16"
	transitSwitchv6Cidr = "fd97::/64"

	// Maximum node Ids that can be generated. Limited to maximum nodes supported by k8s.
	maxNodeIds = 5000
)

type ClusterManager struct {
	client       clientset.Interface
	kube         kube.Interface
	watchFactory *factory.WatchFactory
	stopChan     <-chan struct{}

	// FIXME DUAL-STACK -  Make IP Allocators more dual-stack friendly
	clusterSubnetAllocator       *subnetallocator.SubnetAllocator
	hybridOverlaySubnetAllocator *subnetallocator.SubnetAllocator

	zoneJoinNetworkAllocator *subnetallocator.SubnetAllocator

	// event recorder used to post events to k8s
	recorder record.EventRecorder

	// v4HostSubnetsUsed keeps track of number of v4 subnets currently assigned to nodes
	v4HostSubnetsUsed float64

	// v6HostSubnetsUsed keeps track of number of v6 subnets currently assigned to nodes
	v6HostSubnetsUsed float64

	nodeIdBitmap    *bitmapallocator.AllocationBitmap
	nodeIdCache     map[string]int
	nodeIdCacheLock sync.Mutex

	zoneJoinSubnetCache map[string]([]*net.IPNet)
	zoneIdCacheLock     sync.Mutex

	transitSwitchv4Cidr   *net.IPNet
	transitSwitchBasev4Ip *big.Int

	transitSwitchv6Cidr   *net.IPNet
	transitSwitchBasev6Ip *big.Int
}

// NewOvnController creates a new OVN controller for creating logical network
// infrastructure and policy
func NewClusterManager(ovnClient *util.OVNClientset, wf *factory.WatchFactory, stopChan <-chan struct{},
	recorder record.EventRecorder) *ClusterManager {
	kube := &kube.Kube{
		KClient:              ovnClient.KubeClient,
		EIPClient:            ovnClient.EgressIPClient,
		EgressFirewallClient: ovnClient.EgressFirewallClient,
		CloudNetworkClient:   ovnClient.CloudNetworkClient,
	}

	var hybridOverlaySubnetAllocator *subnetallocator.SubnetAllocator
	if config.HybridOverlay.Enabled {
		hybridOverlaySubnetAllocator = subnetallocator.NewSubnetAllocator()
	}

	nodeIdBitmap := bitmapallocator.NewContiguousAllocationMap(maxNodeIds, "nodeIds")
	_, _ = nodeIdBitmap.Allocate(0)

	_, tsv4Cidr, _ := net.ParseCIDR(transitSwitchv4Cidr)
	_, tsv6Cidr, _ := net.ParseCIDR(transitSwitchv6Cidr)
	return &ClusterManager{
		client:                       ovnClient.KubeClient,
		kube:                         kube,
		watchFactory:                 wf,
		stopChan:                     stopChan,
		clusterSubnetAllocator:       subnetallocator.NewSubnetAllocator(),
		hybridOverlaySubnetAllocator: hybridOverlaySubnetAllocator,
		zoneJoinNetworkAllocator:     subnetallocator.NewSubnetAllocator(),
		recorder:                     recorder,
		nodeIdBitmap:                 nodeIdBitmap,
		nodeIdCache:                  make(map[string]int),
		zoneJoinSubnetCache:          make(map[string]([]*net.IPNet)),
		transitSwitchBasev4Ip:        utilnet.BigForIP(tsv4Cidr.IP),
		transitSwitchv4Cidr:          tsv4Cidr,
		transitSwitchBasev6Ip:        utilnet.BigForIP(tsv6Cidr.IP),
		transitSwitchv6Cidr:          tsv4Cidr,
	}
}

type ovnkubeClusterManagerLeaderMetrics struct{}

func (ovnkubeClusterManagerLeaderMetrics) On(string) {
	metrics.MetricClusterManagerLeader.Set(1)
}

func (ovnkubeClusterManagerLeaderMetrics) Off(string) {
	metrics.MetricClusterManagerLeader.Set(0)
}

type ovnkubeClusterManagerLeaderMetricsProvider struct{}

func (_ ovnkubeClusterManagerLeaderMetricsProvider) NewLeaderMetric() leaderelection.SwitchMetric {
	return ovnkubeClusterManagerLeaderMetrics{}
}

// Start waits until this process is the leader before starting master functions
func (cm *ClusterManager) Start(nodeName string, wg *sync.WaitGroup, ctx context.Context) error {
	klog.Infof("Cluster manager Started.")
	// Set up leader election process first.
	// User lease resource lock as configmap and endpoint lock support is removed from leaderelection library.
	rl, err := resourcelock.New(
		resourcelock.LeasesResourceLock,
		config.Kubernetes.OVNConfigNamespace,
		"ovn-kubernetes-cluster-manager",
		cm.client.CoreV1(),
		cm.client.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity:      nodeName,
			EventRecorder: cm.recorder,
		},
	)
	if err != nil {
		return err
	}

	lec := leaderelection.LeaderElectionConfig{
		Lock:            rl,
		LeaseDuration:   time.Duration(config.MasterHA.ElectionLeaseDuration) * time.Second,
		RenewDeadline:   time.Duration(config.MasterHA.ElectionRenewDeadline) * time.Second,
		RetryPeriod:     time.Duration(config.MasterHA.ElectionRetryPeriod) * time.Second,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				klog.Infof("Won leader election; in active mode")
				// run the cluster controller to init the cluster manager
				start := time.Now()
				defer func() {
					end := time.Since(start)
					metrics.MetriClusterManagerReadyDuration.Set(end.Seconds())
				}()

				if err := cm.StartClusterManager(); err != nil {
					panic(err.Error())
				}
				// run the cluster controller to init the master
				// run only on the active master node.
				if err := cm.Run(nodeName); err != nil {
					panic(err.Error())
				}
			},
			OnStoppedLeading: func() {
				//This node was leader and it lost the election.
				// Whenever the node transitions from leader to follower,
				// we need to handle the transition properly like clearing
				// the cache. It is better to exit for now.
				// kube will restart and this will become a follower.
				klog.Infof("No longer leader; exiting")
				os.Exit(0)
			},
			OnNewLeader: func(newLeaderName string) {
				if newLeaderName != nodeName {
					klog.Infof("Lost the election to %s; in standby mode", newLeaderName)
				}
			},
		},
	}

	leaderelection.SetProvider(ovnkubeClusterManagerLeaderMetricsProvider{})
	leaderElector, err := leaderelection.NewLeaderElector(lec)
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		leaderElector.Run(ctx)
		klog.Infof("Stopped leader election")
		wg.Done()
	}()

	return nil
}

// StartClusterManager runs a subnet IPAM that watches arrival/departure
// of nodes in the cluster
// On an addition to the cluster (node create), a new subnet is created for it.
// ovnkube-master will create the node logical switch and other resources in the
// OVN Northbound database.
// Upon deletion of a node, the node subnet is released.
//
// TODO: Verify that the cluster was not already called with a different global subnet
//  If true, then either quit or perform a complete reconfiguration of the cluster (recreate switches/routers with new subnet values)
func (cm *ClusterManager) StartClusterManager() error {
	klog.Infof("Starting cluster manager")
	metrics.RegisterClusterManagerFunctional()

	existingNodes, err := cm.kube.GetNodes()
	if err != nil {
		klog.Errorf("Error in fetching nodes: %v", err)
		return err
	}
	klog.V(5).Infof("Existing number of nodes: %d", len(existingNodes.Items))

	klog.Infof("Allocating subnets")
	var v4HostSubnetCount, v6HostSubnetCount float64
	for _, clusterEntry := range config.Default.ClusterSubnets {
		err := cm.AddClusterSubnetNetworkRange(clusterEntry.CIDR, clusterEntry.HostSubnetLength)
		if err != nil {
			return err
		}
		klog.V(5).Infof("Added network range %s to the allocator", clusterEntry.CIDR)
		util.CalculateHostSubnetsForClusterEntry(clusterEntry, &v4HostSubnetCount, &v6HostSubnetCount)
	}

	if config.HybridOverlay.Enabled {
		for _, clusterEntry := range config.HybridOverlay.ClusterSubnets {
			err := cm.AddHybridOverlaySubnetNetworkRange(clusterEntry.CIDR, clusterEntry.HostSubnetLength)
			if err != nil {
				return err
			}
			klog.V(5).Infof("Added network range %s to the hybrid overlay allocator", clusterEntry.CIDR)
		}
	}

	for _, zoneSubnetEntry := range config.ClusterManager.ZoneJoinSubnets {
		err := cm.AddJoinSubnetNetworkRange(zoneSubnetEntry.CIDR, zoneSubnetEntry.HostSubnetLength)
		if err != nil {
			return err
		}
		klog.V(5).Infof("Added network range %s to the zone join switch subnet allocator", zoneSubnetEntry.CIDR)
	}
	// update metrics for host subnets
	metrics.RecordSubnetCount(v4HostSubnetCount, v6HostSubnetCount)

	return nil
}

// Run starts the actual watching.
func (cm *ClusterManager) Run(nodeName string) error {
	// Start and sync the watch factory to begin listening for events
	if err := cm.watchFactory.Start(); err != nil {
		return err
	}

	if err := cm.WatchNodes(); err != nil {
		return err
	}

	return nil
}

func (cm *ClusterManager) AddClusterSubnetNetworkRange(network *net.IPNet, hostSubnetLen int) error {
	return cm.clusterSubnetAllocator.AddNetworkRange(network, hostSubnetLen)
}

func (cm *ClusterManager) AddHybridOverlaySubnetNetworkRange(network *net.IPNet, hostSubnetLen int) error {
	return cm.hybridOverlaySubnetAllocator.AddNetworkRange(network, hostSubnetLen)
}

func (cm *ClusterManager) AddJoinSubnetNetworkRange(network *net.IPNet, hostSubnetLen int) error {
	return cm.zoneJoinNetworkAllocator.AddNetworkRange(network, hostSubnetLen)
}

// WatchNodes starts the watching of node resource and calls
// back the appropriate handler logic
func (cm *ClusterManager) WatchNodes() error {
	_, err := cm.watchFactory.AddNodeHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node := obj.(*kapi.Node)
			klog.V(5).Infof("Added event for Node %q", node.Name)
			if err := cm.addUpdateNodeEvent(node); err != nil {
				klog.Errorf("Failed to handle Add node event for node %s, error: %v",
					node.Name, err)
			}
		},
		UpdateFunc: func(old, new interface{}) {
			node := new.(*kapi.Node)
			klog.V(5).Infof("Update event for Node %q", node.Name)
			if err := cm.addUpdateNodeEvent(node); err != nil {
				klog.Errorf("Failed to handle Update node event for node %s, error: %v",
					node.Name, err)
			}
		},
		DeleteFunc: func(obj interface{}) {
			node := obj.(*kapi.Node)
			klog.V(5).Infof("Deleted event for Node %q", node.Name)
			if err := cm.deleteNode(node); err != nil {
				klog.Errorf("Failed to handle Delete node event for node %s, error: %v",
					node.Name, err)
			}
		},
	}, cm.syncNodes)

	return err
}

func (cm *ClusterManager) syncNodes(nodes []interface{}) error {
	for _, tmp := range nodes {
		node, ok := tmp.(*kapi.Node)
		if !ok {
			return fmt.Errorf("spurious object in syncNodes: %v", tmp)
		}

		hostSubnets, _ := util.ParseNodeHostSubnetAnnotation(node)
		if config.HybridOverlay.Enabled && len(hostSubnets) == 0 && houtil.IsHybridOverlayNode(node) {
			// this is a hybrid overlay node so mark as allocated from the hybrid overlay subnet allocator
			hostSubnet, err := houtil.ParseHybridOverlayHostSubnet(node)
			if err != nil {
				klog.Warning(err.Error())
			} else if hostSubnet != nil {
				klog.V(5).Infof("Node %s contains subnets: %v", node.Name, hostSubnet)
				if err := cm.hybridOverlaySubnetAllocator.MarkAllocatedNetwork(hostSubnet); err != nil {
					utilruntime.HandleError(err)
				}
			}
			// there is nothing left to be done if this is a hybrid overlay node
			continue
		}

		klog.V(5).Infof("Node %s contains subnets: %v", node.Name, hostSubnets)
		for _, hostSubnet := range hostSubnets {
			err := cm.clusterSubnetAllocator.MarkAllocatedNetwork(hostSubnet)
			if err != nil {
				utilruntime.HandleError(err)
			}
			util.UpdateUsedHostSubnetsCount(hostSubnet, &cm.v4HostSubnetsUsed, &cm.v6HostSubnetsUsed, true)
		}
		nodeZone := util.GetNodeZone(node)
		zoneSubnets, err := util.ParseZoneJoinSubnetsAnnotation(node)
		if err != nil {
			continue
		}
		_, found := cm.zoneJoinSubnetCache[nodeZone]
		if !found {
			cm.zoneJoinSubnetCache[nodeZone] = zoneSubnets
			for _, subnet := range zoneSubnets {
				if !cm.zoneJoinNetworkAllocator.IsNetworkAllocated(subnet) {
					_ = cm.zoneJoinNetworkAllocator.MarkAllocatedNetwork(subnet)
				}
			}
		}
	}
	return nil
}

func (cm *ClusterManager) addUpdateNodeEvent(node *kapi.Node) error {
	if noHostSubnet := util.NoHostSubnet(node); noHostSubnet {
		if config.HybridOverlay.Enabled && houtil.IsHybridOverlayNode(node) {
			annotator := kube.NewNodeAnnotator(cm.kube, node.Name)
			allocatedSubnet, err := cm.hybridOverlayNodeEnsureSubnet(node, annotator)
			if err != nil {
				return fmt.Errorf("failed to update node %s hybrid overlay subnet annotation: %v", node.Name, err)
			}
			if err := annotator.Run(); err != nil {
				// Release allocated subnet if any errors occurred
				if allocatedSubnet != nil {
					_ = cm.releaseHybridOverlayNodeSubnet(node.Name, allocatedSubnet)
				}
				return fmt.Errorf("failed to set hybrid overlay annotations for node %s: %v", node.Name, err)
			}
		}
		return nil
	}

	return cm.addNode(node)
}

func (cm *ClusterManager) addNode(node *kapi.Node) error {
	var allocatedNodeId int = -1
	hostSubnets, allocatedSubnets, err := cm.allocateNodeSubnets(node)
	if err != nil {
		return err
	}

	// Release the allocation on error
	defer func() {
		if err != nil {
			for _, allocatedSubnet := range allocatedSubnets {
				klog.Warningf("Releasing subnet %v on node %s: %v", allocatedSubnet, node.Name, err)
				errR := cm.clusterSubnetAllocator.ReleaseNetwork(allocatedSubnet)
				if errR != nil {
					klog.Warningf("Error releasing subnet %v on node %s", allocatedSubnet, node.Name)
				}
			}
			cm.removeNodeId(node.Name, allocatedNodeId)
		}
	}()

	allocatedNodeId, nodeIdNeedsUpdate, err := cm.allocateNodeId(node)
	if err != nil {
		return err
	}

	nodeAnnotations := map[string]interface{}{}
	if len(allocatedSubnets) > 0 {
		// Set the HostSubnet annotation on the node object to signal
		// to nodes that their logical infrastructure is set up and they can
		// proceed with their initialization
		subnetAnnotations, err := util.CreateNodeHostSubnetAnnotation(hostSubnets)
		if err != nil {
			return fmt.Errorf("failed to marshal node %q annotation for subnet %s",
				node.Name, util.JoinIPNets(hostSubnets, ","))
		}

		for k, v := range subnetAnnotations {
			nodeAnnotations[k] = v
		}
	}

	if nodeIdNeedsUpdate {
		// Add the node id annotation.
		nodeAnnotations[util.OvnNodeId] = strconv.Itoa(allocatedNodeId)
	}

	zoneJoinSubnets, err := cm.allocateZoneJoinSubnets(node)
	if err != nil {
		return err
	}

	// Generate v4 and v6 transit switch port IPs for the node.
	nodeTransitSwitchPortIps := cm.syncNodeTransitSwitchPortIps(node, allocatedNodeId)
	if nodeTransitSwitchPortIps != nil {
		transitSwitchPortAnnotations, err := util.CreateNodeTransitSwitchPortAddressesAnnotation(nodeTransitSwitchPortIps)
		if err != nil {
			return fmt.Errorf("failed to marshal node transit switch ips for node %s : error - %v",
				node.Name, err)
		}

		for k, v := range transitSwitchPortAnnotations {
			nodeAnnotations[k] = v
		}
	}

	joinSubnetAnnotations, err := util.CreateZoneJoinSubnetsAnnotation(zoneJoinSubnets)
	zoneJoinSubnet := joinSubnetAnnotations[util.OvnZoneJoinSubnets]
	if zoneJoinSubnet != node.Annotations[util.OvnZoneJoinSubnets] {
		nodeAnnotations[util.OvnZoneJoinSubnets] = zoneJoinSubnet
	}

	if len(nodeAnnotations) > 0 {
		// FIXME: the real solution is to reconcile the node object. Once we have a work-queue based
		// implementation where we can add the item back to the work queue when it fails to
		// reconcile, we can get rid of the PollImmediate.
		err = utilwait.PollImmediate(OvnNodeAnnotationRetryInterval, OvnNodeAnnotationRetryTimeout, func() (bool, error) {
			err = cm.kube.SetAnnotationsOnNode(node.Name, nodeAnnotations)
			if err != nil {
				klog.Warningf("Failed to set node annotation, will retry for: %v",
					OvnNodeAnnotationRetryTimeout)
			}
			return err == nil, nil
		},
		)
		if err != nil {
			return fmt.Errorf("failed to set node-subnets annotation on node %s: %v",
				node.Name, err)
		}
	}

	if len(allocatedSubnets) > 1 {
		// If node annotation succeeds and subnets were allocated, update the used subnet count
		for _, hostSubnet := range hostSubnets {
			util.UpdateUsedHostSubnetsCount(hostSubnet,
				&cm.v4HostSubnetsUsed,
				&cm.v6HostSubnetsUsed, true)
		}
		metrics.RecordSubnetUsage(cm.v4HostSubnetsUsed, cm.v6HostSubnetsUsed)
	}

	return err
}

func (cm *ClusterManager) deleteNode(node *kapi.Node) error {
	if config.HybridOverlay.Enabled {
		if subnet, _ := houtil.ParseHybridOverlayHostSubnet(node); subnet != nil {
			if err := cm.releaseHybridOverlayNodeSubnet(node.Name, subnet); err != nil {
				return err
			}
		}
	}

	nodeSubnets, _ := util.ParseNodeHostSubnetAnnotation(node)
	for _, nodeSubnet := range nodeSubnets {
		err := cm.clusterSubnetAllocator.ReleaseNetwork(nodeSubnet)
		if err != nil {
			return fmt.Errorf("error deleting subnet %v for node %q: %s", nodeSubnet, node.Name, err)
		}
		klog.Infof("Deleted nodeSubnet %v for node %s", nodeSubnet, node.Name)

		util.UpdateUsedHostSubnetsCount(nodeSubnet, &cm.v4HostSubnetsUsed, &cm.v6HostSubnetsUsed, false)
	}
	// update metrics
	metrics.RecordSubnetUsage(cm.v4HostSubnetsUsed, cm.v6HostSubnetsUsed)

	nodeId := util.GetNodeId(node)
	cm.removeNodeId(node.Name, nodeId)
	return nil
}

func (cm *ClusterManager) allocateNodeSubnets(node *kapi.Node) ([]*net.IPNet, []*net.IPNet, error) {
	hostSubnets, err := util.ParseNodeHostSubnetAnnotation(node)
	if err != nil {
		// Log the error and try to allocate new subnets
		klog.Infof("Failed to get node %s host subnets annotations: %v", node.Name, err)
	}
	allocatedSubnets := []*net.IPNet{}

	// OVN can work in single-stack or dual-stack only.
	currentHostSubnets := len(hostSubnets)
	expectedHostSubnets := 1
	// if dual-stack mode we expect one subnet per each IP family
	if config.IPv4Mode && config.IPv6Mode {
		expectedHostSubnets = 2
	}

	// node already has the expected subnets annotated
	// assume IP families match, i.e. no IPv6 config and node annotation IPv4
	if expectedHostSubnets == currentHostSubnets {
		klog.Infof("Allocated Subnets %v on Node %s", hostSubnets, node.Name)
		return hostSubnets, allocatedSubnets, nil
	}

	// Node doesn't have the expected subnets annotated
	// it may happen it has more subnets assigned that configured in OVN
	// like in a dual-stack to single-stack conversion
	// or that it needs to allocate new subnet because it is a new node
	// or has been converted from single-stack to dual-stack
	klog.Infof("Expected %d subnets on node %s, found %d: %v",
		expectedHostSubnets, node.Name, currentHostSubnets, hostSubnets)
	// release unexpected subnets
	// filter in place slice
	// https://github.com/golang/go/wiki/SliceTricks#filter-in-place
	foundIPv4 := false
	foundIPv6 := false
	n := 0
	for _, subnet := range hostSubnets {
		// if the subnet is not going to be reused release it
		if config.IPv4Mode && utilnet.IsIPv4CIDR(subnet) && !foundIPv4 {
			klog.V(5).Infof("Valid IPv4 allocated subnet %v on node %s", subnet, node.Name)
			hostSubnets[n] = subnet
			n++
			foundIPv4 = true
			continue
		}
		if config.IPv6Mode && utilnet.IsIPv6CIDR(subnet) && !foundIPv6 {
			klog.V(5).Infof("Valid IPv6 allocated subnet %v on node %s", subnet, node.Name)
			hostSubnets[n] = subnet
			n++
			foundIPv6 = true
			continue
		}
		// this subnet is no longer needed
		klog.V(5).Infof("Releasing subnet %v on node %s", subnet, node.Name)
		err = cm.clusterSubnetAllocator.ReleaseNetwork(subnet)
		if err != nil {
			klog.Warningf("Error releasing subnet %v on node %s", subnet, node.Name)
		}
	}
	// recreate hostSubnets with the valid subnets
	hostSubnets = hostSubnets[:n]
	// allocate new subnets if needed
	if config.IPv4Mode && !foundIPv4 {
		allocatedHostSubnet, err := cm.clusterSubnetAllocator.AllocateIPv4Network()
		if err != nil {
			return nil, nil, fmt.Errorf("error allocating network for node %s: %v", node.Name, err)
		}
		// the allocator returns nil if it can't provide a subnet
		// we should filter them out or they will be appended to the slice
		if allocatedHostSubnet != nil {
			klog.V(5).Infof("Allocating subnet %v on node %s", allocatedHostSubnet, node.Name)
			allocatedSubnets = append(allocatedSubnets, allocatedHostSubnet)
			// Release the allocation on error
			defer func() {
				if err != nil {
					klog.Warningf("Releasing subnet %v on node %s: %v", allocatedHostSubnet, node.Name, err)
					errR := cm.clusterSubnetAllocator.ReleaseNetwork(allocatedHostSubnet)
					if errR != nil {
						klog.Warningf("Error releasing subnet %v on node %s", allocatedHostSubnet, node.Name)
					}
				}
			}()
		}
	}
	if config.IPv6Mode && !foundIPv6 {
		allocatedHostSubnet, err := cm.clusterSubnetAllocator.AllocateIPv6Network()
		if err != nil {
			return nil, nil, fmt.Errorf("error allocating network for node %s: %v", node.Name, err)
		}
		// the allocator returns nil if it can't provide a subnet
		// we should filter them out or they will be appended to the slice
		if allocatedHostSubnet != nil {
			klog.V(5).Infof("Allocating subnet %v on node %s", allocatedHostSubnet, node.Name)
			allocatedSubnets = append(allocatedSubnets, allocatedHostSubnet)
		}
	}
	// check if we were able to allocate the new subnets require
	// this can only happen if OVN is not configured correctly
	// so it will require a reconfiguration and restart.
	wantedSubnets := expectedHostSubnets - currentHostSubnets
	if wantedSubnets > 0 && len(allocatedSubnets) != wantedSubnets {
		return nil, nil, fmt.Errorf("error allocating networks for node %s: %d subnets expected only new %d subnets allocated", node.Name, expectedHostSubnets, len(allocatedSubnets))
	}
	hostSubnets = append(hostSubnets, allocatedSubnets...)
	klog.Infof("Allocated Subnets %v on Node %s", hostSubnets, node.Name)
	return hostSubnets, allocatedSubnets, nil
}

// hybridOverlayNodeEnsureSubnet allocates a subnet and sets the
// hybrid overlay subnet annotation. It returns any newly allocated subnet
// or an error. If an error occurs, the newly allocated subnet will be released.
func (cm *ClusterManager) hybridOverlayNodeEnsureSubnet(node *kapi.Node, annotator kube.Annotator) (*net.IPNet, error) {
	// Do not allocate a subnet if the node already has one
	if subnet, _ := houtil.ParseHybridOverlayHostSubnet(node); subnet != nil {
		return nil, nil
	}

	// Allocate a new host subnet for this node
	hostsubnets, err := cm.hybridOverlaySubnetAllocator.AllocateNetworks()
	if err != nil || len(hostsubnets) < 1 {
		return nil, fmt.Errorf("error allocating hybrid overlay HostSubnet for node %s: %v", node.Name, err)
	}

	if err := annotator.Set(types.HybridOverlayNodeSubnet, hostsubnets[0].String()); err != nil {
		_ = cm.hybridOverlaySubnetAllocator.ReleaseNetwork(hostsubnets[0])
		return nil, err
	}

	klog.Infof("Allocated hybrid overlay HostSubnet %s for node %s", hostsubnets[0], node.Name)
	return hostsubnets[0], nil
}

func (cm *ClusterManager) releaseHybridOverlayNodeSubnet(nodeName string, subnet *net.IPNet) error {
	if len(config.HybridOverlay.ClusterSubnets) == 0 {
		// skip releasing node subnet if hybrid-overlay-cluster-subnets is unset.
		return nil
	}

	if err := cm.hybridOverlaySubnetAllocator.ReleaseNetwork(subnet); err != nil {
		return fmt.Errorf("error deleting hybrid overlay HostSubnet %s for node %q: %s", subnet, nodeName, err)
	}
	klog.Infof("Deleted hybrid overlay HostSubnet %s for node %s", subnet, nodeName)
	return nil
}

func (cm *ClusterManager) allocateZoneJoinSubnets(node *kapi.Node) ([]*net.IPNet, error) {
	cm.zoneIdCacheLock.Lock()
	defer func() {
		cm.zoneIdCacheLock.Unlock()
	}()

	nodeZone := util.GetNodeZone(node)
	allocatedZoneSubnets, found := cm.zoneJoinSubnetCache[nodeZone]
	if found {
		return allocatedZoneSubnets, nil
	}

	allocatedSubnets := []*net.IPNet{}
	if config.IPv4Mode {
		allocatedSubnet, err := cm.zoneJoinNetworkAllocator.AllocateIPv4Network()
		if err != nil {
			return nil, fmt.Errorf("error allocating join IPv4 network for zone %s: %v", nodeZone, err)
		}

		allocatedSubnets = append(allocatedSubnets, allocatedSubnet)
	}

	if config.IPv6Mode {
		allocatedSubnet, err := cm.zoneJoinNetworkAllocator.AllocateIPv6Network()
		if err != nil {
			return nil, fmt.Errorf("error allocating join IPv6 network for zone %s: %v", nodeZone, err)
		}

		allocatedSubnets = append(allocatedSubnets, allocatedSubnet)
	}

	cm.zoneJoinSubnetCache[nodeZone] = allocatedSubnets
	return allocatedSubnets, nil
}

func (cm *ClusterManager) removeNodeId(nodeName string, nodeId int) {
	klog.Infof("Deleting node %q ID %d", nodeName, nodeId)
	if nodeId != -1 {
		cm.nodeIdBitmap.Release(nodeId)
	}
	delete(cm.nodeIdCache, nodeName)
}

func (cm *ClusterManager) allocateNodeId(node *kapi.Node) (int, bool, error) {
	cm.nodeIdCacheLock.Lock()
	defer func() {
		cm.nodeIdCacheLock.Unlock()
	}()

	var nodeId int
	nodeId = util.GetNodeId(node)

	nodeIdInCache, ok := cm.nodeIdCache[node.Name]
	if !ok {
		nodeIdInCache = -1
	}

	if nodeIdInCache != -1 && nodeId != nodeIdInCache {
		return nodeIdInCache, true, nil
	}

	if nodeIdInCache == -1 && nodeId != -1 {
		cm.nodeIdCache[node.Name] = nodeId
		return nodeId, false, nil
	}

	// We need to allocate the node id.
	if nodeIdInCache == -1 && nodeId == -1 {
		var allocated bool
		nodeId, allocated, _ = cm.nodeIdBitmap.AllocateNext()
		if allocated {
			cm.nodeIdCache[node.Name] = nodeId
		} else {
			return -1, false, fmt.Errorf("failed to allocate id for the node %q", node.Name)
		}

		return nodeId, true, nil
	}

	return nodeId, false, nil
}

func (cm *ClusterManager) syncRequiredTransitSwitchPortIps(nodeTransitSwitchPortIps []*net.IPNet, allocatedTransitSwitchPortIps []*net.IPNet) bool {
	if nodeTransitSwitchPortIps == nil || allocatedTransitSwitchPortIps == nil {
		return true
	}

	if len(nodeTransitSwitchPortIps) != len(allocatedTransitSwitchPortIps) {
		return true
	}

	nodeTransitPortv4Ips := 0
	nodeTransitPortv6Ips := 0
	allocatedPortv4Ips := 0
	allocatedPortv6Ips := 0

	for _, ip := range nodeTransitSwitchPortIps {
		if utilnet.IsIPv4(ip.IP) {
			nodeTransitPortv4Ips++
		} else {
			nodeTransitPortv6Ips++
		}
	}

	for _, ip := range allocatedTransitSwitchPortIps {
		if utilnet.IsIPv4(ip.IP) {
			allocatedPortv4Ips++
		} else {
			allocatedPortv6Ips++
		}
	}

	if nodeTransitPortv4Ips != allocatedPortv4Ips || nodeTransitPortv6Ips != allocatedPortv6Ips {
		return true
	}

	for _, nodeIp := range nodeTransitSwitchPortIps {
		allocatedIpFound := false
		for _, allocatedIp := range allocatedTransitSwitchPortIps {
			if nodeIp.String() == allocatedIp.String() {
				allocatedIpFound = true
			}
		}

		if !allocatedIpFound {
			return true
		}
	}

	return false
}

func (cm *ClusterManager) syncNodeTransitSwitchPortIps(node *kapi.Node, nodeId int) []*net.IPNet {
	var transitSwitchPortIps []*net.IPNet

	parsedTransitSwitchPortIps, _ := util.ParseNodeTransitSwitchPortAddresses(node)
	if config.IPv4Mode {
		nodeTransitSwitchPortv4Ip := utilnet.AddIPOffset(cm.transitSwitchBasev4Ip, nodeId)
		transitSwitchPortIps = append(transitSwitchPortIps, &net.IPNet{IP: nodeTransitSwitchPortv4Ip, Mask: cm.transitSwitchv4Cidr.Mask})
	}

	if config.IPv6Mode {
		nodeTransitSwitchPortv6Ip := utilnet.AddIPOffset(cm.transitSwitchBasev6Ip, nodeId)
		transitSwitchPortIps = append(transitSwitchPortIps, &net.IPNet{IP: nodeTransitSwitchPortv6Ip, Mask: cm.transitSwitchv6Cidr.Mask})
	}

	if cm.syncRequiredTransitSwitchPortIps(parsedTransitSwitchPortIps, transitSwitchPortIps) {
		return transitSwitchPortIps
	} else {
		return nil
	}
}
