package clustermanager

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	kapi "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"

	hotypes "github.com/ovn-org/ovn-kubernetes/go-controller/hybrid-overlay/pkg/types"
	houtil "github.com/ovn-org/ovn-kubernetes/go-controller/hybrid-overlay/pkg/util"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/clustermanager/subnetallocator"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	objretry "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/retry"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

const (
	OvnNodeAnnotationRetryInterval = 100 * time.Millisecond
	OvnNodeAnnotationRetryTimeout  = 1 * time.Second
)

type ClusterManager struct {
	client       clientset.Interface
	kube         kube.Interface
	watchFactory *factory.WatchFactory
	stopChan     chan struct{}
	wg           *sync.WaitGroup

	// FIXME DUAL-STACK -  Make IP Allocators more dual-stack friendly
	clusterSubnetAllocator       *subnetallocator.HostSubnetAllocator
	hybridOverlaySubnetAllocator *subnetallocator.HostSubnetAllocator

	// event recorder used to post events to k8s
	recorder record.EventRecorder

	// retry framework for nodes
	retryNodes *objretry.RetryFramework

	multiNetManager *multiNetworkClusterManager
}

// NewOvnController creates a new OVN controller for creating logical network
// infrastructure and policy
func NewClusterManager(ovnClient *util.OVNClientset, wf *factory.WatchFactory, stopChan chan struct{},
	wg *sync.WaitGroup, recorder record.EventRecorder) *ClusterManager {
	kube := &kube.Kube{
		KClient:              ovnClient.KubeClient,
		EIPClient:            ovnClient.EgressIPClient,
		EgressFirewallClient: ovnClient.EgressFirewallClient,
		CloudNetworkClient:   ovnClient.CloudNetworkClient,
	}

	var hybridOverlaySubnetAllocator *subnetallocator.HostSubnetAllocator
	if config.HybridOverlay.Enabled {
		hybridOverlaySubnetAllocator = subnetallocator.NewHostSubnetAllocator()
	}
	cm := &ClusterManager{
		client:                       ovnClient.KubeClient,
		kube:                         kube,
		watchFactory:                 wf,
		stopChan:                     stopChan,
		wg:                           wg,
		clusterSubnetAllocator:       subnetallocator.NewHostSubnetAllocator(),
		hybridOverlaySubnetAllocator: hybridOverlaySubnetAllocator,
		recorder:                     recorder,
	}

	cm.initRetryFramework()

	if config.OVNKubernetesFeature.EnableMultiNetwork {
		cm.multiNetManager = newMultiNetworkClusterManager(ovnClient, cm.recorder)
	}
	return cm
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

func (cm *ClusterManager) initRetryFramework() {
	cm.retryNodes = cm.newRetryFramework(factory.NodeType)
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
//
//	If true, then either quit or perform a complete reconfiguration of the cluster (recreate switches/routers with new subnet values)
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
	if err := cm.InitSubnetAllocatorRanges(config.Default.ClusterSubnets); err != nil {
		klog.Errorf("Failed to initialize host subnet allocator ranges: %v", err)
		return err
	}

	if config.HybridOverlay.Enabled {
		if err := cm.InitHybridOverlaySubnetAllocatorRanges(config.HybridOverlay.ClusterSubnets); err != nil {
			klog.Errorf("Failed to initialize hybrid overlay subnet allocator ranges: %v", err)
			return err
		}
	}

	return nil
}

func (cm *ClusterManager) InitSubnetAllocatorRanges(subnets []config.CIDRNetworkEntry) error {
	return cm.clusterSubnetAllocator.InitRanges(subnets)
}

func (cm *ClusterManager) InitHybridOverlaySubnetAllocatorRanges(subnets []config.CIDRNetworkEntry) error {
	return cm.hybridOverlaySubnetAllocator.InitRanges(subnets)
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

	if cm.multiNetManager != nil {
		klog.Infof("Starts multi network manager")
		return cm.multiNetManager.Run(cm.stopChan)
	}

	return nil
}

// WatchNodes starts the watching of node resource and calls
// back the appropriate handler logic
func (cm *ClusterManager) WatchNodes() error {
	_, err := cm.retryNodes.WatchResource()
	return err
}

func (cm *ClusterManager) syncNodes(nodes []interface{}) error {
	for _, tmp := range nodes {
		node, ok := tmp.(*kapi.Node)
		if !ok {
			return fmt.Errorf("spurious object in syncNodes: %v", tmp)
		}

		hostSubnets, _ := util.ParseNodeHostSubnetAnnotation(node, types.DefaultNetworkName)
		if config.HybridOverlay.Enabled && len(hostSubnets) == 0 && houtil.IsHybridOverlayNode(node) {
			// this is a hybrid overlay node so mark as allocated from the hybrid overlay subnet allocator
			hostSubnet, err := houtil.ParseHybridOverlayHostSubnet(node)
			if err != nil {
				klog.Warning(err.Error())
			} else if hostSubnet != nil {
				klog.V(5).Infof("Node %s contains subnets: %v", node.Name, hostSubnet)
				if err := cm.hybridOverlaySubnetAllocator.MarkSubnetsAllocated(node.Name, hostSubnet); err != nil {
					utilruntime.HandleError(err)
				}
			}
			// there is nothing left to be done if this is a hybrid overlay node
			continue
		}

		klog.V(5).Infof("Node %s contains subnets: %v", node.Name, hostSubnets)
		if err := cm.clusterSubnetAllocator.MarkSubnetsAllocated(node.Name, hostSubnets...); err != nil {
			utilruntime.HandleError(err)
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
					cm.releaseHybridOverlayNodeSubnet(node.Name)
				}
				return fmt.Errorf("failed to set hybrid overlay annotations for node %s: %v", node.Name, err)
			}
		}
		return nil
	}

	return cm.addNode(node)
}

func (cm *ClusterManager) updateNodeAnnotationWithRetry(nodeName string, hostSubnets []*net.IPNet) error {
	// Retry if it fails because of potential conflict which is transient. Return error in the
	// case of other errors (say temporary API server down), and it will be taken care of by the
	// retry mechanism.
	resultErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		// Informer cache should not be mutated, so get a copy of the object
		node, err := cm.watchFactory.GetNode(nodeName)
		if err != nil {
			return err
		}

		cnode := node.DeepCopy()
		cnode.Annotations, err = util.UpdateNodeHostSubnetAnnotation(cnode.Annotations, hostSubnets, types.DefaultNetworkName)
		if err != nil {
			return fmt.Errorf("failed to update node %q annotation subnet %s",
				node.Name, util.JoinIPNets(hostSubnets, ","))
		}
		return cm.kube.PatchNode(node, cnode)
	})
	if resultErr != nil {
		return fmt.Errorf("failed to update node %s annotation", nodeName)
	}
	return nil
}

func (cm *ClusterManager) addNode(node *kapi.Node) error {
	existingSubnets, err := util.ParseNodeHostSubnetAnnotation(node, types.DefaultNetworkName)
	if err != nil && !util.IsAnnotationNotSetError(err) {
		// Log the error and try to allocate new subnets
		klog.Infof("Failed to get node %s host subnets annotations: %v", node.Name, err)
	}

	hostSubnets, allocatedSubnets, err := cm.clusterSubnetAllocator.AllocateNodeSubnets(node.Name, existingSubnets, config.IPv4Mode, config.IPv6Mode)
	if err != nil {
		return err
	}

	if len(allocatedSubnets) == 0 {
		return nil
	}

	// Release the allocation on error
	defer func() {
		if err != nil {
			if errR := cm.clusterSubnetAllocator.ReleaseNodeSubnets(node.Name, allocatedSubnets...); errR != nil {
				klog.Warningf("Error releasing node %s subnets: %v", node.Name, errR)
			}
		}
	}()

	// Set the HostSubnet annotation on the node object to signal
	// to nodes that their logical infrastructure is set up and they can
	// proceed with their initialization
	return cm.updateNodeAnnotationWithRetry(node.Name, hostSubnets)
}

func (cm *ClusterManager) deleteNode(node *kapi.Node) error {
	if config.HybridOverlay.Enabled {
		cm.releaseHybridOverlayNodeSubnet(node.Name)
	}

	cm.clusterSubnetAllocator.ReleaseAllNodeSubnets(node.Name)
	return nil
}

// hybridOverlayNodeEnsureSubnet allocates a subnet and sets the
// hybrid overlay subnet annotation. It returns any newly allocated subnet
// or an error. If an error occurs, the newly allocated subnet will be released.
func (cm *ClusterManager) hybridOverlayNodeEnsureSubnet(node *kapi.Node, annotator kube.Annotator) (*net.IPNet, error) {
	var existingSubnets []*net.IPNet
	// Do not allocate a subnet if the node already has one
	subnet, err := houtil.ParseHybridOverlayHostSubnet(node)
	if err != nil {
		// Log the error and try to allocate new subnets
		klog.Infof("Failed to get node %s hybrid overlay subnet annotation: %v", node.Name, err)
	} else if subnet != nil {
		existingSubnets = []*net.IPNet{subnet}
	}

	// Allocate a new host subnet for this node
	// FIXME: hybrid overlay is only IPv4 for now due to limitations on the Windows side
	hostSubnets, allocatedSubnets, err := cm.hybridOverlaySubnetAllocator.AllocateNodeSubnets(node.Name, existingSubnets, true, false)
	if err != nil {
		return nil, fmt.Errorf("error allocating hybrid overlay HostSubnet for node %s: %v", node.Name, err)
	}

	if err := annotator.Set(hotypes.HybridOverlayNodeSubnet, hostSubnets[0].String()); err != nil {
		_ = cm.hybridOverlaySubnetAllocator.ReleaseNodeSubnets(node.Name, allocatedSubnets...)
		return nil, err
	}

	return hostSubnets[0], nil
}

func (cm *ClusterManager) releaseHybridOverlayNodeSubnet(nodeName string) {
	cm.hybridOverlaySubnetAllocator.ReleaseAllNodeSubnets(nodeName)
	klog.Infof("Deleted hybrid overlay HostSubnets for node %s", nodeName)
}

// shouldUpdateNode() determines if the ovn-kubernetes plugin should update the state of the node.
// ovn-kube should not perform an update if it does not assign a hostsubnet, or if you want to change
// whether or not ovn-kubernetes assigns a hostsubnet
func shouldUpdateNode(node, oldNode *kapi.Node) (bool, error) {
	newNoHostSubnet := util.NoHostSubnet(node)
	oldNoHostSubnet := util.NoHostSubnet(node)

	if oldNoHostSubnet && newNoHostSubnet {
		return false, nil
	} else if oldNoHostSubnet && !newNoHostSubnet {
		return false, fmt.Errorf("error updating node %s, cannot remove assigned hostsubnet, please delete node and recreate", node.Name)
	} else if !oldNoHostSubnet && newNoHostSubnet {
		return false, fmt.Errorf("error updating node %s, cannot assign a hostsubnet to already created node, please delete node and recreate", node.Name)
	}

	return true, nil
}
