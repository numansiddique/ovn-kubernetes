package ovn

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	hocontroller "github.com/ovn-org/ovn-kubernetes/go-controller/hybrid-overlay/pkg/controller"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/informer"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	addressset "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/address_set"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/controller/unidling"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	kapi "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

// Local Controller structure is the object which holds the controls for starting
// and reacting upon the watched resources (e.g. pods, endpoints) on each
// node configured as a local AZ node.
type LocalController struct {
	wg *sync.WaitGroup
	oc *Controller
}

// NewLocalOvnController creates a new OVN controller for creating logical network
// infrastructure and policy
func NewLocalOvnController(ovnClient *util.OVNClientset, wf *factory.WatchFactory, stopChan <-chan struct{}, addressSetFactory addressset.AddressSetFactory,
	libovsdbOvnNBClient libovsdbclient.Client, libovsdbOvnSBClient libovsdbclient.Client,
	recorder record.EventRecorder, nodeName string) *LocalController {
	if addressSetFactory == nil {
		addressSetFactory = addressset.NewOvnAddressSetFactory(libovsdbOvnNBClient)
	}
	oc := NewOvnController(ovnClient, wf, stopChan, addressSetFactory, libovsdbOvnNBClient, libovsdbOvnSBClient, recorder, true, nodeName)
	return &LocalController{
		oc: oc,
	}
}

func (lc *LocalController) Start(wg *sync.WaitGroup) error {
	wg.Add(1)
	go func() {
		lc.wg = wg
		_ = lc.Run(wg)
		klog.Infof("Stopped local controller")
		wg.Done()
	}()

	return nil
}

func (lc *LocalController) Run(wg *sync.WaitGroup) error {
	var err error
	var node *kapi.Node
	var subnets []*net.IPNet

	nodeName := lc.oc.nodeName

	// First wait for the Master to set all the required annotations, timeout is 300s.
	err = wait.PollImmediate(500*time.Millisecond, 300*time.Second, func() (bool, error) {
		node, err = lc.oc.kube.GetNode(nodeName)
		if err != nil {
			klog.Infof("Waiting to retrieve node %s: %v", nodeName, err)
			return false, nil
		}
		if _, err = util.ParseNodeHostSubnetAnnotation(node); err != nil {
			klog.Infof("Waiting for node %s to start, no annotation found on node for subnet: %v", nodeName, err)
			return false, nil
		}
		if util.GetNodeId(node) == -1 {
			klog.Infof("Still waiting for master to annotate nodeId on node %s", nodeName)
			return false, nil
		}
		if _, err = util.ParseNodeChassisIDAnnotation(node); err != nil {
			klog.Infof("Still waiting for master to annotate chassisId on node %s: %v", nodeName, err)
			return false, nil
		}
		if _, err = util.ParseNodeManagementPortMACAddress(node); err != nil {
			klog.Infof("Still waiting for master to annotate MgmtPortMACAddress on node %s: %v", nodeName, err)
			return false, nil
		}
		if _, err = util.ParseNodeL3GatewayAnnotation(node); err != nil {
			klog.Infof("Still waiting for master to annotate L3Gateway on node %s: %v", nodeName, err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("timed out waiting for node's: %q logical switch: %v", nodeName, err)
	}

	nodeId := util.GetNodeId(node)
	joinSubnets, err := config.GetJoinSubnets(nodeId)
	if err != nil {
		return fmt.Errorf("failed to get join subnets for node %s: %v", nodeName, err)
	}
	klog.Infof("Node %s ready for ovn initialization with: host subnet %s join subnet %s",
		nodeName, util.JoinIPNets(subnets, ","), util.JoinIPNets(joinSubnets, ","))

	err = lc.oc.probeOvnFeatures()
	if err != nil {
		return err
	}
	// Start and sync the watch factory to begin listening for events
	if err := lc.oc.watchFactory.Start(); err != nil {
		return err
	}

	if err := lc.oc.StartInterconnectController(wg); err != nil {
		return err
	}

	klog.Infof("Starting the node watcher...")

	lc.WatchNodes()

	klog.Infof("Starting the egress IP watchers...")
	if config.OVNKubernetesFeature.EnableEgressIP {
		// This is probably the best starting order for all egress IP handlers.
		// WatchEgressIPNamespaces and WatchEgressIPPods only use the informer
		// cache to retrieve the egress IPs when determining if namespace/pods
		// match. It is thus better if we initialize them first and allow
		// WatchEgressNodes / WatchEgressIP to initialize after. Those handlers
		// might change the assignments of the existing objects. If we do the
		// inverse and start WatchEgressIPNamespaces / WatchEgressIPPod last, we
		// risk performing a bunch of modifications on the EgressIP objects when
		// we restart and then have these handlers act on stale data when they
		// sync.
		lc.oc.WatchEgressIPNamespaces()
		lc.oc.WatchEgressIPPods()
		lc.oc.WatchEgressNodes()
		lc.oc.WatchEgressIP()
		if util.PlatformTypeIsEgressIPCloudProvider() {
			lc.oc.WatchCloudPrivateIPConfig()
		}
	}

	klog.Infof("Starting the egress Firewall...")
	if config.OVNKubernetesFeature.EnableEgressFirewall {
		var err error
		lc.oc.egressFirewallDNS, err = NewEgressDNS(lc.oc.addressSetFactory, lc.oc.stopChan)
		if err != nil {
			panic(fmt.Sprintf("Error creating egress firewall DNS for node %s: %v", node.Name, err))
		}
		lc.oc.egressFirewallDNS.Run(egressFirewallDNSDefaultDuration)
		lc.oc.egressFirewallHandler = lc.oc.WatchEgressFirewall()
	}

	klog.Infof("Starting the unidling controller...")
	if config.Kubernetes.OVNEmptyLbEvents {
		klog.Infof("Starting unidling controller")
		unidlingController, err := unidling.NewController(
			lc.oc.recorder,
			lc.oc.watchFactory.ServiceInformer(),
			lc.oc.sbClient,
		)
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			unidlingController.Run(lc.oc.stopChan)
		}()
	}

	klog.Infof("Starting the hybrid overlay controller...")
	if lc.oc.hoMaster != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lc.oc.hoMaster.Run(lc.oc.stopChan)
		}()
	}

	return nil
}

func (lc *LocalController) WatchNamespaces() {
}

// FIXME: We only take care of Node addition.  What if the node annotations
// change afterwards (e.g., node changes ID or host subnet changes).  We should
// deal with that too.
func (lc *LocalController) WatchNodes() {
	lc.oc.watchFactory.AddNodeHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node := obj.(*kapi.Node)
			if node.Name != lc.oc.nodeName {
				// We are only interested in the local node
				return
			}

			klog.Infof("Node %s added", node.Name)

			subnets, err := util.ParseNodeHostSubnetAnnotation(node)
			if err != nil {
				panic(fmt.Sprintf("Failed to find annotation node %s for subnet: %v", lc.oc.nodeName, err))
			}

			err = lc.oc.SetupMaster(lc.oc.nodeName, make([]string, 0), util.GetNodeId(node))
			if err != nil {
				panic(fmt.Sprintf("Failed to setup master topology, error: %v", err))
			}

			err = lc.oc.ensureNodeLogicalNetwork(node, subnets)
			if err != nil {
				panic(fmt.Sprintf("Failed to setup node logical network, error: %v", err))
			}

			klog.Infof("Starting some more of the Watchers...")

			// Start service watch factory and sync services
			lc.oc.svcFactory.Start(lc.oc.stopChan)

			// Services should be started after nodes to prevent LB churn
			if err := lc.oc.StartServiceController(lc.wg, true); err != nil {
				panic(fmt.Sprintf("Failed to start service controller, error: %v", err))
			}

			lc.oc.WatchNamespaces()

			lc.oc.WatchPods()

			// WatchNetworkPolicy depends on WatchPods and WatchNamespaces
			lc.oc.WatchNetworkPolicy()

			if err = lc.oc.syncNodeClusterRouterPort(node, subnets); err != nil {
				if !util.IsAnnotationNotSetError(err) {
					klog.Warningf(err.Error())
				}
			}

			err = lc.oc.syncNodeManagementPort(node, subnets)
			if err != nil {
				panic(fmt.Sprintf("Error creating management port for node %s: %v", node.Name, err))
			}

			if err := lc.oc.syncNodeGateway(node, subnets); err != nil {
				panic(fmt.Sprintf("Error syncing node gateway for node %s: %v", node.Name, err))
			}

			// ensure pods that already exist on this node have their logical ports created
			options := metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("spec.nodeName", node.Name).String()}
			pods, err := lc.oc.client.CoreV1().Pods(metav1.NamespaceAll).List(context.TODO(), options)
			if err != nil {
				klog.Errorf("Unable to list existing pods on node: %s, existing pods on this node may not function")
			} else {
				lc.oc.addRetryPods(pods.Items)
				lc.oc.requestRetryPods()
			}

			// Start Hybrid Overlay
			if config.HybridOverlay.Enabled {
				lc.oc.hoMaster, err = hocontroller.NewMaster(
					lc.oc.kube,
					lc.oc.watchFactory.NodeInformer(),
					lc.oc.watchFactory.NamespaceInformer(),
					lc.oc.watchFactory.PodInformer(),
					lc.oc.nbClient,
					lc.oc.sbClient,
					informer.NewDefaultEventHandler,
				)
				if err != nil {
					klog.Errorf("Failed to set up hybrid overlay master: %v", err)
				}
			}
		},
		UpdateFunc: func(old, new interface{}) {
		},
		DeleteFunc: func(obj interface{}) {
		},
	}, nil)
}
